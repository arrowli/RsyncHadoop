/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import javax.net.SocketFactory;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
/* new added */
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;

/**
 * There is a need to perform fast file copy on HDFS (primarily for the purpose
 * of HBase Snapshot). The fast copy mechanism for a file works as follows :
 * 
 * 1) Query metadata for all blocks of the source file.
 * 
 * 2) For each block 'b' of the file, find out its datanode locations.
 * 
 * 3) For each block of the file, add an empty block to the namesystem for the
 * destination file.
 * 
 * 4) For each location of the block, instruct the datanode to make a local copy
 * of that block.
 * 
 * 5) Once each datanode has copied over the its respective blocks, they report
 * to the namenode about it.
 * 
 * 6) Wait for all blocks to be copied and exit.
 * 
 * This would speed up the copying process considerably by removing top of the
 * rack data transfers.
 **/

public class RsyncCopy {

	public static final Log LOG = LogFactory.getLog(RsyncCopy.class);
	public ClientProtocol namenode;
	// Namenode proxy that supports method-based compatibility
	public ProtocolProxy<ClientProtocol> namenodeProtocolProxy = null;
	static Random r = new Random();
	final String clientName;
	Configuration conf;
	SocketFactory socketFactory;
	final FileSystem.Statistics stats;

	private long namenodeVersion = ClientProtocol.versionID;
	protected Integer dataTransferVersion = -1;
	protected volatile int namespaceId = 0;

	final InetAddress localHost;
	InetSocketAddress nameNodeAddr;

	//int ipTosValue = NetUtils.NOT_SET_IP_TOS;

	volatile boolean clientRunning = true;
	private ClientProtocol rpcNamenode;
	int socketTimeout;
	int namenodeRPCSocketTimeout;

	public Object namenodeProxySyncObj = new Object();
	private final Path testPath = new Path("/test");
	private final DistributedFileSystem dfs;
	
	final UserGroupInformation ugi;
	volatile long lastLeaseRenewal;
	private final String authority;

	/**
	 * Create a new DFSClient connected to the given nameNodeAddr or
	 * rpcNamenode. Exactly one of nameNodeAddr or rpcNamenode must be null.
	 */
	RsyncCopy(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
			Configuration conf, FileSystem.Statistics stats, long uniqueId,
			DistributedFileSystem dfs) throws IOException {
		this.conf = conf;
		this.stats = stats;
		this.socketFactory = NetUtils.getSocketFactory(conf,
				ClientProtocol.class);
		this.localHost = InetAddress.getLocalHost();
		this.dfs=(DistributedFileSystem)testPath.getFileSystem(conf);
		String taskId = conf.get("mapred.task.id");
		if (taskId != null) {
			this.clientName = "RsyncCopy_" + taskId + "_" + r.nextInt() + "_"
					+ Thread.currentThread().getId();
		} else {
			this.clientName = "RsyncCopy_" + r.nextInt()
					+ ((uniqueId == 0) ? "" : "_" + uniqueId);
		}

		if (nameNodeAddr != null && rpcNamenode == null) {
			this.nameNodeAddr = nameNodeAddr;
			getNameNode();
		} else {
			throw new IllegalArgumentException(
					"Expecting exactly one of nameNodeAddr and rpcNamenode being null: "
							+ "nameNodeAddr=" + nameNodeAddr + ", rpcNamenode="
							+ rpcNamenode);
		}

		this.ugi = UserGroupInformation.getCurrentUser();
	    
		URI nameNodeUri = NameNode.getUri(NameNode.getAddress(conf));
	    this.authority = nameNodeUri == null? "null": nameNodeUri.getAuthority();

		this.socketTimeout = conf.getInt("dfs.socket.timeout",
				HdfsServerConstants.READ_TIMEOUT);
		this.namenodeRPCSocketTimeout = 60*1000;
	}

	/**
	 * Get the checksum of a file.
	 * 
	 * @param src
	 *            The file path
	 * @return The checksum
	 * @see DistributedFileSystem#getFileChecksum(Path)
	 */
	void getFileChecksum(String src) throws IOException {
		checkOpen();
		getFileChecksum(dataTransferVersion, src, namenode,
				namenodeProtocolProxy, socketFactory, socketTimeout);
	}

	/**
	 * Get the checksum of a file.
	 * 
	 * @param src
	 *            The file path
	 * @return The checksum
	 */
	public static void getFileChecksum(int dataTransferVersion, String src,
			ClientProtocol namenode,
			ProtocolProxy<ClientProtocol> namenodeProxy,
			SocketFactory socketFactory, int socketTimeout) throws IOException {
		// get all block locations
		final LocatedBlocks locatedBlocks = callGetBlockLocations(namenode,
				src, 0, Long.MAX_VALUE, isMetaInfoSuppoted(namenodeProxy));
		if (locatedBlocks == null) {
			throw new IOException(
					"Null block locations, mostly because non-existent file "
							+ src);
		}
		int namespaceId = 0;

		dataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;
			
		final List<LocatedBlock> locatedblocks = locatedBlocks
				.getLocatedBlocks();

		// get block checksum for each block
		for (int i = 0; i < locatedblocks.size(); i++) {
			LocatedBlock lb = locatedblocks.get(i);
			final ExtendedBlock block = lb.getBlock();
			final DatanodeInfo[] datanodes = lb.getLocations();
			LOG.warn(block.toString());
			LOG.warn(datanodes[0].toString());
		}
	}

	private static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
			String src, long start, long length, boolean supportMetaInfo)
			throws IOException {
		try {
			return namenode.getBlockLocations(src, start, length);
		} catch (RemoteException re) {
			throw re.unwrapRemoteException(AccessControlException.class,
					FileNotFoundException.class);
		}
	}

	public static boolean isMetaInfoSuppoted(ProtocolProxy<ClientProtocol> proxy)
			throws IOException {
		return proxy != null
				&& proxy.isMethodSupported("openAndFetchMetaInfo",
						String.class, long.class, long.class);
	}

	private void getNameNode() throws IOException {
		if (nameNodeAddr != null) {
			// The lock is to make sure namenode, namenodeProtocolProxy
			// and rpcNamenode are consistent ultimately. There is still
			// a small window where another thread can see inconsistent
			// version of namenodeProtocolProxy and namenode. But it will
			// only happen during the transit time when name-node upgrade
			// and the exception will likely to be resolved after a retry.
			//
			synchronized (namenodeProxySyncObj) {
				this.namenode = dfs.getClient().getNamenode();
			}
		}
	}

	static ClientProtocol createNamenode(ClientProtocol rpcNamenode,
			Configuration conf) throws IOException {
		long sleepTime = conf.getLong("dfs.client.rpc.retry.sleep",
				org.apache.hadoop.hdfs.protocol.HdfsConstants.LEASE_SOFTLIMIT_PERIOD);
		RetryPolicy createPolicy = RetryPolicies
				.retryUpToMaximumCountWithFixedSleep(5, sleepTime,
						TimeUnit.MILLISECONDS);

		Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
		remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class,
				createPolicy);

		Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
		exceptionToPolicyMap.put(RemoteException.class, RetryPolicies
				.retryByRemoteException(RetryPolicies.TRY_ONCE_THEN_FAIL,
						remoteExceptionToPolicyMap));
		RetryPolicy methodPolicy = RetryPolicies.retryByException(
				RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
		Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();

		methodNameToPolicyMap.put("create", methodPolicy);

		return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
				rpcNamenode, methodNameToPolicyMap);
	}

	private static void printUsage() {
		HelpFormatter formatter = new HelpFormatter();
		Options options = new Options();
		formatter.printHelp("Usage : RsyncCopy [options] <srcs....> <dst>", options);
	}

	protected void checkOpen() throws IOException {
		if (!clientRunning) {
			IOException result = new IOException("Filesystem closed");
			throw result;
		}
	}
	
	public static void main(String args[]) throws Exception {
		if(args.length < 2){
			printUsage();
		}
		Configuration conf = new Configuration();
		InetSocketAddress nameNodeAddr = NameNode
				.getAddress(conf);
		ClientProtocol rpcNamenode = null;
		FileSystem.Statistics stats = null;
		long uniqueId = 0;
		DistributedFileSystem dfs = null;
		RsyncCopy rc = new RsyncCopy(nameNodeAddr, rpcNamenode, conf, stats,
				uniqueId, dfs);
		String src = "/test";
		rc.getFileChecksum(src);
		System.exit(0);
	}
}
