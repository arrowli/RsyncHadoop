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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.DataChecksum.Type;
/* new added */
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

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
	public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L; // 1
																				// hour
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

	// int ipTosValue = NetUtils.NOT_SET_IP_TOS;

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

	private DataEncryptionKey encryptionKey;
	private volatile FsServerDefaults serverDefaults;
	private volatile long serverDefaultsLastUpdate;
	private final Conf dfsClientConf;
	private boolean connectToDnViaHostname;

	/**
	 * Create a new DFSClient connected to the given nameNodeAddr or
	 * rpcNamenode. Exactly one of nameNodeAddr or rpcNamenode must be null.
	 */
	RsyncCopy(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
			Configuration conf, FileSystem.Statistics stats, long uniqueId,
			DistributedFileSystem dfs) throws IOException {
		this.dfsClientConf = new Conf(conf);
		this.conf = conf;
		this.stats = stats;
		this.connectToDnViaHostname = conf.getBoolean(
				DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
		this.socketFactory = NetUtils.getSocketFactory(conf,
				ClientProtocol.class);
		this.localHost = InetAddress.getLocalHost();
		this.dfs = (DistributedFileSystem) testPath.getFileSystem(conf);
		String taskId = conf.get("mapreduce.task.attempt.id");
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
		this.authority = nameNodeUri == null ? "null" : nameNodeUri
				.getAuthority();

		this.socketTimeout = conf.getInt("dfs.client.socket-timeout",
				HdfsServerConstants.READ_TIMEOUT);
		this.namenodeRPCSocketTimeout = 60 * 1000;
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
	 * Get server default values for a number of configuration params.
	 * 
	 * @see ClientProtocol#getServerDefaults()
	 */
	public FsServerDefaults getServerDefaults() throws IOException {
		long now = Time.now();
		if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
			serverDefaults = namenode.getServerDefaults();
			serverDefaultsLastUpdate = now;
		}
		return serverDefaults;
	}

	/**
	 * @return true if data sent between this client and DNs should be
	 *         encrypted, false otherwise.
	 * @throws IOException
	 *             in the event of error communicating with the NN
	 */
	boolean shouldEncryptData() throws IOException {
		FsServerDefaults d = getServerDefaults();
		return d == null ? false : d.getEncryptDataTransfer();
	}

	@InterfaceAudience.Private
	public DataEncryptionKey getDataEncryptionKey() throws IOException {
		if (shouldEncryptData()) {
			synchronized (this) {
				if (encryptionKey == null
						|| encryptionKey.expiryDate < Time.now()) {
					LOG.debug("Getting new encryption token from NN");
					encryptionKey = namenode.getDataEncryptionKey();
				}
				return encryptionKey;
			}
		} else {
			return null;
		}
	}

	/**
	 * Connect to the given datanode's datantrasfer port, and return the
	 * resulting IOStreamPair. This includes encryption wrapping, etc.
	 */
	private static IOStreamPair connectToDN(SocketFactory socketFactory,
			boolean connectToDnViaHostname, DataEncryptionKey encryptionKey,
			DatanodeInfo dn, int timeout) throws IOException {
		boolean success = false;
		Socket sock = null;
		try {
			sock = socketFactory.createSocket();
			String dnAddr = dn.getXferAddr(connectToDnViaHostname);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Connecting to datanode " + dnAddr);
			}
			NetUtils.connect(sock, NetUtils.createSocketAddr(dnAddr), timeout);
			sock.setSoTimeout(timeout);

			OutputStream unbufOut = NetUtils.getOutputStream(sock);
			InputStream unbufIn = NetUtils.getInputStream(sock);
			IOStreamPair ret;
			if (encryptionKey != null) {
				ret = DataTransferEncryptor.getEncryptedStreams(unbufOut,
						unbufIn, encryptionKey);
			} else {
				ret = new IOStreamPair(unbufIn, unbufOut);
			}
			success = true;
			return ret;
		} finally {
			if (!success) {
				IOUtils.closeSocket(sock);
			}
		}
	}

	/**
	 * Get the checksum of a file.
	 * 
	 * @param src
	 *            The file path
	 * @return The checksum
	 */
	public void getFileChecksum(int dataTransferVersion, String src,
			ClientProtocol namenode,
			ProtocolProxy<ClientProtocol> namenodeProxy,
			SocketFactory socketFactory, int socketTimeout) throws IOException {
		final DataOutputBuffer md5out = new DataOutputBuffer();
		// get all block locations
		final LocatedBlocks locatedBlocks = callGetBlockLocations(namenode,
				src, 0, Long.MAX_VALUE, isMetaInfoSuppoted(namenodeProxy));
		if (locatedBlocks == null) {
			throw new IOException(
					"Null block locations, mostly because non-existent file "
							+ src);
		}
		int namespaceId = 0;
		boolean refetchBlocks = false;
		int lastRetriedIndex = -1;
		dataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;

		final List<LocatedBlock> locatedblocks = locatedBlocks
				.getLocatedBlocks();

		int bytesPerCRC = -1;
		DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
		long crcPerBlock = 0;

		// get block checksum for each block
		for (int i = 0; i < locatedblocks.size(); i++) {
			LocatedBlock lb = locatedblocks.get(i);
			final ExtendedBlock block = lb.getBlock();
			final DatanodeInfo[] datanodes = lb.getLocations();

			// try each datanode location of the block
			final int timeout = 3000 * datanodes.length + socketTimeout;
			boolean done = false;
			for (int j = 0; !done && j < datanodes.length; j++) {
				DataOutputStream out = null;
				DataInputStream in = null;

				try {
					// connect to a datanode
					IOStreamPair pair = connectToDN(socketFactory,
							connectToDnViaHostname, getDataEncryptionKey(),
							datanodes[j], timeout);
					out = new DataOutputStream(new BufferedOutputStream(
							pair.out, HdfsConstants.SMALL_BUFFER_SIZE));
					in = new DataInputStream(pair.in);

					LOG.warn("BlockMetadataHeader size : "+BlockMetadataHeader.getHeaderSize());
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("write to " + datanodes[j] + ": "
								+ Op.RSYNC_CHUNKS_CHECKSUM + ", block=" + block);
					}
					// get block MD5
					new Sender(out).chunksChecksum(block, lb.getBlockToken());

					final BlockOpResponseProto reply = BlockOpResponseProto
							.parseFrom(PBHelper.vintPrefixed(in));

					if (reply.getStatus() != Status.SUCCESS) {
						if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
							throw new InvalidBlockTokenException();
						} else {
							throw new IOException("Bad response " + reply
									+ " for block " + block + " from datanode "
									+ datanodes[j]);
						}
					}

					OpChunksChecksumResponseProto checksumData = reply
							.getChunksChecksumResponse();

					// read byte-per-checksum
					final int bpc = checksumData.getBytesPerCrc();
					if (i == 0) { // first block
						bytesPerCRC = bpc;
					} else if (bpc != bytesPerCRC) {
						throw new IOException(
								"Byte-per-checksum not matched: bpc=" + bpc
										+ " but bytesPerCRC=" + bytesPerCRC);
					}

					// read crc-per-block
					final long cpb = checksumData.getCrcPerBlock();
					if (locatedblocks.size() > 1 && i == 0) {
						crcPerBlock = cpb;
					}

					final List<Integer> checksums = checksumData.getChecksumsList();
					
					LOG.warn("checksum size : "+checksumData.getBytesPerChunk());
					LOG.warn("checksum counts : "+checksumData.getChunksPerBlock());
					LOG.warn("checksum list:");
					for(Integer cs : checksums){
						LOG.warn(Integer.toHexString(cs));
					}
					
					// read md5
					final MD5Hash md5 = new MD5Hash(checksumData.getMd5()
							.toByteArray());
					md5.write(md5out);
					LOG.warn(md5);
					// read crc-type
					final DataChecksum.Type ct;
					if (checksumData.hasCrcType()) {
						ct = PBHelper.convert(checksumData.getCrcType());
					} else {
						LOG.debug("Retrieving checksum from an earlier-version DataNode: "
								+ "inferring checksum by reading first byte");
						ct = inferChecksumTypeByReading(clientName,
								socketFactory, socketTimeout, lb, datanodes[j],
								encryptionKey, connectToDnViaHostname);
					}

					if (i == 0) { // first block
						crcType = ct;
					} else if (crcType != DataChecksum.Type.MIXED
							&& crcType != ct) {
						// if crc types are mixed in a file
						crcType = DataChecksum.Type.MIXED;
					}

					done = true;

					if (LOG.isDebugEnabled()) {
						if (i == 0) {
							LOG.debug("set bytesPerCRC=" + bytesPerCRC
									+ ", crcPerBlock=" + crcPerBlock);
						}
						LOG.debug("got reply from " + datanodes[j] + ": md5="
								+ md5);
					}
				} catch (InvalidBlockTokenException ibte) {
					if (i > lastRetriedIndex) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
									+ "for file "
									+ src
									+ " for block "
									+ block
									+ " from datanode "
									+ datanodes[j]
									+ ". Will retry the block once.");
						}
						lastRetriedIndex = i;
						done = true; // actually it's not done; but we'll retry
						i--; // repeat at i-th block
						refetchBlocks = true;
						break;
					}
				} catch (IOException ie) {
					LOG.warn("src=" + src + ", datanodes[" + j + "]="
							+ datanodes[j], ie);
				} finally {
					IOUtils.closeStream(in);
					IOUtils.closeStream(out);
				}
			}

			if (!done) {
				throw new IOException("Fail to get block MD5 for " + block);
			}
		}
	}

	/**
	 * Infer the checksum type for a replica by sending an OP_READ_BLOCK for the
	 * first byte of that replica. This is used for compatibility with older
	 * HDFS versions which did not include the checksum type in
	 * OpBlockChecksumResponseProto.
	 * 
	 * @param in
	 *            input stream from datanode
	 * @param out
	 *            output stream to datanode
	 * @param lb
	 *            the located block
	 * @param clientName
	 *            the name of the DFSClient requesting the checksum
	 * @param dn
	 *            the connected datanode
	 * @return the inferred checksum type
	 * @throws IOException
	 *             if an error occurs
	 */
	private static Type inferChecksumTypeByReading(String clientName,
			SocketFactory socketFactory, int socketTimeout, LocatedBlock lb,
			DatanodeInfo dn, DataEncryptionKey encryptionKey,
			boolean connectToDnViaHostname) throws IOException {
		IOStreamPair pair = connectToDN(socketFactory, connectToDnViaHostname,
				encryptionKey, dn, socketTimeout);

		try {
			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(pair.out,
							HdfsConstants.SMALL_BUFFER_SIZE));
			DataInputStream in = new DataInputStream(pair.in);

			new Sender(out).readBlock(lb.getBlock(), lb.getBlockToken(),
					clientName, 0, 1, true,
					CachingStrategy.newDefaultStrategy());
			final BlockOpResponseProto reply = BlockOpResponseProto
					.parseFrom(PBHelper.vintPrefixed(in));

			if (reply.getStatus() != Status.SUCCESS) {
				if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
					throw new InvalidBlockTokenException();
				} else {
					throw new IOException("Bad response " + reply
							+ " trying to read " + lb.getBlock()
							+ " from datanode " + dn);
				}
			}

			return PBHelper.convert(reply.getReadOpChecksumInfo().getChecksum()
					.getType());
		} finally {
			IOUtils.cleanup(null, pair.in, pair.out);
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
		long sleepTime = conf
				.getLong(
						"dfs.client.rpc.retry.sleep",
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
		formatter.printHelp("Usage : RsyncCopy [options] <srcs....> <dst>",
				options);
	}

	protected void checkOpen() throws IOException {
		if (!clientRunning) {
			IOException result = new IOException("Filesystem closed");
			throw result;
		}
	}

	/* @deprecated */
	void addNewBlock(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException{
		HdfsFileStatus fileInfo = namenode.getFileInfo(src);
		LocatedBlocks lbs = namenode.getBlockLocations(src, 0, fileInfo.getLen());
		LocatedBlock lb1 = namenode.addBlock(src , clientName , lbs.getLastLocatedBlock().getBlock() , null, fileInfo.getFileId() , null);
		LocatedBlock lb2 = namenode.addBlock(src , clientName , lb1.getBlock() , null, fileInfo.getFileId() , null);
		LOG.warn("add block1 : "+lb1);
		LOG.warn("add block2 : "+lb2);
	}
	
	public static void main(String args[]) throws Exception {
		if (args.length < 2) {
			printUsage();
		}
		Configuration conf = new Configuration();
		InetSocketAddress nameNodeAddr = NameNode.getAddress(conf);
		ClientProtocol rpcNamenode = null;
		FileSystem.Statistics stats = null;
		long uniqueId = 0;
		DistributedFileSystem dfs = null;
		RsyncCopy rc = new RsyncCopy(nameNodeAddr, rpcNamenode, conf, stats,
				uniqueId, dfs);
		String src = "/test";
		rc.getFileChecksum(src);
		rc.addNewBlock(src);
		System.exit(0);
	}
}
