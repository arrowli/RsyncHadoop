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
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import javax.net.SocketFactory;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
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
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
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
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumPairProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumStrongProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCalculateSegmentsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksAdaptiveChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.SegmentProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

import com.google.protobuf.ByteString;

/**
 * rsynccopy复制一个文件的工作过程如下 :
 * 
 * 1) 从NN获取文件所有block的位置，包括源文件src和目标文件dst的。
 * 
 * 2) 从src和dst的所有block中获取checksum列表。
 * 
 * 3) 比对后，把dst中部分block的checksum列表传递给src中的部分DN。
 * 
 * 4) 得到checksum列表的DN计算block差异，回传给rsynccopy。
 * 
 * 5) rsynccopy根据收到的信息，控制DN把block差异传递给需要的DN。
 * 
 * 6) DN之间传递block差异，并将差异存到本地的临时文件夹。
 * 
 * 7）rsynccopy确认所有差异传递完毕后，控制DN将block差异合成最后的文件
 * 
 **/

public class RsyncCopy {
	public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L; // 1
																				// hour
	public static final Log LOG = LogFactory.getLog(RsyncCopy.class);
	public ClientProtocol srcNamenode;
	public ClientProtocol dstNamenode;
	// Namenode proxy that supports method-based compatibility
	public ProtocolProxy<ClientProtocol> srcNamenodeProtocolProxy = null;
	public ProtocolProxy<ClientProtocol> dstNamenodeProtocolProxy = null;
	static Random r = new Random();
	String clientName;
	Configuration conf;
	SocketFactory socketFactory;
	FileSystem.Statistics stats;

	private long namenodeVersion = ClientProtocol.versionID;
	protected Integer dataTransferVersion = -1;
	protected volatile int namespaceId = 0;

	InetAddress localHost;
	InetSocketAddress nameNodeAddr;

	// int ipTosValue = NetUtils.NOT_SET_IP_TOS;

	volatile boolean clientRunning = true;
	private ClientProtocol rpcNamenode;
	int socketTimeout;
	int namenodeRPCSocketTimeout;

	public Object namenodeProxySyncObj = new Object();
	private Path srcPath;
	private Path dstPath;
	private DistributedFileSystem srcDfs;
	private DistributedFileSystem dstDfs;

	UserGroupInformation ugi;
	volatile long lastLeaseRenewal;
	private String authority;

	private DataEncryptionKey encryptionKey;
	private volatile FsServerDefaults serverDefaults;
	private volatile long serverDefaultsLastUpdate;
	private boolean connectToDnViaHostname;
	private LeaseRenewer renewer;

	public RsyncCopy(String srcPath, String dstPath) throws IOException {
		conf = new Configuration();
		this.srcPath = new Path(srcPath);
		this.dstPath = new Path(dstPath);

		srcDfs = (DistributedFileSystem) this.srcPath.getFileSystem(conf);
		dstDfs = (DistributedFileSystem) this.dstPath.getFileSystem(conf);
		RsyncCopyInit(NameNode.getAddress(conf), null, null, 0);
	}

	/**
	 * Create a new DFSClient connected to the given nameNodeAddr or
	 * rpcNamenode. Exactly one of nameNodeAddr or rpcNamenode must be null.
	 */
	private void RsyncCopyInit(InetSocketAddress nameNodeAddr,
			ClientProtocol rpcNamenode, FileSystem.Statistics stats,
			long uniqueId) throws IOException {
		this.stats = stats;
		this.connectToDnViaHostname = conf.getBoolean(
				DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
		this.socketFactory = NetUtils.getSocketFactory(conf,
				ClientProtocol.class);
		this.localHost = InetAddress.getLocalHost();
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

		renewer = new LeaseRenewer();
		renewer.setClientName(clientName);
		renewer.setNamenode(dstNamenode); // 只需要更新dstNamenode的lease就可以了，因为并没有更改src的文件内容
		Thread renewerThread = new Thread(renewer);
		renewerThread.start();
	}

	private class LeaseRenewer implements Runnable {
		private long lastRenewalTime;
		/** A fixed lease renewal time period in milliseconds */
		private long renewal = HdfsConstants.LEASE_SOFTLIMIT_PERIOD / 2;
		private String clientName;
		private ClientProtocol namenode;

		public void setClientName(String clientName) {
			this.clientName = clientName;
			lastRenewalTime = Time.now();
		}

		public void setNamenode(ClientProtocol namenode) {
			this.namenode = namenode;
		}

		public void run() {
			try {
				while (true) {
					namenode.renewLease(clientName);
					Thread.sleep(renewal);
				}
			} catch (IOException e) {

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private class RsyncCopyFile {
		private Path srcPath;
		private Path dstPath;
		public ClientProtocol srcNamenode;
		public ClientProtocol dstNamenode;
		// Namenode proxy that supports method-based compatibility
		public ProtocolProxy<ClientProtocol> srcNamenodeProtocolProxy = null;
		public ProtocolProxy<ClientProtocol> dstNamenodeProtocolProxy = null;
		final String clientName;
		Configuration conf;
		SocketFactory socketFactory;
		final FileSystem.Statistics stats;

		private long namenodeVersion = ClientProtocol.versionID;
		protected Integer dataTransferVersion = -1;
		protected volatile int namespaceId = 0;

		// int ipTosValue = NetUtils.NOT_SET_IP_TOS;

		volatile boolean clientRunning = true;
		private ClientProtocol rpcNamenode;
		int socketTimeout;
		int namenodeRPCSocketTimeout;

		final UserGroupInformation ugi;
		volatile long lastLeaseRenewal;
		private final String authority;

		private DataEncryptionKey encryptionKey;
		private volatile FsServerDefaults serverDefaults;
		private volatile long serverDefaultsLastUpdate;
		private boolean connectToDnViaHostname;

		private class ChecksumPair {
			private Integer simple;
			private byte[] md5;

			public ChecksumPair(Integer simple, byte[] bs) {
				this.simple = simple;
				this.md5 = bs;
			}

			public Integer getSimple() {
				return simple;
			}

			public void setSimple(Integer simple) {
				this.simple = simple;
			}

			public byte[] getMd5() {
				return md5;
			}

			public void setMd5(byte[] md5) {
				this.md5 = md5;
			}
		}

		private class BlockInfo {
			private LocatedBlock locatedBlock;
			private LinkedList<ChecksumPair> checksums;
			private List<ChecksumStrongProto> checksumsAdaptive;
			private LinkedList<SegmentProto> segments;

			public BlockInfo(LocatedBlock locatedBlock) {
				this.locatedBlock = locatedBlock;
				this.checksums = new LinkedList<ChecksumPair>();
				this.segments = new LinkedList<SegmentProto>();
			}

			public BlockInfo(LocatedBlock locatedBlock,
					LinkedList<ChecksumPair> checksums,
					LinkedList<SegmentProto> segments) {
				this.locatedBlock = locatedBlock;
				this.checksums = checksums;
				this.segments = segments;
			}

			public LocatedBlock getLocatedBlock() {
				return locatedBlock;
			}

			public void setLocatedBlock(LocatedBlock locatedBlock) {
				this.locatedBlock = locatedBlock;
			}

			public List<ChecksumPair> getChecksums() {
				return checksums;
			}

			public void setChecksums(LinkedList<ChecksumPair> checksums) {
				this.checksums = checksums;
			}

			public void addChecksum(ChecksumPair checksum) {
				this.checksums.add(checksum);
			}

			public LinkedList<SegmentProto> getSegments() {
				return segments;
			}

			public void setSegments(LinkedList<SegmentProto> segments) {
				this.segments = segments;
			}

			public List<ChecksumStrongProto> getChecksumsAdaptive() {
				return checksumsAdaptive;
			}

			public void setChecksumsAdaptive(List<ChecksumStrongProto> list) {
				this.checksumsAdaptive = list;
			}
		}

		private class FileInfo {
			private List<BlockInfo> blocks;
			private String filepath;
			private long fileSize;

			public FileInfo(String filepath) {
				this.setFilepath(filepath);
				this.blocks = new LinkedList<BlockInfo>();
			}

			public List<BlockInfo> getBlocks() {
				return blocks;
			}

			public void setBlocks(List<BlockInfo> blocks) {
				this.blocks = blocks;
			}

			public void addBlock(BlockInfo block) {
				this.blocks.add(block);
			}

			public String getFilepath() {
				return filepath;
			}

			public void setFilepath(String filepath) {
				this.filepath = filepath;
			}

			public long getFileSize() {
				return fileSize;
			}

			public void setFileSize(long fileSize) {
				this.fileSize = fileSize;
			}
		}

		private FileInfo srcFileInfo;
		private FileInfo dstFileInfo;

		// private FileInfo newFileInfo;

		RsyncCopyFile(ClientProtocol srcNamenode,
				ProtocolProxy<ClientProtocol> srcNamenodeProtocolProxy,
				Path srcPath, ClientProtocol dstNamenode,
				ProtocolProxy<ClientProtocol> dstNamenodeProtocolProxy,
				Path dstPath, Configuration conf, FileSystem.Statistics stats,
				long uniqueId) throws IOException {
			this.srcNamenode = srcNamenode;
			this.srcNamenodeProtocolProxy = srcNamenodeProtocolProxy;
			this.srcPath = srcPath;
			this.dstNamenode = dstNamenode;
			this.dstNamenodeProtocolProxy = dstNamenodeProtocolProxy;
			this.dstPath = dstPath;

			this.conf = conf;
			this.stats = stats;
			this.connectToDnViaHostname = conf.getBoolean(
					DFS_CLIENT_USE_DN_HOSTNAME,
					DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
			this.socketFactory = NetUtils.getSocketFactory(conf,
					ClientProtocol.class);
			String taskId = conf.get("mapreduce.task.attempt.id");
			if (taskId != null) {
				this.clientName = "RsyncCopy_" + taskId + "_" + r.nextInt()
						+ "_" + Thread.currentThread().getId();
			} else {
				this.clientName = "RsyncCopy_" + r.nextInt()
						+ ((uniqueId == 0) ? "" : "_" + uniqueId);
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
		 * Get the source file blocks information from NN Get the destination
		 * file blocks information from NN Create the new temp file in dst file
		 * system.
		 */
		void getSDFileInfo() throws IOException {
			LocatedBlocks srcLocatedBlocks = callGetBlockLocations(srcNamenode,
					srcPath.toUri().getPath(), 0, Long.MAX_VALUE,
					isMetaInfoSupported(srcNamenodeProtocolProxy));
			if (srcLocatedBlocks == null) {
				throw new IOException(
						"Null block locations, mostly because non-existent file "
								+ srcPath.toString());
			}
			LocatedBlocks dstLocatedBlocks = callGetBlockLocations(dstNamenode,
					dstPath.toUri().getPath(), 0, Long.MAX_VALUE,
					isMetaInfoSupported(dstNamenodeProtocolProxy));
			if (dstLocatedBlocks == null) {
				throw new IOException(
						"Null block locations, mostly because non-existent file "
								+ dstPath.toString());
			}

			this.srcFileInfo = new FileInfo(srcPath.toUri().getPath());
			this.dstFileInfo = new FileInfo(dstPath.toUri().getPath());

			this.srcFileInfo.setFileSize(srcLocatedBlocks.getFileLength());
			this.dstFileInfo.setFileSize(dstLocatedBlocks.getFileLength());

			for (LocatedBlock lb : srcLocatedBlocks.getLocatedBlocks()) {
				srcFileInfo.addBlock(new BlockInfo(lb));
			}
			for (LocatedBlock lb : dstLocatedBlocks.getLocatedBlocks()) {
				dstFileInfo.addBlock(new BlockInfo(lb));
			}

			// this.newFileInfo =
			// createNewFile(dstPath.toString()+".rsync",srcFileInfo);
		}

		/**
		 * Get the checksum of a file.
		 * 
		 * @param src
		 *            The file path
		 * @return The checksum
		 * @see DistributedFileSystem#getFileChecksum(Path)
		 */
		void getSDFileChecksum(int chunkSize) throws IOException {
			checkOpen();
			getFileChecksum(dataTransferVersion, srcFileInfo, srcNamenode,
					srcNamenodeProtocolProxy, socketFactory, socketTimeout,
					chunkSize);
			getFileChecksum(dataTransferVersion, dstFileInfo, dstNamenode,
					dstNamenodeProtocolProxy, socketFactory, socketTimeout,
					chunkSize);

			// getFileAdaptiveChecksum(dataTransferVersion, dstFileInfo,
			// dstNamenode,
			// dstNamenodeProtocolProxy, socketFactory, socketTimeout);
		}

		/**
		 * Get the checksum of a file.
		 * 
		 * @param src
		 *            The file path
		 * @return The checksum
		 */
		public void getFileChecksum(int dataTransferVersion, FileInfo fileInfo,
				ClientProtocol namenode,
				ProtocolProxy<ClientProtocol> namenodeProxy,
				SocketFactory socketFactory, int socketTimeout, int chunkSize)
				throws IOException {
			LOG.info("getFileShecksum start " + fileInfo.getFilepath());
			final DataOutputBuffer md5out = new DataOutputBuffer();
			// int namespaceId = 0;
			// boolean refetchBlocks = false;
			int lastRetriedIndex = -1;
			dataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;
			int bytesPerCRC = -1;
			DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
			long crcPerBlock = 0;

			// get block checksum for each block
			for (int i = 0; i < fileInfo.getBlocks().size(); i++) {
				LocatedBlock lb = fileInfo.getBlocks().get(i).locatedBlock;
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

						// get block MD5
						int bytesPerBlock = chunkSize;
						new Sender(out).chunksChecksum(block,
								lb.getBlockToken(), bytesPerBlock);

						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
								throw new InvalidBlockTokenException();
							} else {
								throw new IOException("Bad response " + reply
										+ " for block " + block
										+ " from datanode " + datanodes[j]);
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
						if (srcFileInfo.getBlocks().size() > 1 && i == 0) {
							crcPerBlock = cpb;
						}

						final List<ChecksumPairProto> checksums = checksumData
								.getChecksumsList();
						for (ChecksumPairProto cs : checksums) {
							final MD5Hash md5s = new MD5Hash(cs.getMd5()
									.toByteArray());
							fileInfo.getBlocks()
									.get(i)
									.getChecksums()
									.add(new ChecksumPair(cs.getSimple(), md5s
											.getDigest()));
						}
						// read md5
						final MD5Hash md5 = new MD5Hash(checksumData.getMd5()
								.toByteArray());
						md5.write(md5out);
						// read crc-type
						final DataChecksum.Type ct;
						if (checksumData.hasCrcType()) {
							ct = PBHelper.convert(checksumData.getCrcType());
						} else {
							LOG.debug("Retrieving checksum from an earlier-version DataNode: "
									+ "inferring checksum by reading first byte");
							ct = inferChecksumTypeByReading(clientName,
									socketFactory, socketTimeout, lb,
									datanodes[j], encryptionKey,
									connectToDnViaHostname);
						}

						if (i == 0) { // first block
							crcType = ct;
						} else if (crcType != DataChecksum.Type.MIXED
								&& crcType != ct) {
							// if crc types are mixed in a file
							crcType = DataChecksum.Type.MIXED;
						}

						done = true;
					} catch (InvalidBlockTokenException ibte) {
						if (i > lastRetriedIndex) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
										+ "for file "
										+ fileInfo.getFilepath()
										+ " for block "
										+ block
										+ " from datanode "
										+ datanodes[j]
										+ ". Will retry the block once.");
							}
							lastRetriedIndex = i;
							done = true; // actually it's not done; but we'll
											// retry
							i--; // repeat at i-th block
							break;
						}
					} catch (IOException ie) {
						LOG.warn("src=" + fileInfo.getFilepath()
								+ ", datanodes[" + j + "]=" + datanodes[j], ie);
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
		 * Get the checksum of a file.
		 * 
		 * @param src
		 *            The file path
		 * @return The checksum
		 */
		public void getFileAdaptiveChecksum(int dataTransferVersion,
				FileInfo fileInfo, ClientProtocol namenode,
				ProtocolProxy<ClientProtocol> namenodeProxy,
				SocketFactory socketFactory, int socketTimeout, int chunkSize,
				int bminRatio, int bmaxRatio) throws IOException {
			LOG.info("getFileAdaptiveChecksum start " + fileInfo.getFilepath());
			final DataOutputBuffer md5out = new DataOutputBuffer();
			int lastRetriedIndex = -1;
			dataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;
			int bytesPerCRC = -1;
			DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
			long crcPerBlock = 0;

			// get block checksum for each block
			for (int i = 0; i < fileInfo.getBlocks().size(); i++) {
				LocatedBlock lb = fileInfo.getBlocks().get(i).locatedBlock;
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

						// get block MD5
						int bytesPerChunk = chunkSize; // 1MB
						int bmin = bminRatio * bytesPerChunk;
						int bmax = bmaxRatio * bytesPerChunk;
						new Sender(out).chunksAdaptiveChecksum(block,
								lb.getBlockToken(), bytesPerChunk, bmin, bmax);

						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
								throw new InvalidBlockTokenException();
							} else {
								throw new IOException("Bad response " + reply
										+ " for block " + block
										+ " from datanode " + datanodes[j]);
							}
						}

						OpChunksAdaptiveChecksumResponseProto checksumData = reply
								.getChunksAdaptiveChecksumResponse();

						fileInfo.getBlocks()
								.get(i)
								.setChecksumsAdaptive(
										checksumData.getChecksumsList());

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
						if (srcFileInfo.getBlocks().size() > 1 && i == 0) {
							crcPerBlock = cpb;
						}

						// read md5
						final MD5Hash md5 = new MD5Hash(checksumData.getMd5()
								.toByteArray());
						md5.write(md5out);
						LOG.info("Block CS : " + md5);
						// read crc-type
						final DataChecksum.Type ct;
						if (checksumData.hasCrcType()) {
							ct = PBHelper.convert(checksumData.getCrcType());
						} else {
							LOG.debug("Retrieving checksum from an earlier-version DataNode: "
									+ "inferring checksum by reading first byte");
							ct = inferChecksumTypeByReading(clientName,
									socketFactory, socketTimeout, lb,
									datanodes[j], encryptionKey,
									connectToDnViaHostname);
						}

						if (i == 0) { // first block
							crcType = ct;
						} else if (crcType != DataChecksum.Type.MIXED
								&& crcType != ct) {
							// if crc types are mixed in a file
							crcType = DataChecksum.Type.MIXED;
						}

						done = true;
					} catch (InvalidBlockTokenException ibte) {
						if (i > lastRetriedIndex) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
										+ "for file "
										+ fileInfo.getFilepath()
										+ " for block "
										+ block
										+ " from datanode "
										+ datanodes[j]
										+ ". Will retry the block once.");
							}
							lastRetriedIndex = i;
							done = true; // actually it's not done; but we'll
											// retry
							i--; // repeat at i-th block
							break;
						}
					} catch (IOException ie) {
						LOG.warn("src=" + fileInfo.getFilepath()
								+ ", datanodes[" + j + "]=" + datanodes[j], ie);
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

		private void calculateAdaptiveSegments(int chunkSize, int bminRatio,
				int bmaxRatio) throws IOException {
			LOG.info("calculateAdaptiveSegments start");
			getFileAdaptiveChecksum(dataTransferVersion, dstFileInfo,
					dstNamenode, dstNamenodeProtocolProxy, socketFactory,
					socketTimeout, chunkSize, bminRatio, bmaxRatio);
			getFileAdaptiveChecksum(dataTransferVersion, srcFileInfo,
					srcNamenode, srcNamenodeProtocolProxy, socketFactory,
					socketTimeout, chunkSize, bminRatio, bmaxRatio);
			HashMap<ByteString, ChecksumStrongProto> dstChecksums = new HashMap<ByteString, ChecksumStrongProto>();
			int dstBlockIndex = 0;
			for (BlockInfo bi : dstFileInfo.getBlocks()) {
				for (ChecksumStrongProto csp : bi.getChecksumsAdaptive()) {
					dstChecksums.put(
							csp.getMd5(),
							ChecksumStrongProto.newBuilder()
									.setIndex(dstBlockIndex)
									.setOffset(csp.getOffset())
									.setLength(csp.getLength())
									.setMd5(csp.getMd5()).build());
				}
				dstBlockIndex++;
			}

			int index = 0;
			for (BlockInfo bi : srcFileInfo.getBlocks()) {
				for (ChecksumStrongProto csp : bi.getChecksumsAdaptive()) {
					if (!dstChecksums.containsKey(csp.getMd5())) {
						bi.segments.add(SegmentProto.newBuilder().setIndex(-1)
								.setLength(csp.getLength())
								.setOffset(csp.getOffset()).build());
					} else {
						ChecksumStrongProto dstCsp = dstChecksums.get(csp
								.getMd5());
						LOG.info("[Found] block " + index + " segment "
								+ csp.getIndex() + " offset " + csp.getOffset()
								+ " length " + csp.getLength());
						bi.segments.add(SegmentProto.newBuilder()
								.setIndex(dstCsp.getIndex())
								.setOffset(dstCsp.getOffset())
								.setLength(dstCsp.getLength()).build());
					}
				}
				index++;
			}

			int cSegments = 0;
			for (BlockInfo bi : srcFileInfo.getBlocks()) {
				cSegments += bi.getSegments().size();
			}

			LOG.info("File Segments Number : " + cSegments);
		}

		private void calculateSegments(int chunkSize) {
			LOG.info("calculateSegments start");
			List<Integer> simples = new LinkedList<Integer>();
			List<byte[]> md5s = new LinkedList<byte[]>();

			for (BlockInfo bi : dstFileInfo.getBlocks()) {
				for (ChecksumPair cp : bi.getChecksums()) {
					simples.add(cp.getSimple());
					md5s.add(cp.getMd5());
				}
			}

			// 去掉最后一个chunk的checksum，防止其未达到chunksize
			if (simples.size() > 0) {
				simples.remove(simples.size() - 1);
				md5s.remove(md5s.size() - 1);
			}

			int index = 0;
			for (BlockInfo bi : srcFileInfo.getBlocks()) {
				DatanodeInfo[] datanodes = bi.getLocatedBlock().getLocations();
				final int timeout = 3000 * datanodes.length + socketTimeout;
				for (int j = 0; j < datanodes.length; j++) {
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

						// call calculateSegments
						int bytesPerChunk = chunkSize;
						new Sender(out).calculateSegments(bi.getLocatedBlock()
								.getBlock(), bi.getLocatedBlock()
								.getBlockToken(), clientName, simples, md5s,
								bytesPerChunk);

						// read reply
						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
								throw new InvalidBlockTokenException();
							} else {
								throw new IOException("Bad response " + reply
										+ " for block "
										+ bi.getLocatedBlock().getBlock()
										+ " from datanode " + datanodes[j]);
							}
						}

						OpCalculateSegmentsResponseProto segmentsData = reply
								.getCalculateSegmentsResponse();

						LinkedList<SegmentProto> segments = new LinkedList<SegmentProto>(
								segmentsData.getSegmentsList());
						bi.setSegments(segments);
						break;

					} catch (InvalidBlockTokenException ibte) {

					} catch (IOException ie) {

					} finally {
						IOUtils.closeStream(in);
						IOUtils.closeStream(out);
					}
				}

				int offset = 0;
				for (SegmentProto sp : bi.getSegments()) {
					if (sp.getIndex() != -1) {
						LOG.info("[Found] block " + index + " segment "
								+ offset + " offset " + offset * chunkSize
								+ " length " + chunkSize);
					}
					offset++;
				}
				index++;
			}

			int cSegments = 0;
			for (BlockInfo bi : srcFileInfo.getBlocks()) {
				cSegments += bi.getSegments().size();
			}

			LOG.info("File Segments Number : " + cSegments);
		}

		/**
		 * 传送一个block所需的segments，与updateBlock配合使用
		 * 
		 * @param blockInfo
		 *            所需要传送的block信息
		 * @param addedBlock
		 *            所传送的block在目标文件中对应的block
		 * @throws AccessControlException
		 * @throws FileNotFoundException
		 * @throws UnresolvedLinkException
		 * @throws IOException
		 */
		private void sendSegments(BlockInfo blockInfo, LocatedBlock addedBlock,
				int chunkSize) throws AccessControlException,
				FileNotFoundException, UnresolvedLinkException, IOException {
			LOG.info("sendSegments for block "
					+ blockInfo.getLocatedBlock().getBlock() + " start.");

			long blockSize = dstNamenode.getFileInfo(dstFileInfo.getFilepath())
					.getBlockSize();
			long chunksPerBlock = blockSize / chunkSize;

			for (int j = 0; j < blockInfo.getSegments().size(); j++) {
				SegmentProto segment = blockInfo.getSegments().get(j);
				DatanodeInfo[] srcDatanodes = null;
				DatanodeInfo[] dstDatanodes = null;
				LocatedBlock block = null;
				String segmentName = null;
				String blockDirName = addedBlock.getBlock().getBlockId() + "_"
						+ addedBlock.getBlock().getGenerationStamp();
				long offset = 0;
				long length = 0;
				// 如果dstFile中没有这个segment
				if (segment.getIndex() == -1) {
					srcDatanodes = blockInfo.getLocatedBlock().getLocations();
					block = blockInfo.getLocatedBlock();
					offset = segment.getOffset();
					length = segment.getLength();
					segmentName = String.format("%064d", offset) + "_"
							+ String.format("%064d", length);
					dstDatanodes = addedBlock.getLocations();
					/*
					 * LOG.info("SendSegment from srcFile "+
					 * "index : "+segment.getIndex()+
					 * "; offset : "+segment.getOffset()+
					 * "; length : "+segment.getLength());
					 */
				} else {
					srcDatanodes = dstFileInfo.getBlocks()
							.get((int) (segment.getIndex() / chunksPerBlock))
							.getLocatedBlock().getLocations();
					block = dstFileInfo.getBlocks()
							.get((int) (segment.getIndex() / chunksPerBlock))
							.getLocatedBlock();
					offset = segment.getIndex() % chunksPerBlock * chunkSize;
					length = chunkSize;
					segmentName = String.format("%064d", segment.getOffset())
							+ "_" + String.format("%064d", segment.getLength());
					dstDatanodes = addedBlock.getLocations();
					LOG.info("SendSegment from dstFile " + "index : "
							+ segment.getIndex() + "; offset : "
							+ segment.getOffset() + "; length : "
							+ segment.getLength());
				}

				final int timeout = 3000 + socketTimeout;

				for (int k = 0; k < srcDatanodes.length; k++) {
					DataOutputStream out = null;
					DataInputStream in = null;

					try {
						// connect to a datanode
						IOStreamPair pair = connectToDN(socketFactory,
								connectToDnViaHostname, getDataEncryptionKey(),
								srcDatanodes[k], timeout);
						out = new DataOutputStream(new BufferedOutputStream(
								pair.out, HdfsConstants.SMALL_BUFFER_SIZE));
						in = new DataInputStream(pair.in);

						// call sendSegment
						new Sender(out).sendSegment(block.getBlock(),
								block.getBlockToken(), clientName,
								segment.getOffset(), segment.getLength(), true,
								true, segmentName, blockDirName, dstDatanodes);

						// read reply
						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							LOG.warn("Bad response " + reply + " for block "
									+ blockInfo.getLocatedBlock().getBlock()
									+ " from datanode " + srcDatanodes[k]);
						} else {
							break;
						}

					} catch (InvalidBlockTokenException ibte) {

					} catch (IOException ie) {

					} finally {
						IOUtils.closeStream(in);
						IOUtils.closeStream(out);
					}
				}

			}// for segment
		}

		/**
		 * 传送一个block所需的segments，与updateBlock配合使用
		 * 
		 * @param blockInfo
		 *            所需要传送的block信息
		 * @param addedBlock
		 *            所传送的block在目标文件中对应的block
		 * @throws AccessControlException
		 * @throws FileNotFoundException
		 * @throws UnresolvedLinkException
		 * @throws IOException
		 */
		private void sendAdaptiveSegments(BlockInfo blockInfo,
				LocatedBlock addedBlock) throws AccessControlException,
				FileNotFoundException, UnresolvedLinkException, IOException {
			LOG.warn("sendAdaptiveSegments for block "
					+ blockInfo.getLocatedBlock().getBlock() + " start.");

			long dstOffset = 0;

			for (int j = 0; j < blockInfo.getSegments().size(); j++) {
				SegmentProto segment = blockInfo.getSegments().get(j);
				DatanodeInfo[] srcDatanodes = null;
				DatanodeInfo[] dstDatanodes = null;
				LocatedBlock block = null;
				String segmentName = null;
				String blockDirName = addedBlock.getBlock().getBlockId() + "_"
						+ addedBlock.getBlock().getGenerationStamp();
				long offset = 0;
				long length = 0;
				// 如果dstFile中没有这个segment
				if (segment.getIndex() == -1) {
					srcDatanodes = blockInfo.getLocatedBlock().getLocations();
					block = blockInfo.getLocatedBlock();
					offset = segment.getOffset();
					length = segment.getLength();
					segmentName = String.format("%064d", offset) + "_"
							+ String.format("%064d", length);
					dstDatanodes = addedBlock.getLocations();
					dstOffset += length;
					/*
					 * LOG.info("SendSegment from srcFile "+
					 * "index : "+segment.getIndex()+
					 * "; offset : "+segment.getOffset()+
					 * "; length : "+segment.getLength());
					 */
				} else {
					srcDatanodes = dstFileInfo.getBlocks()
							.get((int) segment.getIndex()).getLocatedBlock()
							.getLocations();
					block = dstFileInfo.getBlocks()
							.get((int) segment.getIndex()).getLocatedBlock();
					offset = segment.getOffset();
					length = segment.getLength();
					segmentName = String.format("%064d", dstOffset) + "_"
							+ String.format("%064d", segment.getLength());
					dstOffset += segment.getLength();
					dstDatanodes = addedBlock.getLocations();
					LOG.info("SendSegment from dstFile " + "index : "
							+ segment.getIndex() + "; offset : "
							+ segment.getOffset() + "; length : "
							+ segment.getLength());
				}

				final int timeout = 3000 + socketTimeout;

				for (int k = 0; k < srcDatanodes.length; k++) {
					DataOutputStream out = null;
					DataInputStream in = null;

					try {
						// connect to a datanode
						IOStreamPair pair = connectToDN(socketFactory,
								connectToDnViaHostname, getDataEncryptionKey(),
								srcDatanodes[k], timeout);
						out = new DataOutputStream(new BufferedOutputStream(
								pair.out, HdfsConstants.SMALL_BUFFER_SIZE));
						in = new DataInputStream(pair.in);

						// call sendSegment
						new Sender(out).sendSegment(block.getBlock(),
								block.getBlockToken(), clientName, offset,
								length, true, true, segmentName, blockDirName,
								dstDatanodes);

						// read reply
						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							LOG.warn("Bad response " + reply + " for block "
									+ blockInfo.getLocatedBlock().getBlock()
									+ " from datanode " + srcDatanodes[k]);
						} else {
							break;
						}

					} catch (InvalidBlockTokenException ibte) {

					} catch (IOException ie) {

					} finally {
						IOUtils.closeStream(in);
						IOUtils.closeStream(out);
					}
				}

			}// for segment
		}

		/**
		 * 用于构建新的block
		 * 
		 * @param block
		 *            目标文件块
		 * @throws IOException
		 * @throws UnresolvedLinkException
		 * @throws FileNotFoundException
		 * @throws AccessControlException
		 */
		private void updateBlock(LocatedBlock block)
				throws AccessControlException, FileNotFoundException,
				UnresolvedLinkException, IOException {
			LOG.info("updateBlock for block " + block.getBlock() + " start.");

			DatanodeInfo[] datanodes = block.getLocations();
			final int timeout = 3000 * datanodes.length + socketTimeout;
			for (int j = 0; j < datanodes.length; j++) {
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

					// call updateBlock
					LOG.info("updateBlock " + block.getBlock());
					new Sender(out).updateBlock(block.getBlock(),
							block.getBlockToken());

					// read reply
					final BlockOpResponseProto reply = BlockOpResponseProto
							.parseFrom(PBHelper.vintPrefixed(in));

					if (reply.getStatus() != Status.SUCCESS) {
						LOG.warn("Bad response " + reply + " for block "
								+ block.getBlock() + " from datanode "
								+ datanodes[j]);
					}

				} catch (InvalidBlockTokenException ibte) {

				} catch (IOException ie) {

				} finally {
					IOUtils.closeStream(in);
					IOUtils.closeStream(out);
				}
			}
		}

		private LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
				String src, long start, long length, boolean supportMetaInfo)
				throws IOException {
			try {
				return namenode.getBlockLocations(src, start, length);
			} catch (RemoteException re) {
				throw re.unwrapRemoteException(AccessControlException.class,
						FileNotFoundException.class);
			}
		}

		/**
		 * Connect to the given datanode's datantrasfer port, and return the
		 * resulting IOStreamPair. This includes encryption wrapping, etc.
		 */
		private IOStreamPair connectToDN(SocketFactory socketFactory,
				boolean connectToDnViaHostname,
				DataEncryptionKey encryptionKey, DatanodeInfo dn, int timeout)
				throws IOException {
			boolean success = false;
			Socket sock = null;
			try {
				sock = socketFactory.createSocket();
				String dnAddr = dn.getXferAddr(connectToDnViaHostname);
				NetUtils.connect(sock, NetUtils.createSocketAddr(dnAddr),
						timeout);
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
		 * Infer the checksum type for a replica by sending an OP_READ_BLOCK for
		 * the first byte of that replica. This is used for compatibility with
		 * older HDFS versions which did not include the checksum type in
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
		private Type inferChecksumTypeByReading(String clientName,
				SocketFactory socketFactory, int socketTimeout,
				LocatedBlock lb, DatanodeInfo dn,
				DataEncryptionKey encryptionKey, boolean connectToDnViaHostname)
				throws IOException {
			IOStreamPair pair = connectToDN(socketFactory,
					connectToDnViaHostname, encryptionKey, dn, socketTimeout);

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

				return PBHelper.convert(reply.getReadOpChecksumInfo()
						.getChecksum().getType());
			} finally {
				IOUtils.cleanup(null, pair.in, pair.out);
			}
		}

		private void sleepFor(long ms) throws IOException {
			try {
				Thread.sleep(ms);
			} catch (InterruptedException e) {
				throw new IOException(e.getMessage());
			}
		}

		/**
		 * 新建一个临时文件，文件名为dstFilePath+".rsync"，在这个文件中恢复新的文件内容，再用这个文件替代原有文件
		 * 
		 * @param filePath
		 * @param src
		 * @throws IOException
		 *             TODO:应该让重建的所有block尽量存放在dstFile对应block所在datanode上
		 * @throws InterruptedException
		 */
		private void updateDstFile(int chunkSize) throws IOException,
				InterruptedException {
			String tmpFilePath = dstFileInfo.getFilepath() + ".rsync";
			LOG.info("updateDstFile " + tmpFilePath);
			// dstDfs.create(new Path(tmpFilePath)).close();
			short replication = Short.parseShort(conf.get("dfs.replication",
					"1"));
			long blockSize = Long.parseLong(conf.get("dfs.blocksize",
					"134217728"));
			EnumSetWritable<CreateFlag> flag = new EnumSetWritable<CreateFlag>(
					EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
			HdfsFileStatus status = dstNamenode.create(tmpFilePath,
					FsPermission.getFileDefault(), clientName, flag,
					true/* createParent */, replication, blockSize);

			long fileId = status.getFileId();
			// TODO:用一次append操作只是为了能够得到lease
			// LocatedBlock lastBlock = srcNamenode.append(tmpFilePath,
			// clientName);//文件没有内容的时候，append操作返回的是null！！
			// if(lastBlock != null) LOG.warn("append empty file return " +
			// lastBlock.getBlock());
			// else LOG.warn("append empty file return null");
			ExtendedBlock lastBlock = null;
			long startTime = System.currentTimeMillis();
			long BLK_WAIT_TIME = 30000;
			long WAIT_SLEEP_TIME = 1000;
			for (int i = 0; i < srcFileInfo.getBlocks().size(); i++) {
				LocatedBlock currentBlock = null;
				do{
					try {
						currentBlock = dstNamenode.addBlock(tmpFilePath,
								clientName, lastBlock, (DatanodeInfo[]) null,
								fileId, (String[]) null);
					} catch (RemoteException re) {
						if (re.unwrapRemoteException() instanceof NotReplicatedYetException) {
							if (System.currentTimeMillis() - startTime > BLK_WAIT_TIME) {
								throw re;
							}
							LOG.warn("File not replicated yet : " + dstFileInfo.getFilepath()
									+ " will retry in " + WAIT_SLEEP_TIME / 1000
									+ " seconds");
							sleepFor(WAIT_SLEEP_TIME);
							continue;
						} else {
							LOG.warn(re);
							throw re;
						}
					}
				}while(currentBlock == null);

				LOG.info("Add new block " + currentBlock.getBlock());

				// sendSegments and updateBlock
				sendSegments(srcFileInfo.getBlocks().get(i), currentBlock,
						chunkSize);
				updateBlock(currentBlock);
				currentBlock.getBlock().setNumBytes(
						srcFileInfo.getBlocks().get(i).getLocatedBlock()
								.getBlock().getNumBytes());
				lastBlock = currentBlock.getBlock();
				LOG.info("lastBlock " + lastBlock.getBlockName() + " size "
						+ lastBlock.getNumBytes());
			}

			int count = 0;
			boolean completed = false;
			while ((completed = dstNamenode.complete(tmpFilePath, clientName,
					lastBlock, fileId)) != true && count < 10) {
				LOG.info("File " + tmpFilePath + " can not complete");
				count++;
				Thread.sleep(1000);
			}
			LOG.info("File " + tmpFilePath + " complete " + completed);
		}

		/**
		 * 新建一个临时文件，文件名为dstFilePath+".rsync"，在这个文件中恢复新的文件内容，再用这个文件替代原有文件
		 * 
		 * @param filePath
		 * @param src
		 * @throws IOException
		 *             TODO:应该让重建的所有block尽量存放在dstFile对应block所在datanode上
		 * @throws InterruptedException
		 */
		private void updateAdaptiveDstFile() throws IOException,
				InterruptedException {
			String tmpFilePath = dstFileInfo.getFilepath() + ".rsync";
			LOG.info("updateAdaptiveDstFile " + tmpFilePath);
			// dstDfs.create(new Path(tmpFilePath)).close();
			short replication = Short.parseShort(conf.get("dfs.replication",
					"1"));
			long blockSize = Long.parseLong(conf.get("dfs.blocksize",
					"134217728"));
			EnumSetWritable<CreateFlag> flag = new EnumSetWritable<CreateFlag>(
					EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
			HdfsFileStatus status = dstNamenode.create(tmpFilePath,
					FsPermission.getFileDefault(), clientName, flag,
					true/* createParent */, replication, blockSize);

			long fileId = status.getFileId();
			// TODO:用一次append操作只是为了能够得到lease
			// LocatedBlock lastBlock = srcNamenode.append(tmpFilePath,
			// clientName);//文件没有内容的时候，append操作返回的是null！！
			// if(lastBlock != null) LOG.warn("append empty file return " +
			// lastBlock.getBlock());
			// else LOG.warn("append empty file return null");
			ExtendedBlock lastBlock = null;
			long startTime = System.currentTimeMillis();
			long BLK_WAIT_TIME = 30000;
			long WAIT_SLEEP_TIME = 1000;
			for (int i = 0; i < srcFileInfo.getBlocks().size(); i++) {
				LocatedBlock currentBlock = null;
				do{
					try {
						currentBlock = dstNamenode.addBlock(tmpFilePath,
								clientName, lastBlock, (DatanodeInfo[]) null,
								fileId, (String[]) null);
					} catch (RemoteException re) {
						if (re.unwrapRemoteException() instanceof NotReplicatedYetException) {
							if (System.currentTimeMillis() - startTime > BLK_WAIT_TIME) {
								throw re;
							}
							LOG.warn("File not replicated yet : " + dstFileInfo.getFilepath()
									+ " will retry in " + WAIT_SLEEP_TIME / 1000
									+ " seconds");
							sleepFor(WAIT_SLEEP_TIME);
							continue;
						} else {
							LOG.warn(re);
							throw re;
						}
					}
				}while(currentBlock == null);
				
				LOG.info("Add new block " + currentBlock.getBlock());

				// sendSegments and updateBlock
				sendAdaptiveSegments(srcFileInfo.getBlocks().get(i),
						currentBlock);
				updateBlock(currentBlock);
				currentBlock.getBlock().setNumBytes(
						srcFileInfo.getBlocks().get(i).getLocatedBlock()
								.getBlock().getNumBytes());
				lastBlock = currentBlock.getBlock();
				LOG.info("lastBlock " + lastBlock.getBlockName() + " size "
						+ lastBlock.getNumBytes());
			}

			int count = 0;
			boolean completed = false;
			while ((completed = dstNamenode.complete(tmpFilePath, clientName,
					lastBlock, fileId)) != true && count < 10) {
				LOG.info("File " + tmpFilePath + " can not complete");
				count++;
				Thread.sleep(1000);
			}
			LOG.info("File " + tmpFilePath + " complete " + completed);
		}

		/**
		 * Get server default values for a number of configuration params.
		 * 
		 * @see ClientProtocol#getServerDefaults()
		 */
		public FsServerDefaults getServerDefaults() throws IOException {
			long now = Time.now();
			if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
				serverDefaults = srcNamenode.getServerDefaults();
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
						encryptionKey = srcNamenode.getDataEncryptionKey();
					}
					return encryptionKey;
				}
			} else {
				return null;
			}
		}

		public boolean isMetaInfoSupported(ProtocolProxy<ClientProtocol> proxy)
				throws IOException {
			return proxy != null
					&& proxy.isMethodSupported("openAndFetchMetaInfo",
							String.class, long.class, long.class);
		}

		public void printFileInfo(FileInfo fileInfo) {
			LOG.info("File Info of " + fileInfo.getFilepath());
			LOG.info("\tblock count : " + fileInfo.getBlocks().size());
			for (BlockInfo bi : fileInfo.getBlocks()) {
				LOG.info("\tblock id : "
						+ bi.getLocatedBlock().getBlock().getBlockId()
						+ "; genstamp : "
						+ bi.getLocatedBlock().getBlock().getGenerationStamp()
						+ "; checksum size :" + bi.getChecksums().size()
						+ "; segment size : " + bi.getSegments().size());
			}
		}

		public void run(int method, int chunkSize, int bminRatio, int bmaxRatio)
				throws IOException, InterruptedException {
			if (method == 1) {
				getSDFileInfo();
				getSDFileChecksum(chunkSize);
				calculateSegments(chunkSize);
				updateDstFile(chunkSize);
			} else if (method == 2) {
				getSDFileInfo();
				calculateAdaptiveSegments(chunkSize, bminRatio, bmaxRatio);
				updateAdaptiveDstFile();
			} else {
				System.out.println("Unrecognized method " + method);
			}
		}

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
				this.srcNamenode = srcDfs.getClient().getNamenode();
				this.dstNamenode = dstDfs.getClient().getNamenode();
			}
		}
	}

	private static void printUsage() {
		HelpFormatter formatter = new HelpFormatter();
		Options options = new Options();
		formatter
				.printHelp(
						"Usage : RsyncCopy method[1/2] chunkSize bmin bmax <srcs....> <dst>",
						options);
		System.exit(0);
	}

	protected void checkOpen() throws IOException {
		if (!clientRunning) {
			IOException result = new IOException("Filesystem closed");
			throw result;
		}
	}

	public void run(int method, int chunkSize, int bminRatio, int bmaxRatio)
			throws IOException, InterruptedException {
		getNameNode();
		long uniqueId = 0;
		RsyncCopyFile testCopyFile = new RsyncCopyFile(srcNamenode,
				srcNamenodeProtocolProxy, srcPath, dstNamenode,
				dstNamenodeProtocolProxy, dstPath, conf, stats, uniqueId);
		testCopyFile.run(method, chunkSize, bminRatio, bmaxRatio);
	}

	public static void main(String args[]) throws Exception {
		/*
		 * if (args.length < 6) { printUsage(); } int method =
		 * Integer.parseInt(args[0]); int chunkSize = Integer.parseInt(args[1]);
		 * int bminRatio = Integer.parseInt(args[2]); int bmaxRatio =
		 * Integer.parseInt(args[3]); String srcPath = args[4]; String dstPath =
		 * args[5]; RsyncCopy rc = new RsyncCopy(srcPath,dstPath);
		 */
		File input = new File(args[0]);
		if (!input.canRead()) {
			LOG.error("File " + args[0] + " cannot read.");
		} else {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String command = null;
			while ((command = reader.readLine()) != null) {
				String[] paras = command.split(" ");
				int method = Integer.parseInt(paras[0]);
				int chunkSize = Integer.parseInt(paras[1]);
				int bminRatio = Integer.parseInt(paras[2]);
				int bmaxRatio = Integer.parseInt(paras[3]);
				String srcPath = paras[4];
				String dstPath = paras[5];
				RsyncCopy rc = new RsyncCopy(srcPath, dstPath);
				LOG.info("[TEST]" + " METHOD " + method + " CHUNKSIZE "
						+ chunkSize + " BMIN " + bminRatio + " srcPath "
						+ srcPath + " dstPath " + dstPath);
				rc.run(method, chunkSize, bminRatio, bmaxRatio);
			}
		}
		System.exit(0);
	}
}
