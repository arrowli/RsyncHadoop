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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

import javax.net.SocketFactory;
import javax.swing.text.Segment;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.LeaseRenewer;
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
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumPairProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCalculateSegmentsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.SegmentProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

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

	public RsyncCopy(String srcPath,String dstPath) throws IOException {
		conf = new Configuration();
		this.srcPath = new Path(srcPath);
		this.dstPath = new Path(dstPath);
	
		srcDfs = (DistributedFileSystem)this.srcPath.getFileSystem(conf);
		dstDfs = (DistributedFileSystem)this.dstPath.getFileSystem(conf);
		RsyncCopyInit(NameNode.getAddress(conf), null, null,0);
	}
	/**
	 * Create a new DFSClient connected to the given nameNodeAddr or
	 * rpcNamenode. Exactly one of nameNodeAddr or rpcNamenode must be null.
	 */
	private void RsyncCopyInit(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode, FileSystem.Statistics stats, long uniqueId) throws IOException {
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
	}
	
	private class RsyncCopyFile{
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
		private int chunkSize;
		
		private class ChecksumPair{
			private Integer simple;
			private byte[] md5;
			public ChecksumPair(Integer simple,byte[] bs){
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
		
		private class BlockInfo{
			private LocatedBlock locatedBlock;
			private LinkedList<ChecksumPair> checksums;
			private LinkedList<SegmentProto> segments;
			public BlockInfo(LocatedBlock locatedBlock){
				this.locatedBlock = locatedBlock;
				this.checksums = new LinkedList<ChecksumPair>();
				this.segments = new LinkedList<SegmentProto>();
			}
			public BlockInfo(LocatedBlock locatedBlock,LinkedList<ChecksumPair> checksums,LinkedList<SegmentProto> segments){
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
			public void addChecksum(ChecksumPair checksum){
				this.checksums.add(checksum);
			}
			public LinkedList<SegmentProto> getSegments() {
				return segments;
			}
			public void setSegments(LinkedList<SegmentProto> segments) {
				this.segments = segments;
			}
		}
		
		private class FileInfo{
			private List<BlockInfo> blocks;
			private String filepath;
			private long fileSize;

			public FileInfo(String filepath){
				this.setFilepath(filepath);
				this.blocks = new LinkedList<BlockInfo>();
			}
			
			public List<BlockInfo> getBlocks() {
				return blocks;
			}

			public void setBlocks(List<BlockInfo> blocks) {
				this.blocks = blocks;
			}
			
			public void addBlock(BlockInfo block){
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
		private FileInfo newFileInfo;
		
		RsyncCopyFile(ClientProtocol srcNamenode,ProtocolProxy<ClientProtocol> srcNamenodeProtocolProxy,Path srcPath,
				ClientProtocol dstNamenode,ProtocolProxy<ClientProtocol> dstNamenodeProtocolProxy,Path dstPath,
				Configuration conf, FileSystem.Statistics stats, long uniqueId) throws IOException {
			this.srcNamenode = srcNamenode;
			this.srcNamenodeProtocolProxy = srcNamenodeProtocolProxy;
			this.srcPath = srcPath;
			this.dstNamenode = dstNamenode;
			this.dstNamenodeProtocolProxy = dstNamenodeProtocolProxy;
			this.dstPath = dstPath;
			
			this.conf = conf;
			this.stats = stats;
			this.connectToDnViaHostname = conf.getBoolean(
					DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
			this.socketFactory = NetUtils.getSocketFactory(conf,
					ClientProtocol.class);
			String taskId = conf.get("mapreduce.task.attempt.id");
			if (taskId != null) {
				this.clientName = "RsyncCopy_" + taskId + "_" + r.nextInt() + "_"
						+ Thread.currentThread().getId();
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
			this.chunkSize = 1024*1024;
		}
		/**
		 * Get the source file blocks information from NN
		 * Get the destination file blocks information from NN
		 * Create the new temp file in dst file system.
		 */
		void getSDFileInfo() throws IOException {
			LocatedBlocks srcLocatedBlocks = callGetBlockLocations(
					srcNamenode,srcPath.toString(),0,Long.MAX_VALUE,
					isMetaInfoSupported(srcNamenodeProtocolProxy));
			if (srcLocatedBlocks == null) {
				throw new IOException(
						"Null block locations, mostly because non-existent file "
								+ srcPath.toString());
			}
			LocatedBlocks dstLocatedBlocks = callGetBlockLocations(
					dstNamenode,dstPath.toString(),0,Long.MAX_VALUE,
					isMetaInfoSupported(dstNamenodeProtocolProxy));
			if (dstLocatedBlocks == null) {
				throw new IOException(
						"Null block locations, mostly because non-existent file "
								+ dstPath.toString());
			}

			this.srcFileInfo = new FileInfo(srcPath.toString());
			this.dstFileInfo = new FileInfo(dstPath.toString());
			
			this.srcFileInfo.setFileSize(srcLocatedBlocks.getFileLength());
			this.dstFileInfo.setFileSize(dstLocatedBlocks.getFileLength());
			
			for(LocatedBlock lb : srcLocatedBlocks.getLocatedBlocks()){
				srcFileInfo.addBlock(new BlockInfo(lb));
			}
			for(LocatedBlock lb : dstLocatedBlocks.getLocatedBlocks()){
				dstFileInfo.addBlock(new BlockInfo(lb));
			}
			
			this.newFileInfo = createNewFile(dstPath.toString()+".rsync",srcFileInfo);
		}
		/**
		 * Get the checksum of a file.
		 * 
		 * @param src
		 *            The file path
		 * @return The checksum
		 * @see DistributedFileSystem#getFileChecksum(Path)
		 */
		void getSDFileChecksum() throws IOException {
			checkOpen();
			getFileChecksum(dataTransferVersion, srcFileInfo, srcNamenode,
					srcNamenodeProtocolProxy, socketFactory, socketTimeout);
			getFileChecksum(dataTransferVersion, dstFileInfo, dstNamenode,
					dstNamenodeProtocolProxy, socketFactory, socketTimeout);
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
				SocketFactory socketFactory, int socketTimeout) throws IOException {
			LOG.warn("getFileShecksum start");
			final DataOutputBuffer md5out = new DataOutputBuffer();
			int namespaceId = 0;
			boolean refetchBlocks = false;
			int lastRetriedIndex = -1;
			dataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;
			int bytesPerCRC = -1;
			DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
			long crcPerBlock = 0;

			// get block checksum for each block
			for (int i = 0; i < srcFileInfo.getBlocks().size(); i++) {
				LocatedBlock lb = srcFileInfo.getBlocks().get(i).locatedBlock;
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
						if (srcFileInfo.getBlocks().size() > 1 && i == 0) {
							crcPerBlock = cpb;
						}

						final List<ChecksumPairProto> checksums = checksumData.getChecksumsList();
						
						LOG.warn("checksum size : "+checksumData.getBytesPerChunk());
						LOG.warn("checksum counts : "+checksumData.getChunksPerBlock());
						LOG.warn("checksum list:");
						for(ChecksumPairProto cs : checksums){
							final MD5Hash md5s = new MD5Hash(cs.getMd5().toByteArray());
							LOG.warn("Simple CS : "+
									Integer.toHexString(cs.getSimple())+
									" ; MD5 CS : "+md5s);
							fileInfo.getBlocks().get(i).getChecksums().add(
									new ChecksumPair(cs.getSimple(),md5s.getDigest()));
						}
						
						// read md5
						final MD5Hash md5 = new MD5Hash(checksumData.getMd5()
								.toByteArray());
						md5.write(md5out);
						LOG.warn("Block CS : "+md5);
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
										+ fileInfo.getFilepath()
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
						LOG.warn("src=" + fileInfo.getFilepath() + ", datanodes[" + j + "]="
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
		
		private void calculateSegments(){
			LOG.warn("calculateSegments start");
			List<Integer> simples = new LinkedList<Integer>();
			List<byte[]> md5s = new LinkedList<byte[]>();
			
			for(BlockInfo bi : dstFileInfo.getBlocks()){
				for(ChecksumPair cp : bi.getChecksums()){
					simples.add(cp.getSimple());
					md5s.add(cp.getMd5());
				}
			}
			
			//去掉最后一个chunk的checksum，防止其未达到chunksize
			simples.remove(simples.size()-1);
			md5s.remove(md5s.size()-1);
			
			for(BlockInfo bi : srcFileInfo.getBlocks()){
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
						new Sender(out).calculateSegments(
								bi.getLocatedBlock().getBlock(), 
								bi.getLocatedBlock().getBlockToken(), 
								clientName, simples, md5s);

						//read reply
						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
								throw new InvalidBlockTokenException();
							} else {
								throw new IOException("Bad response " + reply
										+ " for block " + bi.getLocatedBlock().getBlock() + " from datanode "
										+ datanodes[j]);
							}
						}

						OpCalculateSegmentsResponseProto segmentsData = reply
								.getCalculateSegmentsResponse();

						LinkedList<SegmentProto> segments = new LinkedList<SegmentProto>(segmentsData.getSegmentsList());
						LOG.warn("Block "+bi.getLocatedBlock().getBlock().getBlockName()+
								" divide into "+segments.size()+" segments.");
						bi.setSegments(segments);
						break;
						
					} catch (InvalidBlockTokenException ibte) {
						
					} catch (IOException ie) {
						
					}finally {
						IOUtils.closeStream(in);
						IOUtils.closeStream(out);
					}
				}
			}
		}
		
		private void sendSegments() throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException{
			LOG.warn("sendSegment start.");
			long blockSize = dstNamenode.getFileInfo(newFileInfo.getFilepath()).getBlockSize();
			long chunksPerBlock = blockSize/chunkSize;
			if(newFileInfo.getBlocks().size() != srcFileInfo.getBlocks().size()){
				throw new IOException("newFileInfo size not match srcFileInfo.");
			}
			for(int i = 0 ; i < srcFileInfo.getBlocks().size() ; i++){
				BlockInfo bi = srcFileInfo.getBlocks().get(i);
				for(int j = 0 ; j < bi.getSegments().size() ; j++){
					SegmentProto segment = bi.getSegments().get(j);
					DatanodeInfo[] srcDatanodes = null;
					DatanodeInfo[] dstDatanodes = null;
					LocatedBlock block = null;
					String segmentName = null;
					String blockDirName = newFileInfo.getBlocks().get(i).getLocatedBlock().getBlock().getBlockId()
							+"_"+newFileInfo.getBlocks().get(i).getLocatedBlock().getBlock().getGenerationStamp();
					long offset = 0;
					long length = 0;
					//如果dstFile中没有这个segment
					if(segment.getIndex() == -1){
						srcDatanodes = bi.getLocatedBlock().getLocations();
						block = bi.getLocatedBlock();
						offset = segment.getOffset();
						length = segment.getLength();
						segmentName = String.format("%064d", offset)+"_"+String.format("%064d", length);
						dstDatanodes = newFileInfo.getBlocks()
								.get((int)(segment.getOffset()/blockSize))
								.getLocatedBlock().getLocations();
						
						LOG.warn("SendSegment from srcFile "+
								"index : "+segment.getIndex()+
								"; offset : "+segment.getOffset()+
								"; length : "+segment.getLength());
					}else{
						srcDatanodes = dstFileInfo.getBlocks()
								.get((int)(segment.getIndex()/chunksPerBlock))
								.getLocatedBlock().getLocations();
						block = dstFileInfo.getBlocks()
								.get((int)(segment.getIndex()/chunksPerBlock))
								.getLocatedBlock();
						offset = segment.getIndex()%chunksPerBlock*chunkSize;
						length = chunkSize;
						segmentName = String.format("%064d", segment.getOffset())+"_"+
								String.format("%064d", segment.getLength());
						dstDatanodes = newFileInfo.getBlocks()
								.get((int)(segment.getOffset()/blockSize))
								.getLocatedBlock().getLocations();
						LOG.warn("SendSegment from dstFile "+
								"index : "+segment.getIndex()+
								"; offset : "+segment.getOffset()+
								"; length : "+segment.getLength());
					}
					
					final int timeout = 3000 + socketTimeout;
					boolean noBreak = true;
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
									block.getBlockToken(), 
									clientName, 
									segment.getOffset(), 
									segment.getLength(), true, true,
									segmentName,
									blockDirName,
									dstDatanodes);
	
							//read reply
							final BlockOpResponseProto reply = BlockOpResponseProto
									.parseFrom(PBHelper.vintPrefixed(in));
	
							if (reply.getStatus() != Status.SUCCESS) {
								LOG.warn("Bad response " + reply
											+ " for block " + bi.getLocatedBlock().getBlock() + " from datanode "
											+ srcDatanodes[k]);
							}else{
								break;
							}
							
						} catch (InvalidBlockTokenException ibte) {
							
						} catch (IOException ie) {
							
						}finally {
							IOUtils.closeStream(in);
							IOUtils.closeStream(out);
						}
					}
					
				}//for segment
			}//for block
		}
		
		private void updateBlocks(){
			LOG.warn("updateBlock start.");
			for(BlockInfo bi : newFileInfo.getBlocks()){
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

						
						
						// call updateBlock
						LOG.warn("updateBlock "+bi.getLocatedBlock().getBlock());
						new Sender(out).updateBlock(bi.getLocatedBlock().getBlock(), 
								bi.getLocatedBlock().getBlockToken());

						//read reply
						final BlockOpResponseProto reply = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(in));

						if (reply.getStatus() != Status.SUCCESS) {
							LOG.warn("Bad response " + reply
										+ " for block " + bi.getLocatedBlock().getBlock() + " from datanode "
										+ datanodes[j]);
						}
						
					} catch (InvalidBlockTokenException ibte) {
						
					} catch (IOException ie) {
						
					}finally {
						IOUtils.closeStream(in);
						IOUtils.closeStream(out);
					}
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
		private Type inferChecksumTypeByReading(String clientName,
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
		
		FileInfo createNewFile(String filePath,FileInfo src) throws IOException {
			FileInfo ret = new FileInfo(filePath);
			dstDfs.create(new Path(filePath)).close();
			long fileId = dstNamenode.getFileInfo(filePath).getFileId();
			//TODO:用一次append操作只是为了能够得到lease
			LocatedBlock lastBlock = srcNamenode.append(filePath, clientName);//文件没有内容的时候，append操作返回的是null！！
			for(int i = 0 ; i < src.getBlocks().size() ; i++){
				BlockInfo bi = src.getBlocks().get(i);
				DatanodeInfo[] datanodes = bi.getLocatedBlock().getLocations();
				String[] datanodesString = new String[datanodes.length];
				for(int j = 0 ; j < datanodes.length ; j++){
					datanodesString[j] = datanodes[j].getHostName();
				}
				lastBlock = dstNamenode.addBlock(filePath, 
						clientName, 
						lastBlock != null ? lastBlock.getBlock():null , 
						(DatanodeInfo[])null, 
						fileId, 
						datanodesString);
				LOG.warn("Add new block "+lastBlock.getBlock());
				ret.getBlocks().add(new BlockInfo(lastBlock));
			}
		
			//srcNamenode.complete(filePath, clientName , lastBlock.getBlock() , fileId);
			return ret;
		}
		
		/* @deprecated */
		void addNewBlock(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException{
			HdfsFileStatus fileInfo = srcNamenode.getFileInfo(src);
			LocatedBlock lb = srcNamenode.append(src, clientName);
			LocatedBlock lb1 = srcNamenode.addBlock(src , clientName , lb.getBlock() , null, fileInfo.getFileId() , null);
			final DatanodeInfo[] datanodes = lb1.getLocations();

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
								+ Op.RSYNC_CHUNKS_CHECKSUM + ", block=" + lb.getBlock());
					}
					// inflate block
					new Sender(out).inflateBlock(lb1.getBlock(), lb1.getBlockToken(), clientName, 1024*1024, 0);
					//new Sender(out).chunksChecksum(block, lb.getBlockToken());

					final BlockOpResponseProto reply = BlockOpResponseProto
							.parseFrom(PBHelper.vintPrefixed(in));

					if (reply.getStatus() != Status.SUCCESS) {
						if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
							throw new InvalidBlockTokenException();
						} else {
							throw new IOException("Bad response " + reply
									+ " for block " + lb1 + " from datanode "
									+ datanodes[j]);
						}
					}
				} catch (IOException ie) {
					LOG.warn("src=" + src + ", datanodes[" + j + "]="
							+ datanodes[j], ie);
				} finally {
					IOUtils.closeStream(in);
					IOUtils.closeStream(out);
					srcNamenode.complete(src, clientName , lb1.getBlock() , fileInfo.getFileId());
				}
			}
			
			LOG.warn("add block1 : "+lb1);
		}
		
		protected void checkOpen() throws IOException {
			if (!clientRunning) {
				IOException result = new IOException("Filesystem closed");
				throw result;
			}
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
		
		public void printFileInfo(FileInfo fileInfo){
			LOG.warn("File Info of "+fileInfo.getFilepath());
			LOG.warn("\tblock count : "+fileInfo.getBlocks().size());
			for(BlockInfo bi : fileInfo.getBlocks()){
				LOG.warn("\tblock id : "+bi.getLocatedBlock().getBlock().getBlockId()+
						"; genstamp : "+bi.getLocatedBlock().getBlock().getGenerationStamp()+
						"; checksum size :"+bi.getChecksums().size()+
						"; segment size : "+bi.getSegments().size());
			}
		}
		
		public void run() throws IOException {
			getSDFileInfo();
			getSDFileChecksum();
			calculateSegments();
			printFileInfo(srcFileInfo);
			printFileInfo(dstFileInfo);
			sendSegments();
			updateBlocks();
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
		formatter.printHelp("Usage : RsyncCopy [options] <srcs....> <dst>",
				options);
	}

	protected void checkOpen() throws IOException {
		if (!clientRunning) {
			IOException result = new IOException("Filesystem closed");
			throw result;
		}
	}

	
	public void run() throws IOException {
		getNameNode();
		long uniqueId = 0;
		RsyncCopyFile testCopyFile = new RsyncCopyFile(
				srcNamenode,srcNamenodeProtocolProxy,srcPath,
				dstNamenode,dstNamenodeProtocolProxy,dstPath,
				conf,stats,uniqueId );
		testCopyFile.run();
	}
	
	public static void main(String args[]) throws Exception {
		if (args.length < 2) {
			printUsage();
		}
		RsyncCopy rc = new RsyncCopy("/test","/test");
		rc.run();
		System.exit(0);
	}
}
