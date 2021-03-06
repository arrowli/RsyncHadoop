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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_UNSUPPORTED;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;
import static org.apache.hadoop.util.Time.now;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.File;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor.InvalidMagicNumberException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Receiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumPairProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumStrongProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DNTransferAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCalculateSegmentsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksAdaptiveChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.SegmentProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsUnsupportedException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsVersionException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;

import com.google.protobuf.ByteString;

/**
 * Thread for processing incoming/outgoing data stream.
 */
class DataXceiver extends Receiver implements Runnable {
	public static final Log LOG = DataNode.LOG;
	static final Log ClientTraceLog = DataNode.ClientTraceLog;

	private final Peer peer;
	private final String remoteAddress; // address of remote side
	private final String localAddress; // local address of this daemon
	private final DataNode datanode;
	private final DNConf dnConf;
	private final DataXceiverServer dataXceiverServer;
	private final boolean connectToDnViaHostname;
	private long opStartTime; // the start time of receiving an Op
	private final InputStream socketIn;
	private OutputStream socketOut;

	/**
	 * Client Name used in previous operation. Not available on first request on
	 * the socket.
	 */
	private String previousOpClientName;

	public static DataXceiver create(Peer peer, DataNode dn,
			DataXceiverServer dataXceiverServer) throws IOException {
		return new DataXceiver(peer, dn, dataXceiverServer);
	}

	private DataXceiver(Peer peer, DataNode datanode,
			DataXceiverServer dataXceiverServer) throws IOException {

		this.peer = peer;
		this.dnConf = datanode.getDnConf();
		this.socketIn = peer.getInputStream();
		this.socketOut = peer.getOutputStream();
		this.datanode = datanode;
		this.dataXceiverServer = dataXceiverServer;
		this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
		remoteAddress = peer.getRemoteAddressString();
		localAddress = peer.getLocalAddressString();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Number of active connections is: "
					+ datanode.getXceiverCount());
		}
	}

	/**
	 * Update the current thread's name to contain the current status. Use this
	 * only after this receiver has started on its thread, i.e., outside the
	 * constructor.
	 */
	private void updateCurrentThreadName(String status) {
		StringBuilder sb = new StringBuilder();
		sb.append("DataXceiver for client ");
		if (previousOpClientName != null) {
			sb.append(previousOpClientName).append(" at ");
		}
		sb.append(remoteAddress);
		if (status != null) {
			sb.append(" [").append(status).append("]");
		}
		Thread.currentThread().setName(sb.toString());
	}

	/** Return the datanode object. */
	DataNode getDataNode() {
		return datanode;
	}

	private OutputStream getOutputStream() {
		return socketOut;
	}

	/**
	 * Read/write data from/to the DataXceiverServer.
	 */
	@Override
	public void run() {
		int opsProcessed = 0;
		Op op = null;

		dataXceiverServer.addPeer(peer);
		try {
			peer.setWriteTimeout(datanode.getDnConf().socketWriteTimeout);
			InputStream input = socketIn;
			if (dnConf.encryptDataTransfer) {
				IOStreamPair encryptedStreams = null;
				try {
					encryptedStreams = DataTransferEncryptor
							.getEncryptedStreams(socketOut, socketIn,
									datanode.blockPoolTokenSecretManager,
									dnConf.encryptionAlgorithm);
				} catch (InvalidMagicNumberException imne) {
					LOG.info("Failed to read expected encryption handshake from client "
							+ "at "
							+ peer.getRemoteAddressString()
							+ ". Perhaps the client "
							+ "is running an older version of Hadoop which does not support "
							+ "encryption");
					return;
				}
				input = encryptedStreams.in;
				socketOut = encryptedStreams.out;
			}
			input = new BufferedInputStream(input,
					HdfsConstants.SMALL_BUFFER_SIZE);

			super.initialize(new DataInputStream(input));

			// We process requests in a loop, and stay around for a short
			// timeout.
			// This optimistic behaviour allows the other end to reuse
			// connections.
			// Setting keepalive timeout to 0 disable this behavior.
			do {
				updateCurrentThreadName("Waiting for operation #"
						+ (opsProcessed + 1));

				try {
					if (opsProcessed != 0) {
						assert dnConf.socketKeepaliveTimeout > 0;
						peer.setReadTimeout(dnConf.socketKeepaliveTimeout);
					} else {
						peer.setReadTimeout(dnConf.socketTimeout);
					}
					op = readOp();
				} catch (InterruptedIOException ignored) {
					// Time out while we wait for client rpc
					break;
				} catch (IOException err) {
					// Since we optimistically expect the next op, it's quite
					// normal to get EOF here.
					if (opsProcessed > 0
							&& (err instanceof EOFException || err instanceof ClosedChannelException)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Cached " + peer + " closing after "
									+ opsProcessed + " ops");
						}
					} else {
						throw err;
					}
					break;
				}

				// restore normal timeout
				if (opsProcessed != 0) {
					peer.setReadTimeout(dnConf.socketTimeout);
				}

				opStartTime = now();
				processOp(op);
				++opsProcessed;
			} while (!peer.isClosed() && dnConf.socketKeepaliveTimeout > 0);
		} catch (Throwable t) {
			LOG.error(datanode.getDisplayName()
					+ ":DataXceiver error processing "
					+ ((op == null) ? "unknown" : op.name()) + " operation "
					+ " src: " + remoteAddress + " dest: " + localAddress, t);
		} finally {
			if (LOG.isDebugEnabled()) {
				LOG.debug(datanode.getDisplayName()
						+ ":Number of active connections is: "
						+ datanode.getXceiverCount());
			}
			updateCurrentThreadName("Cleaning up");
			dataXceiverServer.closePeer(peer);
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void requestShortCircuitFds(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> token, int maxVersion)
			throws IOException {
		updateCurrentThreadName("Passing file descriptors for block " + blk);
		BlockOpResponseProto.Builder bld = BlockOpResponseProto.newBuilder();
		FileInputStream fis[] = null;
		try {
			if (peer.getDomainSocket() == null) {
				throw new IOException("You cannot pass file descriptors over "
						+ "anything but a UNIX domain socket.");
			}
			fis = datanode
					.requestShortCircuitFdsForRead(blk, token, maxVersion);
			bld.setStatus(SUCCESS);
			bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
		} catch (ShortCircuitFdsVersionException e) {
			bld.setStatus(ERROR_UNSUPPORTED);
			bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
			bld.setMessage(e.getMessage());
		} catch (ShortCircuitFdsUnsupportedException e) {
			bld.setStatus(ERROR_UNSUPPORTED);
			bld.setMessage(e.getMessage());
		} catch (InvalidToken e) {
			bld.setStatus(ERROR_ACCESS_TOKEN);
			bld.setMessage(e.getMessage());
		} catch (IOException e) {
			bld.setStatus(ERROR);
			bld.setMessage(e.getMessage());
		}
		try {
			bld.build().writeDelimitedTo(socketOut);
			if (fis != null) {
				FileDescriptor fds[] = new FileDescriptor[fis.length];
				for (int i = 0; i < fds.length; i++) {
					fds[i] = fis[i].getFD();
				}
				byte buf[] = new byte[] { (byte) 0 };
				peer.getDomainSocket().sendFileDescriptors(fds, buf, 0,
						buf.length);
			}
		} finally {
			if (ClientTraceLog.isInfoEnabled()) {
				DatanodeRegistration dnR = datanode.getDNRegistrationForBP(blk
						.getBlockPoolId());
				BlockSender.ClientTraceLog.info(String.format(
						"src: 127.0.0.1, dest: 127.0.0.1, op: REQUEST_SHORT_CIRCUIT_FDS,"
								+ " blockid: %s, srvID: %s, success: %b",
						blk.getBlockId(), dnR.getStorageID(), (fis != null)));
			}
			if (fis != null) {
				IOUtils.cleanup(LOG, fis);
			}
		}
	}

	@Override
	public void readBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientName, final long blockOffset, final long length,
			final boolean sendChecksum, final CachingStrategy cachingStrategy)
			throws IOException {
		previousOpClientName = clientName;

		OutputStream baseStream = getOutputStream();
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				baseStream, HdfsConstants.SMALL_BUFFER_SIZE));
		checkAccess(out, true, block, blockToken, Op.READ_BLOCK,
				BlockTokenSecretManager.AccessMode.READ);

		// send the block
		BlockSender blockSender = null;
		DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block
				.getBlockPoolId());
		final String clientTraceFmt = clientName.length() > 0
				&& ClientTraceLog.isInfoEnabled() ? String.format(
				DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress, "%d",
				"HDFS_READ", clientName, "%d", dnR.getStorageID(), block, "%d")
				: dnR + " Served block " + block + " to " + remoteAddress;

		updateCurrentThreadName("Sending block " + block);
		try {
			try {
				blockSender = new BlockSender(block, blockOffset, length, true,
						false, sendChecksum, datanode, clientTraceFmt,
						cachingStrategy);
			} catch (IOException e) {
				String msg = "opReadBlock " + block + " received exception "
						+ e;
				LOG.info(msg);
				sendResponse(ERROR, msg);
				throw e;
			}

			// send op status
			writeSuccessWithChecksumInfo(blockSender, new DataOutputStream(
					getOutputStream()));

			long read = blockSender.sendBlock(out, baseStream, null); // send
																		// data

			if (blockSender.didSendEntireByteRange()) {
				// If we sent the entire range, then we should expect the client
				// to respond with a Status enum.
				try {
					ClientReadStatusProto stat = ClientReadStatusProto
							.parseFrom(PBHelper.vintPrefixed(in));
					if (!stat.hasStatus()) {
						LOG.warn("Client "
								+ peer.getRemoteAddressString()
								+ " did not send a valid status code after reading. "
								+ "Will close connection.");
						IOUtils.closeStream(out);
					}
				} catch (IOException ioe) {
					LOG.debug(
							"Error reading client status response. Will close connection.",
							ioe);
					IOUtils.closeStream(out);
				}
			} else {
				IOUtils.closeStream(out);
			}
			datanode.metrics.incrBytesRead((int) read);
			datanode.metrics.incrBlocksRead();
		} catch (SocketException ignored) {
			if (LOG.isTraceEnabled()) {
				LOG.trace(dnR + ":Ignoring exception while serving " + block
						+ " to " + remoteAddress, ignored);
			}
			// Its ok for remote side to close the connection anytime.
			datanode.metrics.incrBlocksRead();
			IOUtils.closeStream(out);
		} catch (IOException ioe) {
			/*
			 * What exactly should we do here? Earlier version shutdown()
			 * datanode if there is disk error.
			 */
			LOG.warn(dnR + ":Got exception while serving " + block + " to "
					+ remoteAddress, ioe);
			throw ioe;
		} finally {
			IOUtils.closeStream(blockSender);
		}

		// update metrics
		datanode.metrics.addReadBlockOp(elapsed());
		datanode.metrics.incrReadsFromClient(peer.isLocal());
	}

	@Override
	public void writeBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientname, final DatanodeInfo[] targets,
			final DatanodeInfo srcDataNode, final BlockConstructionStage stage,
			final int pipelineSize, final long minBytesRcvd,
			final long maxBytesRcvd, final long latestGenerationStamp,
			DataChecksum requestedChecksum, CachingStrategy cachingStrategy)
			throws IOException {
		previousOpClientName = clientname;
		updateCurrentThreadName("Receiving block " + block);
		final boolean isDatanode = clientname.length() == 0;
		final boolean isClient = !isDatanode;
		final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
				|| stage == BlockConstructionStage.TRANSFER_FINALIZED;

		// check single target for transfer-RBW/Finalized
		if (isTransfer && targets.length > 0) {
			throw new IOException(stage + " does not support multiple targets "
					+ Arrays.asList(targets));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("opWriteBlock: stage=" + stage + ", clientname="
					+ clientname + "\n  block  =" + block + ", newGs="
					+ latestGenerationStamp + ", bytesRcvd=[" + minBytesRcvd
					+ ", " + maxBytesRcvd + "]" + "\n  targets="
					+ Arrays.asList(targets) + "; pipelineSize=" + pipelineSize
					+ ", srcDataNode=" + srcDataNode);
			LOG.debug("isDatanode=" + isDatanode + ", isClient=" + isClient
					+ ", isTransfer=" + isTransfer);
			LOG.debug("writeBlock receive buf size "
					+ peer.getReceiveBufferSize() + " tcp no delay "
					+ peer.getTcpNoDelay());
		}

		// We later mutate block's generation stamp and length, but we need to
		// forward the original version of the block to downstream mirrors, so
		// make a copy here.
		final ExtendedBlock originalBlock = new ExtendedBlock(block);
		block.setNumBytes(dataXceiverServer.estimateBlockSize);
		LOG.info("Receiving " + block + " src: " + remoteAddress + " dest: "
				+ localAddress);

		// reply to upstream datanode or client
		final DataOutputStream replyOut = new DataOutputStream(
				new BufferedOutputStream(getOutputStream(),
						HdfsConstants.SMALL_BUFFER_SIZE));
		checkAccess(replyOut, isClient, block, blockToken, Op.WRITE_BLOCK,
				BlockTokenSecretManager.AccessMode.WRITE);

		DataOutputStream mirrorOut = null; // stream to next target
		DataInputStream mirrorIn = null; // reply from next target
		Socket mirrorSock = null; // socket to next target
		BlockReceiver blockReceiver = null; // responsible for data handling
		String mirrorNode = null; // the name:port of next target
		String firstBadLink = ""; // first datanode that failed in connection
									// setup
		Status mirrorInStatus = SUCCESS;
		try {
			if (isDatanode
					|| stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
				// open a block receiver
				blockReceiver = new BlockReceiver(block, in,
						peer.getRemoteAddressString(),
						peer.getLocalAddressString(), stage,
						latestGenerationStamp, minBytesRcvd, maxBytesRcvd,
						clientname, srcDataNode, datanode, requestedChecksum,
						cachingStrategy);
			} else {
				datanode.data.recoverClose(block, latestGenerationStamp,
						minBytesRcvd);
			}

			//
			// Connect to downstream machine, if appropriate
			//
			if (targets.length > 0) {
				InetSocketAddress mirrorTarget = null;
				// Connect to backup machine
				mirrorNode = targets[0].getXferAddr(connectToDnViaHostname);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Connecting to datanode " + mirrorNode);
				}
				mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
				mirrorSock = datanode.newSocket();
				try {
					int timeoutValue = dnConf.socketTimeout
							+ (HdfsServerConstants.READ_TIMEOUT_EXTENSION * targets.length);
					int writeTimeout = dnConf.socketWriteTimeout
							+ (HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * targets.length);
					NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
					mirrorSock.setSoTimeout(timeoutValue);
					mirrorSock
							.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);

					OutputStream unbufMirrorOut = NetUtils.getOutputStream(
							mirrorSock, writeTimeout);
					InputStream unbufMirrorIn = NetUtils
							.getInputStream(mirrorSock);
					if (dnConf.encryptDataTransfer) {
						IOStreamPair encryptedStreams = DataTransferEncryptor
								.getEncryptedStreams(
										unbufMirrorOut,
										unbufMirrorIn,
										datanode.blockPoolTokenSecretManager
												.generateDataEncryptionKey(block
														.getBlockPoolId()));

						unbufMirrorOut = encryptedStreams.out;
						unbufMirrorIn = encryptedStreams.in;
					}
					mirrorOut = new DataOutputStream(new BufferedOutputStream(
							unbufMirrorOut, HdfsConstants.SMALL_BUFFER_SIZE));
					mirrorIn = new DataInputStream(unbufMirrorIn);

					new Sender(mirrorOut).writeBlock(originalBlock, blockToken,
							clientname, targets, srcDataNode, stage,
							pipelineSize, minBytesRcvd, maxBytesRcvd,
							latestGenerationStamp, requestedChecksum,
							cachingStrategy);

					mirrorOut.flush();

					// read connect ack (only for clients, not for replication
					// req)
					if (isClient) {
						BlockOpResponseProto connectAck = BlockOpResponseProto
								.parseFrom(PBHelper.vintPrefixed(mirrorIn));
						mirrorInStatus = connectAck.getStatus();
						firstBadLink = connectAck.getFirstBadLink();
						if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
							LOG.info("Datanode "
									+ targets.length
									+ " got response for connect ack "
									+ " from downstream datanode with firstbadlink as "
									+ firstBadLink);
						}
					}

				} catch (IOException e) {
					if (isClient) {
						BlockOpResponseProto.newBuilder().setStatus(ERROR)
								// NB: Unconditionally using the xfer addr w/o
								// hostname
								.setFirstBadLink(targets[0].getXferAddr())
								.build().writeDelimitedTo(replyOut);
						replyOut.flush();
					}
					IOUtils.closeStream(mirrorOut);
					mirrorOut = null;
					IOUtils.closeStream(mirrorIn);
					mirrorIn = null;
					IOUtils.closeSocket(mirrorSock);
					mirrorSock = null;
					if (isClient) {
						LOG.error(datanode + ":Exception transfering block "
								+ block + " to mirror " + mirrorNode + ": " + e);
						throw e;
					} else {
						LOG.info(datanode + ":Exception transfering " + block
								+ " to mirror " + mirrorNode
								+ "- continuing without the mirror", e);
					}
				}
			}

			// send connect-ack to source for clients and not
			// transfer-RBW/Finalized
			if (isClient && !isTransfer) {
				if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
					LOG.info("Datanode "
							+ targets.length
							+ " forwarding connect ack to upstream firstbadlink is "
							+ firstBadLink);
				}
				BlockOpResponseProto.newBuilder().setStatus(mirrorInStatus)
						.setFirstBadLink(firstBadLink).build()
						.writeDelimitedTo(replyOut);
				replyOut.flush();
			}

			// receive the block and mirror to the next target
			if (blockReceiver != null) {
				String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
				blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
						mirrorAddr, null, targets);

				// send close-ack for transfer-RBW/Finalized
				if (isTransfer) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("TRANSFER: send close-ack");
					}
					writeResponse(SUCCESS, null, replyOut);
				}
			}

			// update its generation stamp
			if (isClient
					&& stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
				block.setGenerationStamp(latestGenerationStamp);
				block.setNumBytes(minBytesRcvd);
			}

			// if this write is for a replication request or recovering
			// a failed close for client, then confirm block. For other
			// client-writes,
			// the block is finalized in the PacketResponder.
			if (isDatanode
					|| stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
				datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT);
				LOG.info("Received " + block + " src: " + remoteAddress
						+ " dest: " + localAddress + " of size "
						+ block.getNumBytes());
			}

		} catch (IOException ioe) {
			LOG.info("opWriteBlock " + block + " received exception " + ioe);
			throw ioe;
		} finally {
			// close all opened streams
			IOUtils.closeStream(mirrorOut);
			IOUtils.closeStream(mirrorIn);
			IOUtils.closeStream(replyOut);
			IOUtils.closeSocket(mirrorSock);
			IOUtils.closeStream(blockReceiver);
		}

		// update metrics
		datanode.metrics.addWriteBlockOp(elapsed());
		datanode.metrics.incrWritesFromClient(peer.isLocal());
	}

	@Override
	public void inflateBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientname, final long newSize,
			final long latestGenerationStamp)
			throws IOException {
		final boolean isDatanode = clientname.length() == 0;
		final boolean isClient = !isDatanode;

		if (LOG.isDebugEnabled()) {
			LOG.debug("opInflateBlock:  clientname="
					+ clientname + "\n  block  =" + block + ", newGs="
					+ latestGenerationStamp);
			LOG.debug("isDatanode=" + isDatanode + ", isClient=" + isClient);
			LOG.debug("inflateBlock new block size " + newSize);
		}

		final DataOutputStream out = new DataOutputStream(getOutputStream());
		
		checkAccess(out, isClient, block, blockToken, Op.WRITE_BLOCK,
				BlockTokenSecretManager.AccessMode.WRITE);
		OutputStream dout = null;
		OutputStream cout = null;
		try{
			ReplicaInPipelineInterface replicaInfo = datanode.data.createTemporary(block);
			ReplicaOutputStreams streams = replicaInfo.createStreams(true,DataChecksum.newDataChecksum(Type.DEFAULT, 1024*1024 /*bytesPerChecksum*/));
			dout = streams.getDataOut(); // to block file at local disk
			cout = streams.getChecksumOut(); // output stream for checksum file

			byte[] b = new byte[(int)newSize];
			dout.write(b);

			// update its generation stamp
			block.setGenerationStamp(latestGenerationStamp);
			block.setNumBytes(newSize);
			
			BlockOpResponseProto.newBuilder()
			.setStatus(SUCCESS).build()
			.writeDelimitedTo(out);
			out.flush();
		} catch (IOException ioe) {
			LOG.info("opInflateBlock " + block + " received exception " + ioe);
			throw ioe;
		} finally {
			// close all opened streams
			dout.close();
			cout.close();
		}

		// update metrics
		datanode.metrics.addWriteBlockOp(elapsed());
		datanode.metrics.incrWritesFromClient(peer.isLocal());
	}
	
	@Override
	public void transferBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientName, final DatanodeInfo[] targets)
			throws IOException {
		checkAccess(socketOut, true, blk, blockToken, Op.TRANSFER_BLOCK,
				BlockTokenSecretManager.AccessMode.COPY);
		previousOpClientName = clientName;
		updateCurrentThreadName(Op.TRANSFER_BLOCK + " " + blk);

		final DataOutputStream out = new DataOutputStream(getOutputStream());
		try {
			datanode.transferReplicaForPipelineRecovery(blk, targets,
					clientName);
			writeResponse(Status.SUCCESS, null, out);
		} finally {
			IOUtils.closeStream(out);
		}
	}

	//https://github.com/wen866595/jrsync/tree/master/src/bruce/rsync
	//网上找的adler32算法java版
	public final static int MOD_ADLER = 65521;
	public static int adler32(byte[] buf, int offset, int length) {
		int i;
		int s1,s2;
		s1 = s2 = 0;
		for(i = offset ; i < length + offset ; i++){
			s1 = (s1+(int)(buf[i]&0xff))%MOD_ADLER;
			s2 = (s1+s2)%MOD_ADLER;
		}
		
		return ((s1&0xffff)+(s2<<16))&0xffffffff;
	}
	public static int nextAdler32(int oldAdler32, byte preByte, byte nextByte, int chunkSize) {
		int s1,s2;
		s1 = oldAdler32 & 0xffff;
		s2 = (oldAdler32 >> 16)& 0xffff;
		s1 = (s1-(int)(preByte&0xff) + (int)(nextByte&0xff) + MOD_ADLER)%MOD_ADLER;
		s2 = (s2+s1-(chunkSize*(int)(preByte&0xff))%MOD_ADLER + MOD_ADLER)%MOD_ADLER;
		return ((s1&0xffff)+(s2<<16))&0xffffffff;
	}
	
	@Override
	public void chunksChecksum(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			int bytesPerChunk) throws IOException {
		final DataOutputStream out = new DataOutputStream(getOutputStream());
		checkAccess(out, true, block, blockToken, Op.RSYNC_CHUNKS_CHECKSUM,
				BlockTokenSecretManager.AccessMode.READ);
		updateCurrentThreadName("Reading metadata for block " + block);
		final LengthInputStream metadataIn = datanode.data
				.getMetaDataInputStream(block);
		final DataInputStream checksumIn = new DataInputStream(
				new BufferedInputStream(metadataIn,
						HdfsConstants.IO_FILE_BUFFER_SIZE));

		updateCurrentThreadName("Getting checksums for block " + block);
		InputStream blockIn = null;
		try {
			// read metadata file
			final BlockMetadataHeader header = BlockMetadataHeader
					.readHeader(checksumIn);
			final DataChecksum checksum = header.getChecksum();
			final int bytesPerCRC = checksum.getBytesPerChecksum();
			final long crcPerBlock = (metadataIn.getLength() - BlockMetadataHeader
					.getHeaderSize()) / checksum.getChecksumSize();
			
			final List<ChecksumPairProto> checksums = new LinkedList<ChecksumPairProto>();

			//generate checksum, chunk = 1MB = 1 * 2^20 B
			byte[] buf = new byte[bytesPerChunk];
			blockIn = datanode.data.getBlockInputStream(block, 0);
			//Adler32 cs = new Adler32();
			
			int bytesRead = 0;
			while((bytesRead=blockIn.read(buf)) != -1){
				//cs.reset();
				//cs.update(buf);
				//checksums.add((int)cs.getValue());
				int simple = adler32(buf,0,bytesRead);
				MD5Hash md5s = MD5Hash.digest(buf,0,bytesRead);
				//md5Checksums.add(ByteString.copyFrom(md5s.getDigest()));
				checksums.add(
						ChecksumPairProto.newBuilder()
							.setSimple(simple)
							.setMd5(ByteString.copyFrom(md5s.getDigest()))
							.build());
			}
			blockIn.close();
			
			// compute block checksum
			final MD5Hash md5 = MD5Hash.digest(checksumIn);

			if (LOG.isDebugEnabled()) {
				LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
						+ ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
			}
			
			LOG.warn("total length "+metadataIn.getLength());
			LOG.warn("header length "+BlockMetadataHeader.getHeaderSize());
			LOG.warn("bytesPerChunk "+bytesPerChunk);
			
			// write reply
			BlockOpResponseProto.newBuilder().setStatus(SUCCESS)
					.setChunksChecksumResponse(OpChunksChecksumResponseProto
							.newBuilder().setBytesPerCrc(bytesPerCRC)
							.setCrcPerBlock(crcPerBlock)
							.addAllChecksums(checksums)
							.setMd5(ByteString.copyFrom(md5.getDigest()))
							.setCrcType(PBHelper.convert(checksum.getChecksumType()))
							)
					.build()
					.writeDelimitedTo(out);
			out.flush();
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(checksumIn);
			IOUtils.closeStream(metadataIn);
			IOUtils.closeStream(blockIn);
		}

		// update metrics
		datanode.metrics.addBlockChecksumOp(elapsed());
	}

	@Override
	public void chunksAdaptiveChecksum(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			int bytesPerChunk,
			int bmin,
			int bmax) throws IOException {
		LOG.warn("chunksAdaptiveChecksum is called.");
		final DataOutputStream out = new DataOutputStream(getOutputStream());
		checkAccess(out, true, block, blockToken, Op.RSYNC_CHUNKS_ADAPTIVE_CHECKSUM,
				BlockTokenSecretManager.AccessMode.READ);
		updateCurrentThreadName("Reading metadata for block " + block);
		final LengthInputStream metadataIn = datanode.data
				.getMetaDataInputStream(block);
		final DataInputStream checksumIn = new DataInputStream(
				new BufferedInputStream(metadataIn,
						HdfsConstants.IO_FILE_BUFFER_SIZE));

		updateCurrentThreadName("Getting adaptive checksums for block " + block);
		InputStream blockIn = null;
		try {
			// read metadata file
			final BlockMetadataHeader header = BlockMetadataHeader
					.readHeader(checksumIn);
			final DataChecksum checksum = header.getChecksum();
			final int bytesPerCRC = checksum.getBytesPerChecksum();
			final long crcPerBlock = (metadataIn.getLength() - BlockMetadataHeader
					.getHeaderSize()) / checksum.getChecksumSize();

			//generate checksum, chunk = 1MB = 1 * 2^20 B
			byte[] buf = new byte[(int) block.getNumBytes()];
			blockIn = datanode.data.getBlockInputStream(block, 0);
			blockIn.read(buf);
			blockIn.close();
			//Adler32 cs = new Adler32();
			MessageDigest mdInst = null;
			try{
				mdInst = MessageDigest.getInstance("MD5");
			}catch(NoSuchAlgorithmException e){
				LOG.warn("no such algorithm : "+e); 
			}

			List<ChecksumStrongProto> checksums = new LinkedList<ChecksumStrongProto>();
			
			class idxCS{
				public int index;
				public int checksum;
				public idxCS pre;
				public idxCS next;
				idxCS(int index,int checksum){
					this.index = index;
					this.checksum = checksum;
					this.pre = null;
					this.next = null;
				}
			}
			
			//List<StrongChecksum> checksums = new LinkedList<StrongChecksum>();
			idxCS head = new idxCS(0,Integer.MAX_VALUE);
			idxCS tail = new idxCS(buf.length,Integer.MAX_VALUE);
			head.next = tail;
			tail.pre = head;
			idxCS cur = head;
			
			int preCS = adler32(buf, 0, bytesPerChunk);
			int cs = nextAdler32(preCS, buf[0], buf[bytesPerChunk], bytesPerChunk);
			for(int i = 2 ; i < buf.length-bytesPerChunk-bmin ; i++){
				int nextCS = nextAdler32(cs, buf[i], buf[i+bytesPerChunk], bytesPerChunk);
				if((cs > preCS)&&(cs > nextCS)){
					idxCS newNode = new idxCS(i,cs);
					/*
					newNode.pre = cur;
					newNode.next = cur.next;
					cur.next.pre = newNode;
					cur.next = newNode;
					cur = cur.next;
					*/
					boolean remove = true;
					boolean insert = true;
					cur = tail.pre;
					while((cur != head)&&(cur != tail)&&(remove == true)){
						remove = false;
						if(newNode.index - cur.index < bmin){
							if(newNode.checksum > cur.checksum){
								//System.out.println("cur.index "+cur.index+"; cur.checksum "+cur.checksum);
								cur.pre.next = cur.next;
								cur.next.pre = cur.pre;
								cur = cur.pre;
								remove = true;
							}else{
								insert = false;
								remove = false;
							}
						}
					}
					
					if((insert == true)&&(newNode.index < buf.length-bytesPerChunk-bmin)){
						newNode.pre = tail.pre;
						newNode.next = tail;
						tail.pre.next = newNode;
						tail.pre = newNode;
					}
				}
				preCS = cs;
				cs = nextCS;
			}
			
			int offset = 0;
			int index = 1;
			idxCS nextOffset = head.next;
			while(nextOffset != tail){
				mdInst.update(buf, offset, nextOffset.index-offset);
				checksums.add(ChecksumStrongProto.newBuilder()
						.setIndex(index)
						.setLength(nextOffset.index-offset)
						.setOffset(offset)
						.setMd5(ByteString.copyFrom(mdInst.digest()))
						.build());
				index++;
				offset = nextOffset.index;
				nextOffset = nextOffset.next;
			}
			mdInst.update(buf, offset, buf.length-offset);
			checksums.add(ChecksumStrongProto.newBuilder()
					.setIndex(index)
					.setLength(buf.length-offset)
					.setOffset(offset)
					.setMd5(ByteString.copyFrom(mdInst.digest()))
					.build());
			LOG.warn("checksums size "+checksums.size());
			for(int i = 0 ; i < checksums.size() ; i++){
				LOG.warn("Index "+checksums.get(i).getIndex()+
						" offset "+checksums.get(i).getOffset()+
						" length "+checksums.get(i).getLength());
			}
			// compute block checksum
			final MD5Hash md5 = MD5Hash.digest(checksumIn);

			if (LOG.isDebugEnabled()) {
				LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
						+ ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
			}
			
			LOG.warn("total length "+metadataIn.getLength());
			LOG.warn("header length "+BlockMetadataHeader.getHeaderSize());
			LOG.warn("bytesPerChunk "+bytesPerChunk);
			
			// write reply
			BlockOpResponseProto.newBuilder().setStatus(SUCCESS)
					.setChunksAdaptiveChecksumResponse(OpChunksAdaptiveChecksumResponseProto
							.newBuilder().setBytesPerCrc(bytesPerCRC)
							.setCrcPerBlock(crcPerBlock)
							.addAllChecksums(checksums)
							.setMd5(ByteString.copyFrom(md5.getDigest()))
							.setCrcType(PBHelper.convert(checksum.getChecksumType()))
							.setChunksInBlock(index)
							)
					.build()
					.writeDelimitedTo(out);
			out.flush();
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(checksumIn);
			IOUtils.closeStream(metadataIn);
			IOUtils.closeStream(blockIn);
		}

		// update metrics
		datanode.metrics.addBlockChecksumOp(elapsed());
	}
	
	@Override
	public void calculateSegments(final ExtendedBlock blk,
		      final Token<BlockTokenIdentifier> blockToken,
		      final String clientname,
		      final List<Integer> simples,
		      final List<byte[]> md5s,
		      int bytesPerChunk) throws IOException{
		LOG.warn("CalculateSegments is called.");
		LOG.warn("Client name : "+clientname);
		LOG.warn("Chunk checksum list size : "+simples.size());
		LOG.warn("BytesPerChunk " + bytesPerChunk);
		MessageDigest mdInst = null;
		try{
			mdInst = MessageDigest.getInstance("MD5");
		}catch(NoSuchAlgorithmException e){
			LOG.warn("no such algorithm : "+e); 
		}
		final DataOutputStream out = new DataOutputStream(getOutputStream());
		
		final LengthInputStream metadataIn = datanode.data
				.getMetaDataInputStream(blk);
		final DataInputStream checksumIn = new DataInputStream(
				new BufferedInputStream(metadataIn,
						HdfsConstants.IO_FILE_BUFFER_SIZE));

		updateCurrentThreadName("Calculating segments for block " + blk);
		try {
			// read metadata file
			final BlockMetadataHeader header = BlockMetadataHeader
					.readHeader(checksumIn);
			final DataChecksum checksum = header.getChecksum();
			final int bytesPerCRC = checksum.getBytesPerChecksum();
			final long crcPerBlock = (metadataIn.getLength() - BlockMetadataHeader
					.getHeaderSize()) / checksum.getChecksumSize();
			//final int bytesPerChunk = 1024*1024;
			//final long chunksPerBlock = (datanode.getConf().getLong("dfs.blocksize", 128*1024*1024) - BlockMetadataHeader
			//		.getHeaderSize() + bytesPerChunk - 1) / bytesPerChunk;
			
			//Initialize dst file checksum hashtable
			class ChecksumPair{
				public int index;
				public int simple;
				public byte[] md5;
				ChecksumPair(int index,int simple,byte[] md5){
					this.index = index;
					this.simple = simple;
					this.md5 = md5;
				}
			}
			HashMap<Integer,List<ChecksumPair> > checksumMap = new HashMap<Integer,List<ChecksumPair> >();
			for(int i = 0 ; i < simples.size() ; i++){
				if(checksumMap.containsKey(simples.get(i))){
					checksumMap.get(simples.get(i)).add(new ChecksumPair(i,simples.get(i),md5s.get(i)));
				}else{
					List<ChecksumPair>  md5 = new LinkedList<ChecksumPair>();
					md5.add(new ChecksumPair(i,simples.get(i),md5s.get(i)));
					checksumMap.put(simples.get(i), md5);
				}
			}

			//read src file block checksum to generate segments info
			long blockSize = blk.getNumBytes();
			LOG.warn("Buffer block size " + blockSize);
			byte[] buf = new byte[(int) blockSize];
			InputStream blockIn = datanode.data.getBlockInputStream(blk, 0);
			blockIn.read(buf);
			blockIn.close();
			//Adler32 cs = new Adler32();
			
			List<SegmentProto> segments = new LinkedList<SegmentProto>();
			int startOffset = 0;//上次segment保存的偏移
			int nowOffset = bytesPerChunk;//目前读到的偏移
			int simple = adler32(buf,nowOffset-bytesPerChunk,bytesPerChunk);
			
			int loopTimes = 0;
			int loopEnd = (int) blockSize;
			do{
				long steps = 0;
				boolean found = false;
				if(checksumMap.containsKey(simple)){
					mdInst.update(buf,nowOffset-bytesPerChunk,bytesPerChunk);
					byte[] md5 = mdInst.digest();
					for(ChecksumPair cp : checksumMap.get(simple)){
						//find a match chunk
						if(MessageDigest.isEqual(md5, cp.md5)){
							found = true;
							if(nowOffset-startOffset > bytesPerChunk){
								segments.add(SegmentProto
										.newBuilder()
										.setOffset(startOffset)
										.setLength(nowOffset-startOffset-bytesPerChunk)
										.setIndex(-1)
										.build());
							}
							
							LOG.warn("found a chunk : index "+cp.index+" offset "+Long.toHexString(nowOffset-bytesPerChunk));
							segments.add(SegmentProto
									.newBuilder()
									.setOffset(nowOffset-bytesPerChunk > 0 ? nowOffset - bytesPerChunk : 0)
									.setIndex(cp.index)
									.setLength(bytesPerChunk)
									.build());
							startOffset = nowOffset;
							
							steps = blockSize - nowOffset > bytesPerChunk ? bytesPerChunk : blockSize - nowOffset;
							nowOffset += steps;
							break;
						}
					}
					
					if(found == false){
						steps = blockSize - nowOffset > 1 ? 1 : blockSize - nowOffset;
						nowOffset += steps;
					}
				}else{
					steps = blockSize - nowOffset > 1 ? 1 : blockSize - nowOffset;
					nowOffset += steps;
				}
				
				//simple = adler32(buf,nowOffset-bytesPerChunk,bytesPerChunk);
				if(found == false)
					simple = nextAdler32(simple,buf[nowOffset-bytesPerChunk-1],buf[nowOffset-1],bytesPerChunk);
				else
					simple = adler32(buf,nowOffset-bytesPerChunk,bytesPerChunk);
				if(nowOffset%(1024*1024) == 0) LOG.warn("Calculate "+nowOffset+" bytes now.");
				if(steps < 1) LOG.warn("steps is "+steps+"; nowOffset "+nowOffset+"; startOffset "+startOffset);
				loopTimes++;
			}while((nowOffset < blockSize) && (loopTimes < loopEnd));
			
			if(nowOffset-startOffset > 0){
				segments.add(SegmentProto
						.newBuilder()
						.setOffset(startOffset)
						.setLength(nowOffset-startOffset)
						.setIndex(-1)
						.build());
			}
			
			// compute block checksum
			final MD5Hash md5 = MD5Hash.digest(checksumIn);
			
			// write reply
			BlockOpResponseProto.newBuilder().setStatus(SUCCESS)
					.setCalculateSegmentsResponse(OpCalculateSegmentsResponseProto
							.newBuilder().setNumSegments(segments.size())
							.addAllSegments(segments))
					.build()
					.writeDelimitedTo(out);
			out.flush();
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(checksumIn);
			IOUtils.closeStream(metadataIn);
		}

		// update metrics
		datanode.metrics.addBlockChecksumOp(elapsed());
	}
	
	@Override
	public void sendSegment(final ExtendedBlock blk,
		      final Token<BlockTokenIdentifier> blockToken,
		      final String clientname,
		      final long blockOffset,
		      final long length,
		      final boolean sendChecksum,
		      final boolean isClient,
		      final String segmentName,
		      final String blockDirName,
		      final DatanodeInfo[] targets) throws IOException {
		LOG.warn("sendSegment is called. blk "+blk+
				",block offset "+Long.toHexString(blockOffset)+
				",length "+Long.toHexString(length)+";isClient = "+isClient);
		DataOutputStream out = null;
		Socket sock = null;
		InputStream blockIn = null;
		//TODO:try-catch-finally的逻辑还是有问题的，
		//修改的内容：finally中在出现异常时需要释放的资源统一释放
		if(isClient){
			LOG.warn("source datanode");
			boolean finished = false;
			for(DatanodeInfo target : targets){
				try {
			        final String dnAddr = target.getXferAddr(connectToDnViaHostname);
			        InetSocketAddress curTarget = NetUtils.createSocketAddr(dnAddr);
			        if (LOG.isDebugEnabled()) {
			          LOG.debug("Connecting to datanode " + dnAddr);
			        }
			        sock = datanode.newSocket();
			        NetUtils.connect(sock, curTarget, dnConf.socketTimeout);
			        sock.setSoTimeout(targets.length * dnConf.socketTimeout);
	
			        long writeTimeout = dnConf.socketWriteTimeout + 
			                            HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
			        OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
			        InputStream unbufIn = NetUtils.getInputStream(sock);
			        
			        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
			            HdfsConstants.SMALL_BUFFER_SIZE));
			        in = new DataInputStream(unbufIn);
	
			        new Sender(out).sendSegment(blk, blockToken, clientname, blockOffset, 
			        		length, sendChecksum,false,segmentName,blockDirName,targets);
	
			        // send segment data & checksum
			        Adler32 checksum = new Adler32();
			        blockIn = datanode.data.getBlockInputStream(blk, blockOffset);
			        byte[] buffer = new byte[(int) length];
			        long bytesRead = blockIn.read(buffer);
			        checksum.update(buffer);
			        out.writeLong(length);
			        out.writeLong(checksum.getValue());
			        out.write(buffer);
			        out.flush();
			        LOG.warn("Sender bytesToRead "+length + "; checksum "+checksum.getValue()+"; bytesRead "+bytesRead);
			        //response 
			        final BlockOpResponseProto reply = BlockOpResponseProto
							.parseFrom(PBHelper.vintPrefixed(in));

					if (reply.getStatus() != Status.SUCCESS) {
						LOG.warn("Bad response " + reply
									+ " for block " + blk + " from datanode "
									+ target);
						writeResponse(Status.ERROR, null, out);
						return;
					}
			        
			      } catch (IOException ie) {
			        LOG.warn("Failed to transfer " + blk +
		        		"segment["+Long.toHexString(blockOffset)+":"+ 
			        	Long.toHexString(blockOffset+length-1) +"] to " +
		        		target + " got ", ie);
			      } finally {
			        if(blockIn != null)IOUtils.closeStream(blockIn);
			      }
			}
			writeResponse(Status.SUCCESS, null, out);
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
			if(sock != null)IOUtils.closeSocket(sock);
		}else{
			LOG.warn("destination datanode");
			out = new DataOutputStream(getOutputStream());
			String dfsDataPath = null;
			if (System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1){
				dfsDataPath = "C:/tmp";
			}else{
				dfsDataPath = datanode.getConf().get("dfs.datanode.data.dir",null);
			}
			if(dfsDataPath == null){
				LOG.warn("dfs.datanode.data.dir is not set");
				return;
			}
			
			File dfsDataRoot = new File(dfsDataPath);
			if(!dfsDataRoot.exists() || dfsDataRoot.isFile()){
				LOG.warn("Directory "+dfsDataPath+"does not exist.");
			}
			String dfsTmpPath = "/current/rsync_tmp";
			String blkPath = "/"+blockDirName;//blk.getBlockId()+"_"+blk.getGenerationStamp(); 
			String segmentPath = "/"+segmentName;
			
			File blockDir = new File(dfsDataPath+dfsTmpPath+blkPath);
			File segmentFile = new File(dfsDataPath+dfsTmpPath+blkPath+segmentPath);
			
			//如果没有blockDir的文件夹，新建一个，如果之前有写入中断的，可能会留下残缺文件，删除就行了。
			if(!blockDir.exists()) blockDir.mkdirs();
			if(segmentFile.exists()) segmentFile.delete();
			segmentFile.createNewFile();
			FileOutputStream outFile = new FileOutputStream(segmentFile);
			
			//TODO:还没有校验接收到的数据
			try{
				long bytesToRead = in.readLong();
				long checksum = in.readLong();
				LOG.warn("Receiver bytesToRead "+bytesToRead + "; checksum "+checksum);
				int bytesRead = -1;
				long bytesSum = 0;
				byte[] buffer = new byte[1024*1024];
				while((bytesSum < bytesToRead)&&					//为什么只通过in.read(buffer) ！= -1不能判断输入结束...
						((bytesRead = in.read(buffer)) != -1)){
					bytesSum += bytesRead;
					outFile.write(buffer,0,bytesRead);
					if(bytesSum > bytesToRead){
						LOG.warn("Get more data than expact.");
					}
				}
				outFile.flush();
				writeResponse(Status.SUCCESS, null, out);
			}catch (IOException e){
				throw new IOException("block ["+blk.getBlockId()+"] offset ["+blockOffset+"] length ["+length+"] read fail");
			}finally{
				outFile.close();
				if(out != null)IOUtils.closeStream(out);
		        IOUtils.closeStream(in);
		        if(sock != null)IOUtils.closeSocket(sock);
			}
			
		}
	}
	
	@Override
	public void blockChecksum(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken) throws IOException {
		final DataOutputStream out = new DataOutputStream(getOutputStream());
		checkAccess(out, true, block, blockToken, Op.BLOCK_CHECKSUM,
				BlockTokenSecretManager.AccessMode.READ);
		updateCurrentThreadName("Reading metadata for block " + block);
		final LengthInputStream metadataIn = datanode.data
				.getMetaDataInputStream(block);
		final DataInputStream checksumIn = new DataInputStream(
				new BufferedInputStream(metadataIn,
						HdfsConstants.IO_FILE_BUFFER_SIZE));

		updateCurrentThreadName("Getting checksum for block " + block);
		try {
			// read metadata file
			final BlockMetadataHeader header = BlockMetadataHeader
					.readHeader(checksumIn);
			final DataChecksum checksum = header.getChecksum();
			final int bytesPerCRC = checksum.getBytesPerChecksum();
			final long crcPerBlock = (metadataIn.getLength() - BlockMetadataHeader
					.getHeaderSize()) / checksum.getChecksumSize();

			// compute block checksum
			final MD5Hash md5 = MD5Hash.digest(checksumIn);

			if (LOG.isDebugEnabled()) {
				LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
						+ ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
			}

			// write reply
			BlockOpResponseProto
					.newBuilder()
					.setStatus(SUCCESS)
					.setChecksumResponse(
							OpBlockChecksumResponseProto
									.newBuilder()
									.setBytesPerCrc(bytesPerCRC)
									.setCrcPerBlock(crcPerBlock)
									.setMd5(ByteString.copyFrom(md5.getDigest()))
									.setCrcType(
											PBHelper.convert(checksum
													.getChecksumType())))
					.build().writeDelimitedTo(out);
			out.flush();
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(checksumIn);
			IOUtils.closeStream(metadataIn);
		}

		// update metrics
		datanode.metrics.addBlockChecksumOp(elapsed());
	}

	@Override
	public void updateBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken) throws IOException {
		LOG.warn("updateBlock start.");
		final DataOutputStream out = new DataOutputStream(getOutputStream());
		checkAccess(out, true, block, blockToken, Op.RSYNC_UPDATE_BLOCK,
				BlockTokenSecretManager.AccessMode.WRITE);
		
		//如果isCreate为false的话，需要检查已存在的block的信息是否完整，所以应当置为true，同时如果原block存在的话，应当删除
		//如果requestedChecksum
		boolean isCreate = true;
		int bytesPerChecksum = Integer.parseInt(datanode.getConf().get("dfs.bytes-per-checksum","512"));
		DataChecksum requestedChecksum = DataChecksum.newDataChecksum(Type.CRC32C, bytesPerChecksum);
		
		block.setNumBytes(dataXceiverServer.estimateBlockSize);
		
		ReplicaInPipelineInterface replicaInfo = datanode.data.createRbw(block);
		datanode.notifyNamenodeReceivingBlock(block);
		
		ReplicaOutputStreams streams = replicaInfo.createStreams(isCreate, requestedChecksum);
		OutputStream cout = streams.getChecksumOut();
		OutputStream dout = streams.getDataOut();
		
		//get segments file path
		String dfsDataPath = null;
		if (System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1){
			dfsDataPath = "C:/tmp";
		}else{
			dfsDataPath = datanode.getConf().get("dfs.datanode.data.dir",null);
		}
		
		if(dfsDataPath == null){
			LOG.warn("dfs.datanode.data.dir is not set");
			return;
		}
		
		String dfsTmpPath = "/current/rsync_tmp";
		String blkPath = "/"+block.getBlockId()+"_"+block.getGenerationStamp();
		File blockDir = new File(dfsDataPath+dfsTmpPath+blkPath);

		String[] segmentFiles = blockDir.list();
		if(segmentFiles == null){
			LOG.warn(dfsDataPath+dfsTmpPath+blkPath+"is not exist or has no files.");
			writeResponse(Status.ERROR_EXISTS,null,out);
			out.close();
			return;
		}
		Arrays.sort(segmentFiles);
		//删除原有block
		/*
		String blockName = "blk_"+block.getBlockId();
		String blockMetaName = blockName+"_"+block.getGenerationStamp()+".meta";
		String finalizedDir = dfsDataPath+"/current/"+block.getBlockPoolId()+"/current/finalized";
		String rbwDir = dfsDataPath+"/current/"+block.getBlockPoolId()+"/current/rbw";
		String tmpDir = dfsDataPath+"/current/"+block.getBlockPoolId()+"/tmp";
		
		File fBlockFile = new File(finalizedDir+"/"+blockName);
		File fBlockMetaFile = new File(finalizedDir+"/"+blockMetaName);
		if(fBlockFile.exists()){
			fBlockFile.delete();
		}
		fBlockFile.createNewFile();
		if(fBlockMetaFile.exists()){
			fBlockMetaFile.delete();
		}
		fBlockMetaFile.createNewFile();
		OutputStream dout = new FileOutputStream(fBlockFile);
		OutputStream cout = new FileOutputStream(fBlockMetaFile);
		*/
		DataOutputStream checksumOut = new DataOutputStream(new BufferedOutputStream(cout,HdfsConstants.SMALL_BUFFER_SIZE));
		
		ByteBuffer dataBuf = ByteBuffer.allocate(1024*bytesPerChecksum);
		ByteBuffer checksumBuf = ByteBuffer.allocate(1024*requestedChecksum.getChecksumSize());
		
		BlockMetadataHeader.writeHeader(checksumOut, requestedChecksum);
		long blockLength = 0;
		long startOffset = 0;
		byte[] lastChecksum = new byte[requestedChecksum.getChecksumSize()];
		for(String segmentFile : segmentFiles){
			File segment = new File(dfsDataPath+dfsTmpPath+blkPath+"/"+segmentFile);
			LOG.warn("Read segment "+segment.getName());
			long bytesLeft = segment.length();
			FileInputStream segmentIn = new FileInputStream(segment);
			do{
				long bytesToRead = bytesLeft > dataBuf.capacity() - startOffset ? dataBuf.capacity() - startOffset : bytesLeft;
				LOG.warn("Read "+bytesToRead+" bytes.");
				segmentIn.read(dataBuf.array(), (int)startOffset, (int)bytesToRead);
				startOffset += bytesToRead;
				bytesLeft -= bytesToRead;
				if(startOffset == dataBuf.capacity()){//data full, need write to file
					requestedChecksum.calculateChunkedSums(dataBuf, checksumBuf);
					int offset = checksumBuf.capacity()-requestedChecksum.getChecksumSize();
					int length = requestedChecksum.getChecksumSize();
					LOG.warn("lastChecksum offset "+offset+" length "+length);
					System.arraycopy(checksumBuf.array(), offset, lastChecksum, 0, length);
					dout.write(dataBuf.array());
					checksumOut.write(checksumBuf.array());
					startOffset = 0;
					blockLength += dataBuf.capacity();
				}
			}while(bytesLeft > 0);
			segmentIn.close();
		}
		
		if(startOffset > 0){
			blockLength += startOffset;
			ByteBuffer leftData = ByteBuffer.allocate((int)startOffset);
			dataBuf.get(leftData.array(), 0, (int)startOffset);
			requestedChecksum.calculateChunkedSums(leftData, checksumBuf);
			dout.write(leftData.array());
			cout.write(checksumBuf.array(),0,
					(int)(startOffset+bytesPerChecksum-1)/bytesPerChecksum*requestedChecksum.getChecksumSize());
			
			int offset = (int)(startOffset+bytesPerChecksum-1)/
					bytesPerChecksum*requestedChecksum.getChecksumSize()-
					requestedChecksum.getChecksumSize();
			int length = requestedChecksum.getChecksumSize();
			LOG.warn("lastChecksum offset "+offset+" length "+length);
			System.arraycopy(checksumBuf.array(), offset, lastChecksum, 0, length);
		}
		dout.flush();
		dout.close();
		checksumOut.flush();
		checksumOut.close();
		LOG.warn("block length is "+blockLength);
		replicaInfo.setNumBytes(blockLength);
		replicaInfo.setBytesAcked(blockLength);
		replicaInfo.setLastChecksumAndDataLen(blockLength, lastChecksum);
		
		block.setNumBytes(blockLength);
		datanode.data.finalizeBlock(block);
		//datanode.getActiveNamenodeForBP(block.getBlockPoolId())
		//	.blockReceivedAndDeleted(registration, poolId, receivedAndDeletedBlocks);
		datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT);
		writeResponse(Status.SUCCESS,null,out);
		
		datanode.metrics.incrBytesWritten((int)blockLength);
		datanode.metrics.addBlockChecksumOp(elapsed());
	}
	
	@Override
	public void copyBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken) throws IOException {
		updateCurrentThreadName("Copying block " + block);
		// Read in the header
		if (datanode.isBlockTokenEnabled) {
			try {
				datanode.blockPoolTokenSecretManager.checkAccess(blockToken,
						null, block, BlockTokenSecretManager.AccessMode.COPY);
			} catch (InvalidToken e) {
				LOG.warn("Invalid access token in request from "
						+ remoteAddress + " for OP_COPY_BLOCK for block "
						+ block + " : " + e.getLocalizedMessage());
				sendResponse(ERROR_ACCESS_TOKEN, "Invalid access token");
				return;
			}

		}

		if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to
																// start
			String msg = "Not able to copy block " + block.getBlockId() + " "
					+ "to " + peer.getRemoteAddressString()
					+ " because threads " + "quota is exceeded.";
			LOG.info(msg);
			sendResponse(ERROR, msg);
			return;
		}

		BlockSender blockSender = null;
		DataOutputStream reply = null;
		boolean isOpSuccess = true;

		try {
			// check if the block exists or not
			blockSender = new BlockSender(block, 0, -1, false, false, true,
					datanode, null, CachingStrategy.newDropBehind());

			// set up response stream
			OutputStream baseStream = getOutputStream();
			reply = new DataOutputStream(new BufferedOutputStream(baseStream,
					HdfsConstants.SMALL_BUFFER_SIZE));

			// send status first
			writeSuccessWithChecksumInfo(blockSender, reply);
			// send block content to the target
			long read = blockSender.sendBlock(reply, baseStream,
					dataXceiverServer.balanceThrottler);

			datanode.metrics.incrBytesRead((int) read);
			datanode.metrics.incrBlocksRead();

			LOG.info("Copied " + block + " to " + peer.getRemoteAddressString());
		} catch (IOException ioe) {
			isOpSuccess = false;
			LOG.info("opCopyBlock " + block + " received exception " + ioe);
			throw ioe;
		} finally {
			dataXceiverServer.balanceThrottler.release();
			if (isOpSuccess) {
				try {
					// send one last byte to indicate that the resource is
					// cleaned.
					reply.writeChar('d');
				} catch (IOException ignored) {
				}
			}
			IOUtils.closeStream(reply);
			IOUtils.closeStream(blockSender);
		}

		// update metrics
		datanode.metrics.addCopyBlockOp(elapsed());
	}

	@Override
	public void replaceBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken, final String delHint,
			final DatanodeInfo proxySource) throws IOException {
		updateCurrentThreadName("Replacing block " + block + " from " + delHint);

		/* read header */
		block.setNumBytes(dataXceiverServer.estimateBlockSize);
		if (datanode.isBlockTokenEnabled) {
			try {
				datanode.blockPoolTokenSecretManager
						.checkAccess(blockToken, null, block,
								BlockTokenSecretManager.AccessMode.REPLACE);
			} catch (InvalidToken e) {
				LOG.warn("Invalid access token in request from "
						+ remoteAddress + " for OP_REPLACE_BLOCK for block "
						+ block + " : " + e.getLocalizedMessage());
				sendResponse(ERROR_ACCESS_TOKEN, "Invalid access token");
				return;
			}
		}

		if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to
																// start
			String msg = "Not able to receive block " + block.getBlockId()
					+ " from " + peer.getRemoteAddressString()
					+ " because threads " + "quota is exceeded.";
			LOG.warn(msg);
			sendResponse(ERROR, msg);
			return;
		}

		Socket proxySock = null;
		DataOutputStream proxyOut = null;
		Status opStatus = SUCCESS;
		String errMsg = null;
		BlockReceiver blockReceiver = null;
		DataInputStream proxyReply = null;

		try {
			// get the output stream to the proxy
			final String dnAddr = proxySource
					.getXferAddr(connectToDnViaHostname);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Connecting to datanode " + dnAddr);
			}
			InetSocketAddress proxyAddr = NetUtils.createSocketAddr(dnAddr);
			proxySock = datanode.newSocket();
			NetUtils.connect(proxySock, proxyAddr, dnConf.socketTimeout);
			proxySock.setSoTimeout(dnConf.socketTimeout);

			OutputStream unbufProxyOut = NetUtils.getOutputStream(proxySock,
					dnConf.socketWriteTimeout);
			InputStream unbufProxyIn = NetUtils.getInputStream(proxySock);
			if (dnConf.encryptDataTransfer) {
				IOStreamPair encryptedStreams = DataTransferEncryptor
						.getEncryptedStreams(unbufProxyOut, unbufProxyIn,
								datanode.blockPoolTokenSecretManager
										.generateDataEncryptionKey(block
												.getBlockPoolId()));
				unbufProxyOut = encryptedStreams.out;
				unbufProxyIn = encryptedStreams.in;
			}

			proxyOut = new DataOutputStream(new BufferedOutputStream(
					unbufProxyOut, HdfsConstants.SMALL_BUFFER_SIZE));
			proxyReply = new DataInputStream(new BufferedInputStream(
					unbufProxyIn, HdfsConstants.IO_FILE_BUFFER_SIZE));

			/* send request to the proxy */
			new Sender(proxyOut).copyBlock(block, blockToken);

			// receive the response from the proxy

			BlockOpResponseProto copyResponse = BlockOpResponseProto
					.parseFrom(PBHelper.vintPrefixed(proxyReply));

			if (copyResponse.getStatus() != SUCCESS) {
				if (copyResponse.getStatus() == ERROR_ACCESS_TOKEN) {
					throw new IOException("Copy block " + block + " from "
							+ proxySock.getRemoteSocketAddress()
							+ " failed due to access token error");
				}
				throw new IOException("Copy block " + block + " from "
						+ proxySock.getRemoteSocketAddress() + " failed");
			}

			// get checksum info about the block we're copying
			ReadOpChecksumInfoProto checksumInfo = copyResponse
					.getReadOpChecksumInfo();
			DataChecksum remoteChecksum = DataTransferProtoUtil
					.fromProto(checksumInfo.getChecksum());
			// open a block receiver and check if the block does not exist
			blockReceiver = new BlockReceiver(block, proxyReply, proxySock
					.getRemoteSocketAddress().toString(), proxySock
					.getLocalSocketAddress().toString(), null, 0, 0, 0, "",
					null, datanode, remoteChecksum,
					CachingStrategy.newDropBehind());

			// receive a block
			blockReceiver.receiveBlock(null, null, null, null,
					dataXceiverServer.balanceThrottler, null);

			// notify name node
			datanode.notifyNamenodeReceivedBlock(block, delHint);

			LOG.info("Moved " + block + " from "
					+ peer.getRemoteAddressString());

		} catch (IOException ioe) {
			opStatus = ERROR;
			errMsg = "opReplaceBlock " + block + " received exception " + ioe;
			LOG.info(errMsg);
			throw ioe;
		} finally {
			// receive the last byte that indicates the proxy released its
			// thread resource
			if (opStatus == SUCCESS) {
				try {
					proxyReply.readChar();
				} catch (IOException ignored) {
				}
			}

			// now release the thread resource
			dataXceiverServer.balanceThrottler.release();

			// send response back
			try {
				sendResponse(opStatus, errMsg);
			} catch (IOException ioe) {
				LOG.warn("Error writing reply back to "
						+ peer.getRemoteAddressString());
			}
			IOUtils.closeStream(proxyOut);
			IOUtils.closeStream(blockReceiver);
			IOUtils.closeStream(proxyReply);
		}

		// update metrics
		datanode.metrics.addReplaceBlockOp(elapsed());
	}

	private long elapsed() {
		return now() - opStartTime;
	}

	/**
	 * Utility function for sending a response.
	 * 
	 * @param opStatus
	 *            status message to write
	 * @param message
	 *            message to send to the client or other DN
	 */
	private void sendResponse(Status status, String message) throws IOException {
		writeResponse(status, message, getOutputStream());
	}

	private static void writeResponse(Status status, String message,
			OutputStream out) throws IOException {
		BlockOpResponseProto.Builder response = BlockOpResponseProto
				.newBuilder().setStatus(status);
		if (message != null) {
			response.setMessage(message);
		}
		response.build().writeDelimitedTo(out);
		out.flush();
	}

	private void writeSuccessWithChecksumInfo(BlockSender blockSender,
			DataOutputStream out) throws IOException {

		ReadOpChecksumInfoProto ckInfo = ReadOpChecksumInfoProto
				.newBuilder()
				.setChecksum(
						DataTransferProtoUtil.toProto(blockSender.getChecksum()))
				.setChunkOffset(blockSender.getOffset()).build();

		BlockOpResponseProto response = BlockOpResponseProto.newBuilder()
				.setStatus(SUCCESS).setReadOpChecksumInfo(ckInfo).build();
		response.writeDelimitedTo(out);
		out.flush();
	}

	private void checkAccess(OutputStream out, final boolean reply,
			final ExtendedBlock blk, final Token<BlockTokenIdentifier> t,
			final Op op, final BlockTokenSecretManager.AccessMode mode)
			throws IOException {
		if (datanode.isBlockTokenEnabled) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking block access token for block '"
						+ blk.getBlockId() + "' with mode '" + mode + "'");
			}
			try {
				datanode.blockPoolTokenSecretManager.checkAccess(t, null, blk,
						mode);
			} catch (InvalidToken e) {
				try {
					if (reply) {
						BlockOpResponseProto.Builder resp = BlockOpResponseProto
								.newBuilder().setStatus(ERROR_ACCESS_TOKEN);
						if (mode == BlockTokenSecretManager.AccessMode.WRITE) {
							DatanodeRegistration dnR = datanode
									.getDNRegistrationForBP(blk
											.getBlockPoolId());
							// NB: Unconditionally using the xfer addr w/o
							// hostname
							resp.setFirstBadLink(dnR.getXferAddr());
						}
						resp.build().writeDelimitedTo(out);
						out.flush();
					}
					LOG.warn("Block token verification failed: op=" + op
							+ ", remoteAddress=" + remoteAddress + ", message="
							+ e.getLocalizedMessage());
					throw e;
				} finally {
					IOUtils.closeStream(out);
				}
			}
		}
	}
}
