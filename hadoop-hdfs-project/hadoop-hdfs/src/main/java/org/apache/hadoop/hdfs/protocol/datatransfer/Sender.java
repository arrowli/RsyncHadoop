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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.toProto;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumPairProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCalculateSegmentsProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksAdaptiveChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpChunksChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpInflateBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpSendSegmentProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpRequestShortCircuitAccessProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpUpdateBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.Message;
import com.google.protobuf.ByteString;

/** Sender */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Sender implements DataTransferProtocol {
	private final DataOutputStream out;

	/** Create a sender for DataTransferProtocol with a output stream. */
	public Sender(final DataOutputStream out) {
		this.out = out;
	}

	/** Initialize a operation. */
	private static void op(final DataOutput out, final Op op)
			throws IOException {
		out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
		op.write(out);
	}

	private static void send(final DataOutputStream out, final Op opcode,
			final Message proto) throws IOException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Sending DataTransferOp "
					+ proto.getClass().getSimpleName() + ": " + proto);
		}
		op(out, opcode);
		proto.writeDelimitedTo(out);
		out.flush();
	}

	static private CachingStrategyProto getCachingStrategy(
			CachingStrategy cachingStrategy) {
		CachingStrategyProto.Builder builder = CachingStrategyProto
				.newBuilder();
		if (cachingStrategy.getReadahead() != null) {
			builder.setReadahead(cachingStrategy.getReadahead().longValue());
		}
		if (cachingStrategy.getDropBehind() != null) {
			builder.setDropBehind(cachingStrategy.getDropBehind()
					.booleanValue());
		}
		return builder.build();
	}

	@Override
	public void readBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientName, final long blockOffset, final long length,
			final boolean sendChecksum, final CachingStrategy cachingStrategy)
			throws IOException {

		OpReadBlockProto proto = OpReadBlockProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildClientHeader(blk,
								clientName, blockToken)).setOffset(blockOffset)
				.setLen(length).setSendChecksums(sendChecksum)
				.setCachingStrategy(getCachingStrategy(cachingStrategy))
				.build();

		send(out, Op.READ_BLOCK, proto);
	}

	@Override
	public void writeBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientName, final DatanodeInfo[] targets,
			final DatanodeInfo source, final BlockConstructionStage stage,
			final int pipelineSize, final long minBytesRcvd,
			final long maxBytesRcvd, final long latestGenerationStamp,
			DataChecksum requestedChecksum,
			final CachingStrategy cachingStrategy) throws IOException {
		ClientOperationHeaderProto header = DataTransferProtoUtil
				.buildClientHeader(blk, clientName, blockToken);

		ChecksumProto checksumProto = DataTransferProtoUtil
				.toProto(requestedChecksum);

		OpWriteBlockProto.Builder proto = OpWriteBlockProto.newBuilder()
				.setHeader(header).addAllTargets(PBHelper.convert(targets, 1))
				.setStage(toProto(stage)).setPipelineSize(pipelineSize)
				.setMinBytesRcvd(minBytesRcvd).setMaxBytesRcvd(maxBytesRcvd)
				.setLatestGenerationStamp(latestGenerationStamp)
				.setRequestedChecksum(checksumProto)
				.setCachingStrategy(getCachingStrategy(cachingStrategy));

		if (source != null) {
			proto.setSource(PBHelper.convertDatanodeInfo(source));
		}

		send(out, Op.WRITE_BLOCK, proto.build());
	}

	@Override
	public void transferBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientName, final DatanodeInfo[] targets)
			throws IOException {

		OpTransferBlockProto proto = OpTransferBlockProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildClientHeader(blk,
								clientName, blockToken))
				.addAllTargets(PBHelper.convert(targets)).build();

		send(out, Op.TRANSFER_BLOCK, proto);
	}

	@Override
	public void requestShortCircuitFds(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken, int maxVersion)
			throws IOException {
		OpRequestShortCircuitAccessProto proto = OpRequestShortCircuitAccessProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.setMaxVersion(maxVersion).build();
		send(out, Op.REQUEST_SHORT_CIRCUIT_FDS, proto);
	}

	@Override
	public void replaceBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken, final String delHint,
			final DatanodeInfo source) throws IOException {
		OpReplaceBlockProto proto = OpReplaceBlockProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.setDelHint(delHint)
				.setSource(PBHelper.convertDatanodeInfo(source)).build();

		send(out, Op.REPLACE_BLOCK, proto);
	}

	@Override
	public void copyBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken) throws IOException {
		OpCopyBlockProto proto = OpCopyBlockProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.build();

		send(out, Op.COPY_BLOCK, proto);
	}

	@Override
	public void blockChecksum(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken) throws IOException {
		OpBlockChecksumProto proto = OpBlockChecksumProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.build();

		send(out, Op.BLOCK_CHECKSUM, proto);
	}

	@Override
	public void chunksChecksum(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken,
			int bytesPerChunk) throws IOException {
		OpChunksChecksumProto proto = OpChunksChecksumProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.setBytesPerChunk(bytesPerChunk)
				.build();
		send(out, Op.RSYNC_CHUNKS_CHECKSUM, proto);
	}
	
	@Override
	public void chunksAdaptiveChecksum(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken,
			int bytesPerChunk,
			int bmin,
			int bmax) throws IOException {
		OpChunksAdaptiveChecksumProto proto = OpChunksAdaptiveChecksumProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.setBytesPerChunk(bytesPerChunk)
				.setBmin(bmin)
				.setBmax(bmax)
				.build();
		send(out, Op.RSYNC_CHUNKS_ADAPTIVE_CHECKSUM, proto);
	}
	
	@Override
	public void inflateBlock(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientname, final long newSize,
			final long latestGenerationStamp)
			throws IOException {
		ClientOperationHeaderProto header = DataTransferProtoUtil
				.buildClientHeader(block, clientname, blockToken);

		OpInflateBlockProto.Builder proto = OpInflateBlockProto.newBuilder()
				.setHeader(header)
				.setNewSize(newSize)
				.setLatestGenerationStamp(latestGenerationStamp);
				
		send(out, Op.RSYNC_INFLATE_BLOCK , proto.build());
	}
	
	@Override
	public void calculateSegments(final ExtendedBlock block,
			final Token<BlockTokenIdentifier> blockToken,
			final String clientname,final List<Integer> simples,List<byte[]> md5s,int bytesPerChunk) throws IOException {
		ClientOperationHeaderProto header = DataTransferProtoUtil
				.buildClientHeader(block, clientname, blockToken);
		
		OpCalculateSegmentsProto.Builder proto = OpCalculateSegmentsProto.newBuilder()
				.setHeader(header)
				.setBytesPerChunk(bytesPerChunk)
				.setChunksPerBlock(block.getNumBytes()/bytesPerChunk);
		for(int i = 0 ; i < simples.size() ; i++){/* 这段代码可以移到PBHelper中，建立新的convert函数 */
			proto.addChecksums(
					ChecksumPairProto.newBuilder()
					.setSimple(simples.get(i))
					.setMd5(ByteString.copyFrom(md5s.get(i)))
					.build());
		}
		
		send(out,Op.RSYNC_CALCULATE_SEGMENTS,proto.build());
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
		ClientOperationHeaderProto header = DataTransferProtoUtil
				.buildClientHeader(blk, clientname, blockToken);
		
		OpSendSegmentProto.Builder proto = OpSendSegmentProto.newBuilder()
				.setHeader(header)
				.setBlockOffset(blockOffset)
				.setLength(length)
				.setSendChecksums(sendChecksum)
				.setIsClient(isClient)
				.setSegmentName(segmentName)
				.setBlockDirName(blockDirName)
				.addAllTargets(PBHelper.convert(targets));
		
		send(out,Op.RSYNC_SEND_SEGMENT,proto.build());
	}
	
	@Override
	public void updateBlock(final ExtendedBlock blk,
			final Token<BlockTokenIdentifier> blockToken) throws IOException {
		OpUpdateBlockProto proto = OpUpdateBlockProto
				.newBuilder()
				.setHeader(
						DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
				.build();
		send(out, Op.RSYNC_UPDATE_BLOCK, proto);
	}
}
