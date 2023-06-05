/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a Lucene commit point with additional information required to manage this commit in the object store as well as locally. Such
 * objects are uploaded to the object store as binary blobs.
 */
public record StatelessCompoundCommit(
    ShardId shardId,
    long generation,
    long primaryTerm,
    String nodeEphemeralId,
    Map<String, BlobLocation> commitFiles
) implements Writeable {

    public static final String NAME = "stateless_commit_";

    @Override
    public String toString() {
        return "stateless_commit " + shardId + '[' + primaryTerm + "][" + generation + ']';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVLong(generation);
        out.writeVLong(primaryTerm);
        out.writeString(nodeEphemeralId);
        out.writeMap(commitFiles, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public static StatelessCompoundCommit readFromTransport(StreamInput in) throws IOException {
        return new StatelessCompoundCommit(
            new ShardId(in),
            in.readVLong(),
            in.readVLong(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, BlobLocation::readFromTransport)
        );
    }

    public static class Writer {

        private final ShardId shardId;
        private final long generation;
        private final long primaryTerm;
        private final String nodeEphemeralId;

        // Referenced blob files are files already stored in different blobs on the object store. We already
        // know the location, so we directly serialize the location. Internal files are files that
        // are going to be written in this commit file. We do not know their specific blob locations as we
        // don't know the correct offset until serializing the header of the commit. However, on the read path we
        // convert these internal files into blob locations as we can correctly calculate the offsets after
        // knowing the length of the serialized header.
        private final Map<String, BlobLocation> referencedBlobFiles = new HashMap<>();
        private final List<InternalFile> internalFiles = new ArrayList<>();

        public Writer(ShardId shardId, long generation, long primaryTerm, String nodeEphemeralId) {
            this.shardId = shardId;
            this.generation = generation;
            this.primaryTerm = primaryTerm;
            this.nodeEphemeralId = nodeEphemeralId;
        }

        public void addReferencedBlobFile(String name, BlobLocation location) {
            referencedBlobFiles.put(name, location);
        }

        public void addInternalFile(String fileName, long fileLength) {
            internalFiles.add(new InternalFile(fileName, fileLength));
        }

        public List<String> getInternalFiles() {
            return internalFiles.stream().map(InternalFile::name).collect(Collectors.toList());
        }

        private record InternalFile(String name, long length) implements Writeable {

            private InternalFile(StreamInput streamInput) throws IOException {
                this(streamInput.readString(), streamInput.readLong());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeLong(length);
            }
        }

        private long headerSize = -1;

        public long writeToStore(OutputStream output, Directory directory) throws IOException {
            PositionTrackingOutputStreamStreamOutput positionTracking = new PositionTrackingOutputStreamStreamOutput(output);
            writeHeader(positionTracking, CURRENT_VERSION);

            for (InternalFile internalFile : internalFiles) {
                try (ChecksumIndexInput input = directory.openChecksumInput(internalFile.name(), IOContext.READONCE)) {
                    Streams.copy(new InputStreamIndexInput(input, internalFile.length()), positionTracking, false);
                }
            }

            return positionTracking.position();
        }

        void writeHeader(PositionTrackingOutputStreamStreamOutput positionTracking, int version) throws IOException {
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTracking);
            CodecUtil.writeHeader(new OutputStreamDataOutput(out), SHARD_COMMIT_CODEC, version);
            TransportVersion.writeVersion(TransportVersion.CURRENT, out);
            out.writeWriteable(shardId);
            out.writeVLong(generation);
            out.writeVLong(primaryTerm);
            out.writeString(nodeEphemeralId);
            out.writeMap(
                referencedBlobFiles,
                StreamOutput::writeString,
                (so, v) -> v.writeToStore(so, version >= VERSION_WITH_BLOB_LENGTH)
            );
            out.writeList(internalFiles);
            out.flush();
            // Add 8 bytes for the header size field and 4 bytes for the checksum
            headerSize = positionTracking.position() + 8 + 4;
            out.writeLong(headerSize);
            out.writeInt((int) out.getChecksum());
            out.flush();
        }

        public StatelessCompoundCommit finish(String commitFileName) {
            Map<String, BlobLocation> commitFiles = combineCommitFiles(
                commitFileName,
                primaryTerm,
                internalFiles,
                headerSize,
                referencedBlobFiles
            );

            return new StatelessCompoundCommit(shardId, generation, primaryTerm, nodeEphemeralId, Collections.unmodifiableMap(commitFiles));
        }
    }

    private static final String SHARD_COMMIT_CODEC = "stateless_commit";
    static final int VERSION_WITH_COMMIT_FILES = 0;
    static final int VERSION_WITH_BLOB_LENGTH = 1;
    static final int CURRENT_VERSION = VERSION_WITH_BLOB_LENGTH;

    public static StatelessCompoundCommit readFromStore(StreamInput input) throws IOException {
        try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(input, SHARD_COMMIT_CODEC)) {
            int version = CodecUtil.checkHeader(
                new InputStreamDataInput(in),
                SHARD_COMMIT_CODEC,
                VERSION_WITH_COMMIT_FILES,
                CURRENT_VERSION
            );
            TransportVersion transportVersion = TransportVersion.readVersion(in);
            if (TransportVersion.isCompatible(transportVersion) == false) {
                throw new IOException("Incompatible transport version: " + transportVersion);
            }
            ShardId shardId = new ShardId(in);
            long generation = in.readVLong();
            long primaryTerm = in.readVLong();
            String nodeEphemeralId = in.readString();
            Map<String, BlobLocation> referencedBlobLocations = in.readMap(
                StreamInput::readString,
                (is) -> BlobLocation.readFromStore(is, version >= VERSION_WITH_BLOB_LENGTH)
            );
            List<Writer.InternalFile> internalFiles = in.readList(Writer.InternalFile::new);
            long headerSize = in.readLong();
            long expectedChecksum = in.getChecksum();
            long readChecksum = Integer.toUnsignedLong(in.readInt());
            if (readChecksum != expectedChecksum) {
                throw new CorruptIndexException(
                    in.getSource(),
                    "checksum verification failed - expected: 0x"
                        + Long.toHexString(expectedChecksum)
                        + ", got: 0x"
                        + Long.toHexString(readChecksum)
                );
            }

            String commitFileName = NAME + generation;
            Map<String, BlobLocation> commitFiles = combineCommitFiles(
                commitFileName,
                primaryTerm,
                internalFiles,
                headerSize,
                referencedBlobLocations
            );

            return new StatelessCompoundCommit(shardId, generation, primaryTerm, nodeEphemeralId, Collections.unmodifiableMap(commitFiles));
        } catch (Exception e) {
            throw new IOException("Failed to read shard commit", e);
        }
    }

    // This method combines the pre-existing blob locations with the files internally uploaded in this commit
    // to one map of commit file locations.
    private static Map<String, BlobLocation> combineCommitFiles(
        String commitFileName,
        long primaryTerm,
        List<Writer.InternalFile> internalFiles,
        long startingOffset,
        Map<String, BlobLocation> referencedBlobFiles
    ) {
        long blobLength = internalFiles.stream().mapToLong(Writer.InternalFile::length).sum() + startingOffset;

        var commitFiles = Maps.<String, BlobLocation>newHashMapWithExpectedSize(referencedBlobFiles.size() + internalFiles.size());
        commitFiles.putAll(referencedBlobFiles);

        long currentOffset = startingOffset;
        for (Writer.InternalFile internalFile : internalFiles) {
            commitFiles.put(
                internalFile.name(),
                new BlobLocation(primaryTerm, commitFileName, blobLength, currentOffset, internalFile.length())
            );
            currentOffset += internalFile.length();
        }

        return commitFiles;
    }
}
