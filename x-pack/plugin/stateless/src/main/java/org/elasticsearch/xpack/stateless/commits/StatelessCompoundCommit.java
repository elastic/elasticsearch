/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.utils.IndexingShardRecoveryComparator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.COMPOUND_COMMIT_WITH_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents a Lucene commit point with additional information required to manage this commit in the object store as well as locally. Such
 * objects are uploaded to the object store as binary blobs.
 */
public record StatelessCompoundCommit(
    ShardId shardId,
    PrimaryTermAndGeneration primaryTermAndGeneration,
    long translogRecoveryStartFile,
    String nodeEphemeralId,
    Map<String, BlobLocation> commitFiles,
    // the size of the compound commit including codec, header, checksums and all files
    long sizeInBytes
) implements Writeable {

    public StatelessCompoundCommit(
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        Map<String, BlobLocation> commitFiles,
        long sizeInBytes
    ) {
        this(shardId, new PrimaryTermAndGeneration(primaryTerm, generation), 0, nodeEphemeralId, commitFiles, sizeInBytes);
    }

    private static final String PREFIX = "stateless_commit_";

    public PrimaryTermAndGeneration primaryTermAndGeneration() {
        return primaryTermAndGeneration;
    }

    public long primaryTerm() {
        return primaryTermAndGeneration.primaryTerm();
    }

    public long generation() {
        return primaryTermAndGeneration.generation();
    }

    @Override
    public String toString() {
        return "StatelessCompoundCommit{"
            + "shardId="
            + shardId
            + ", generation="
            + generation()
            + ", primaryTerm="
            + primaryTerm()
            + ", translogRecoveryStartFile="
            + translogRecoveryStartFile
            + ", nodeEphemeralId='"
            + nodeEphemeralId
            + "', sizeInBytes="
            + sizeInBytes
            + '}';
    }

    public String toShortDescription() {
        return '[' + blobNameFromGeneration(generation()) + "][" + primaryTerm() + "][" + generation() + ']';
    }

    public String toLongDescription() {
        return shardId + toShortDescription() + '[' + translogRecoveryStartFile + "][" + nodeEphemeralId + "][" + commitFiles + ']';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        // For backward compatibility, use different order than PrimaryTermAndGeneration.writeTo(StreamOutput)
        out.writeVLong(primaryTermAndGeneration.generation());
        out.writeVLong(primaryTermAndGeneration.primaryTerm());
        out.writeVLong(translogRecoveryStartFile);
        out.writeString(nodeEphemeralId);
        out.writeMap(commitFiles, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        if (out.getTransportVersion().onOrAfter(COMPOUND_COMMIT_WITH_SIZE)) {
            out.writeVLong(sizeInBytes);
        }
    }

    public static StatelessCompoundCommit readFromTransport(StreamInput in) throws IOException {
        return new StatelessCompoundCommit(
            new ShardId(in),
            primaryTermAndGeneration(in),
            in.readVLong(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, BlobLocation::readFromTransport),
            in.getTransportVersion().onOrAfter(COMPOUND_COMMIT_WITH_SIZE) ? in.readVLong() : 0
        );
    }

    private static PrimaryTermAndGeneration primaryTermAndGeneration(StreamInput in) throws IOException {
        // For backward compatibility, use a different order than PrimaryTermAndGeneration(StreamInput))
        long generation = in.readVLong();
        long primaryTerm = in.readVLong();
        return new PrimaryTermAndGeneration(primaryTerm, generation);
    }

    public Set<String> getInternalFiles() {
        final String compoundCommitBlobName = StatelessCompoundCommit.blobNameFromGeneration(primaryTermAndGeneration.generation());
        return commitFiles().entrySet()
            .stream()
            .filter(commitFile -> compoundCommitBlobName.equals(commitFile.getValue().blobName()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Writes the StatelessCompoundCommit header to the given StreamOutput and returns the number of bytes written
     * @return the header size in bytes
     */
    static long writeHeader(
        PositionTrackingOutputStreamStreamOutput positionTracking,
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        long translogRecoveryStartFile,
        Map<String, BlobLocation> referencedBlobFiles,
        List<InternalFile> internalFiles
    ) throws IOException {
        return writeHeader(
            positionTracking,
            shardId,
            generation,
            primaryTerm,
            nodeEphemeralId,
            translogRecoveryStartFile,
            referencedBlobFiles,
            internalFiles,
            CURRENT_VERSION
        );
    }

    static void writeInternalFilesToStore(OutputStream outputStream, List<InternalFile> internalFiles, Directory directory)
        throws IOException {
        for (InternalFile internalFile : internalFiles) {
            try (ChecksumIndexInput input = directory.openChecksumInput(internalFile.name(), IOContext.READONCE)) {
                Streams.copy(new InputStreamIndexInput(input, internalFile.length()), outputStream, false);
            }
        }
    }

    // visible for testing
    static long writeHeader(
        PositionTrackingOutputStreamStreamOutput positionTracking,
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        long translogRecoveryStartFile,
        Map<String, BlobLocation> referencedBlobFiles,
        List<InternalFile> internalFiles,
        int version
    ) throws IOException {
        if (version < VERSION_WITH_XCONTENT_ENCODING) {
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTracking);
            CodecUtil.writeHeader(new OutputStreamDataOutput(out), SHARD_COMMIT_CODEC, version);
            TransportVersion.writeVersion(TransportVersion.current(), out);
            out.writeWriteable(shardId);
            out.writeVLong(generation);
            out.writeVLong(primaryTerm);
            out.writeString(nodeEphemeralId);
            out.writeMap(
                referencedBlobFiles,
                StreamOutput::writeString,
                (so, v) -> v.writeToStore(so, version >= VERSION_WITH_BLOB_LENGTH)
            );
            out.writeCollection(internalFiles);
            out.flush();
            // Add 8 bytes for the header size field and 4 bytes for the checksum
            var headerSize = positionTracking.position() + 8 + 4;
            out.writeLong(headerSize);
            out.writeInt((int) out.getChecksum());
            out.flush();
            return headerSize;
        } else {
            return writeXContentHeader(
                shardId,
                generation,
                primaryTerm,
                nodeEphemeralId,
                translogRecoveryStartFile,
                referencedBlobFiles,
                internalFiles,
                version,
                positionTracking
            );
        }
    }

    private static long writeXContentHeader(
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        long translogRecoveryStartFile,
        Map<String, BlobLocation> referencedBlobFiles,
        List<InternalFile> internalFiles,
        int version,
        PositionTrackingOutputStreamStreamOutput positionTracking
    ) throws IOException {
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTracking);
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), SHARD_COMMIT_CODEC, version);
        long codecSize = positionTracking.position();

        var bytesStreamOutput = new BytesStreamOutput();
        try (var b = new XContentBuilder(XContentType.SMILE.xContent(), bytesStreamOutput)) {
            b.startObject();
            {
                shardIdXContent(shardId, b);
                b.field("generation", generation);
                b.field("primary_term", primaryTerm);
                b.field("node_ephemeral_id", nodeEphemeralId);
                b.field(IndexEngine.TRANSLOG_RECOVERY_START_FILE, translogRecoveryStartFile);
                b.startObject("commit_files");
                {
                    for (Map.Entry<String, BlobLocation> e : referencedBlobFiles.entrySet()) {
                        b.field(e.getKey());
                        e.getValue().toXContent(b, ToXContent.EMPTY_PARAMS);
                    }
                }
                b.endObject();
                b.startArray("internal_files");
                {
                    for (InternalFile f : internalFiles) {
                        f.toXContent(b, ToXContent.EMPTY_PARAMS);
                    }
                }
                b.endArray();
            }
            b.endObject();
        }
        // Write the end marker manually, can't customize XContent to use SmileGenerator.Feature#WRITE_END_MARKER
        bytesStreamOutput.write(XContentType.SMILE.xContent().streamSeparator());
        bytesStreamOutput.flush();

        BytesReference xContentHeader = bytesStreamOutput.bytes();
        out.writeInt(xContentHeader.length());
        out.writeInt((int) out.getChecksum());
        xContentHeader.writeTo(out);
        out.writeInt((int) out.getChecksum());
        out.flush();

        var headerSize = positionTracking.position();
        assert headerSize >= 0;
        assert headerSize == codecSize + 4 + 4 + xContentHeader.length() + 4;
        return headerSize;
    }

    private static final String SHARD_COMMIT_CODEC = "stateless_commit";
    static final int VERSION_WITH_COMMIT_FILES = 0;
    static final int VERSION_WITH_BLOB_LENGTH = 1;
    static final int VERSION_WITH_XCONTENT_ENCODING = 2;
    static final int CURRENT_VERSION = VERSION_WITH_XCONTENT_ENCODING;

    public static StatelessCompoundCommit readFromStore(StreamInput in) throws IOException {
        return readFromStoreAtOffset(in, 0, StatelessCompoundCommit::blobNameFromGeneration);
    }

    /**
     * Reads the compound commit header from the data store at the specified offset within the input stream.
     * It's expected that the input stream is already positioned at the specified offset.
     * The {@param offset} parameter is utilized to construct the {@link StatelessCompoundCommit} instance,
     * referring to the compound commit at the given offset within the {@link BatchedCompoundCommit}.
     * @param in the input stream to read from
     * @param offset the offset within the blob where this compound commit header starts
     * @param blobNameSupplier a function that provides the blob name where this compound commit is stored
     */
    public static StatelessCompoundCommit readFromStoreAtOffset(StreamInput in, long offset, Function<Long, String> blobNameSupplier)
        throws IOException {
        try (BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(in, SHARD_COMMIT_CODEC)) {
            int version = CodecUtil.checkHeader(
                new InputStreamDataInput(input),
                SHARD_COMMIT_CODEC,
                VERSION_WITH_COMMIT_FILES,
                CURRENT_VERSION
            );
            if (version < VERSION_WITH_XCONTENT_ENCODING) {
                TransportVersion.readVersion(input);
                ShardId shardId = new ShardId(input);
                long generation = input.readVLong();
                long primaryTerm = input.readVLong();
                String nodeEphemeralId = input.readString();
                Map<String, BlobLocation> referencedBlobLocations = input.readMap(
                    StreamInput::readString,
                    (is) -> BlobLocation.readFromStore(is, version == VERSION_WITH_BLOB_LENGTH)
                );
                List<InternalFile> internalFiles = input.readCollectionAsList(InternalFile::new);
                long headerSize = input.readLong();
                verifyChecksum(input);
                long totalSizeInBytes = headerSize + internalFiles.stream().mapToLong(InternalFile::length).sum();
                return statelessCompoundCommit(
                    shardId,
                    generation,
                    primaryTerm,
                    0,
                    nodeEphemeralId,
                    referencedBlobLocations,
                    internalFiles,
                    offset,
                    headerSize,
                    totalSizeInBytes,
                    blobNameSupplier
                );
            } else {
                assert version == VERSION_WITH_XCONTENT_ENCODING;

                int xContentLength = input.readInt();
                verifyChecksum(input);

                byte[] bytes = new byte[xContentLength];
                input.readBytes(bytes, 0, bytes.length);
                verifyChecksum(input);

                // codec header + serialized header size + checksum + header content + checksum
                var headerSize = CodecUtil.headerLength(SHARD_COMMIT_CODEC) + 4 + 4 + xContentLength + 4;
                return readXContentHeader(new BytesArray(bytes).streamInput(), headerSize, offset, blobNameSupplier);
            }
        } catch (Exception e) {
            throw new IOException("Failed to read shard commit", e);
        }
    }

    private static void verifyChecksum(BufferedChecksumStreamInput input) throws IOException {
        long actualChecksum = input.getChecksum();
        long expectedChecksum = Integer.toUnsignedLong(input.readInt());
        if (actualChecksum != expectedChecksum) {
            throw new CorruptIndexException(
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(actualChecksum),
                input.getSource()
            );
        }
    }

    private static StatelessCompoundCommit readXContentHeader(
        StreamInput is,
        long headerSize,
        long offset,
        Function<Long, String> blobNameSupplier
    ) throws IOException {
        record XContentStatelessCompoundCommit(
            ShardId shardId,
            long generation,
            long primaryTerm,
            long translogRecoveryStartFile,
            String nodeEphemeralId,
            Map<String, BlobLocation> referencedBlobLocations,
            List<InternalFile> internalFiles
        ) {
            @SuppressWarnings("unchecked")
            private static final ConstructingObjectParser<XContentStatelessCompoundCommit, Void> PARSER = new ConstructingObjectParser<>(
                "stateless_compound_commit",
                true,
                args -> new XContentStatelessCompoundCommit(
                    (ShardId) args[0],
                    (long) args[1],
                    (long) args[2],
                    args[3] == null ? 0 : (long) args[3],
                    (String) args[4],
                    (Map<String, BlobLocation>) args[5],
                    (List<InternalFile>) args[6]
                )
            );
            static {
                PARSER.declareObject(constructorArg(), SHARD_ID_PARSER, new ParseField("shard_id"));
                PARSER.declareLong(constructorArg(), new ParseField("generation"));
                PARSER.declareLong(constructorArg(), new ParseField("primary_term"));
                PARSER.declareLong(optionalConstructorArg(), new ParseField(IndexEngine.TRANSLOG_RECOVERY_START_FILE));
                PARSER.declareString(constructorArg(), new ParseField("node_ephemeral_id"));
                PARSER.declareObject(
                    constructorArg(),
                    (p, c) -> p.map(HashMap::new, BlobLocation::fromXContent),
                    new ParseField("commit_files")
                );
                PARSER.declareObjectArray(constructorArg(), InternalFile.PARSER, new ParseField("internal_files"));
            }
        }

        try (XContentParser parser = XContentType.SMILE.xContent().createParser(XContentParserConfiguration.EMPTY, is)) {
            XContentStatelessCompoundCommit c = XContentStatelessCompoundCommit.PARSER.parse(parser, null);
            assert headerSize > 0;
            long totalSizeInBytes = headerSize + c.internalFiles.stream().mapToLong(InternalFile::length).sum();
            return statelessCompoundCommit(
                c.shardId,
                c.generation,
                c.primaryTerm,
                c.translogRecoveryStartFile,
                c.nodeEphemeralId,
                c.referencedBlobLocations,
                c.internalFiles,
                offset,
                headerSize,
                totalSizeInBytes,
                blobNameSupplier
            );
        }
    }

    private static StatelessCompoundCommit statelessCompoundCommit(
        ShardId shardId,
        long generation,
        long primaryTerm,
        long translogRecoveryStartFile,
        String nodeEphemeralId,
        Map<String, BlobLocation> referencedBlobLocations,
        List<InternalFile> internalFiles,
        long internalFilesOffset,
        long headerSizeInBytes,
        long totalSizeInBytes,
        Function<Long, String> blobNameSupplier
    ) {
        String commitFileName = blobNameSupplier.apply(generation);
        Map<String, BlobLocation> commitFiles = combineCommitFiles(
            commitFileName,
            primaryTerm,
            internalFiles,
            referencedBlobLocations,
            internalFilesOffset,
            headerSizeInBytes,
            totalSizeInBytes
        );
        return new StatelessCompoundCommit(
            shardId,
            new PrimaryTermAndGeneration(primaryTerm, generation),
            translogRecoveryStartFile,
            nodeEphemeralId,
            Collections.unmodifiableMap(commitFiles),
            totalSizeInBytes
        );
    }

    // This method combines the pre-existing blob locations with the files internally uploaded in this commit
    // to one map of commit file locations.
    private static Map<String, BlobLocation> combineCommitFiles(
        String blobName,
        long primaryTerm,
        List<InternalFile> internalFiles,
        Map<String, BlobLocation> referencedBlobFiles,
        long internalFilesOffset,
        long headerSizeInBytes,
        long totalSizeInBytes
    ) {
        long blobLength = internalFilesOffset + totalSizeInBytes;

        var commitFiles = Maps.<String, BlobLocation>newHashMapWithExpectedSize(referencedBlobFiles.size() + internalFiles.size());
        commitFiles.putAll(referencedBlobFiles);

        long currentOffset = internalFilesOffset + headerSizeInBytes;
        for (InternalFile internalFile : internalFiles) {
            commitFiles.put(internalFile.name(), new BlobLocation(primaryTerm, blobName, blobLength, currentOffset, internalFile.length()));
            currentOffset += internalFile.length();
        }

        return commitFiles;
    }

    private static void shardIdXContent(ShardId shardId, XContentBuilder b) throws IOException {
        // Can't use Shard#toXContent because it loses index_uuid
        b.startObject("shard_id").field("index", shardId.getIndex()).field("id", shardId.id()).endObject();
    }

    private static final ConstructingObjectParser<ShardId, Void> SHARD_ID_PARSER = new ConstructingObjectParser<>(
        "shard_id",
        args -> new ShardId((Index) args[0], (int) args[1])
    );
    static {
        SHARD_ID_PARSER.declareObject(constructorArg(), (p, c) -> Index.fromXContent(p), new ParseField("index"));
        SHARD_ID_PARSER.declareInt(constructorArg(), new ParseField("id"));
    }

    // Since CC and BCC share the same naming scheme, this method works equally for both of them.
    public static boolean startsWithBlobPrefix(String name) {
        return name.startsWith(StatelessCompoundCommit.PREFIX);
    }

    // Since CC and BCC share the same naming scheme, this method works equally for both of them.
    public static String blobNameFromGeneration(long generation) {
        assert generation > 0 : generation;
        return StatelessCompoundCommit.PREFIX + generation;
    }

    // Since CC and BCC share the same naming scheme, this method works equally for both of them.
    public static long parseGenerationFromBlobName(String name) {
        assert startsWithBlobPrefix(name) : name;
        return Long.parseLong(name.substring(name.lastIndexOf('_') + 1));
    }

    record InternalFile(String name, long length) implements Writeable, ToXContentObject, Comparable<InternalFile> {

        private static final ConstructingObjectParser<InternalFile, Void> PARSER = new ConstructingObjectParser<>(
            "internal_file",
            true,
            args -> new InternalFile((String) args[0], (long) args[1])
        );

        // Order commit files in an optimized order for indexing shard recoveries
        public static final Comparator<String> INTERNAL_FILES_COMPARATOR = new IndexingShardRecoveryComparator();

        static {
            PARSER.declareString(constructorArg(), new ParseField("name"));
            PARSER.declareLong(constructorArg(), new ParseField("length"));
        }

        private InternalFile(StreamInput streamInput) throws IOException {
            this(streamInput.readString(), streamInput.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeLong(length);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("name", name).field("length", length).endObject();
        }

        @Override
        public int compareTo(InternalFile o) {
            return INTERNAL_FILES_COMPARATOR.compare(this.name, o.name);
        }
    }
}
