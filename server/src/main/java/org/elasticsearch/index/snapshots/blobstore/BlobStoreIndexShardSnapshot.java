/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Shard snapshot metadata
 */
public class BlobStoreIndexShardSnapshot implements ToXContentFragment {

    /**
     * Information about snapshotted file
     */
    public static class FileInfo implements Writeable {

        private final String name;
        private final ByteSizeValue partSize;
        private final long partBytes;
        private final int numberOfParts;
        private final StoreFileMetadata metadata;

        /**
         * Constructs a new instance of file info
         *
         * @param name         file name as stored in the blob store
         * @param metadata  the files meta data
         * @param partSize     size of the single chunk
         */
        public FileInfo(String name, StoreFileMetadata metadata, ByteSizeValue partSize) {
            this.name = Objects.requireNonNull(name);
            this.metadata = metadata;

            long partBytes = Long.MAX_VALUE;
            if (partSize != null && partSize.getBytes() > 0) {
                partBytes = partSize.getBytes();
            }

            if (metadata.length() == 0) {
                numberOfParts = 1;
            } else {
                long longNumberOfParts = 1L + (metadata.length() - 1L) / partBytes; // ceil(len/partBytes), but beware of long overflow
                numberOfParts = (int) longNumberOfParts;
                if (numberOfParts != longNumberOfParts) { // also beware of int overflow, although 2^32 parts is already ludicrous
                    throw new IllegalArgumentException("part size [" + partSize + "] too small for file [" + metadata + "]");
                }
            }

            this.partSize = partSize;
            this.partBytes = partBytes;
            assert IntStream.range(0, numberOfParts).mapToLong(this::partBytes).sum() == metadata.length();
        }

        public FileInfo(StreamInput in) throws IOException {
            this(in.readString(), new StoreFileMetadata(in), in.readOptionalWriteable(ByteSizeValue::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            metadata.writeTo(out);
            out.writeOptionalWriteable(partSize);
        }

        /**
         * Returns the base file name
         *
         * @return file name
         */
        public String name() {
            return name;
        }

        /**
         * Returns part name if file is stored as multiple parts
         *
         * @param part part number
         * @return part name
         */
        public String partName(int part) {
            if (numberOfParts > 1) {
                return name + ".part" + part;
            } else {
                return name;
            }
        }

        /**
         * Returns base file name from part name
         *
         * @param blobName part name
         * @return base file name
         */
        public static String canonicalName(String blobName) {
            if (blobName.contains(".part")) {
                return blobName.substring(0, blobName.indexOf(".part"));
            }
            return blobName;
        }

        /**
         * Returns original file name
         *
         * @return original file name
         */
        public String physicalName() {
            return metadata.name();
        }

        /**
         * File length
         *
         * @return file length
         */
        public long length() {
            return metadata.length();
        }

        /**
         * Returns part size
         *
         * @return part size
         */
        public ByteSizeValue partSize() {
            return partSize;
        }

        /**
         * Returns the size (in bytes) of a given part
         *
         * @return the size (in bytes) of a given part
         */
        public long partBytes(int part) {
            assert 0 <= part && part < numberOfParts : part + " vs " + numberOfParts;
            if (numberOfParts == 1) {
                return length();
            }
            // First and last-but-one parts have a size equal to partBytes
            if (part < (numberOfParts - 1)) {
                return partBytes;
            }
            // Last part size is deducted from the length and the number of parts
            final long lastPartBytes = length() - (this.partBytes * (numberOfParts - 1));
            assert 0 < lastPartBytes && lastPartBytes <= partBytes : lastPartBytes + " vs " + partBytes;
            return lastPartBytes;
        }

        /**
         * Returns number of parts
         *
         * @return number of parts
         */
        public int numberOfParts() {
            return numberOfParts;
        }

        /**
         * Returns file md5 checksum provided by {@link org.elasticsearch.index.store.Store}
         *
         * @return file checksum
         */
        public String checksum() {
            return metadata.checksum();
        }

        /**
         * Returns the StoreFileMetadata for this file info.
         */
        public StoreFileMetadata metadata() {
            return metadata;
        }

        /**
         * Checks if a file in a store is the same file
         *
         * @param md file in a store
         * @return true if file in a store this this file have the same checksum and length
         */
        public boolean isSame(StoreFileMetadata md) {
            return metadata.isSame(md);
        }

        /**
         * Checks if a file in a store is the same file
         *
         * @param fileInfo file in a store
         * @return true if file in a store this this file have the same checksum and length
         */
        public boolean isSame(FileInfo fileInfo) {
            if (numberOfParts != fileInfo.numberOfParts) {
                return false;
            }
            if (partBytes != fileInfo.partBytes) {
                return false;
            }
            if (name.equals(fileInfo.name) == false) {
                return false;
            }
            if (partSize != null) {
                if (partSize.equals(fileInfo.partSize) == false) {
                    return false;
                }
            } else {
                if (fileInfo.partSize != null) {
                    return false;
                }
            }
            return metadata.isSame(fileInfo.metadata);
        }

        static final String NAME = "name";
        static final String PHYSICAL_NAME = "physical_name";
        static final String LENGTH = "length";
        static final String CHECKSUM = "checksum";
        static final String PART_SIZE = "part_size";
        static final String WRITTEN_BY = "written_by";
        static final String META_HASH = "meta_hash";

        /**
         * Serializes file info into JSON
         *
         * @param file    file info
         * @param builder XContent builder
         * @param params  parameters
         */
        public static void toXContent(FileInfo file, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(NAME, file.name);
            builder.field(PHYSICAL_NAME, file.metadata.name());
            builder.field(LENGTH, file.metadata.length());
            builder.field(CHECKSUM, file.metadata.checksum());
            if (file.partSize != null) {
                builder.field(PART_SIZE, file.partSize.getBytes());
            }

            if (file.metadata.writtenBy() != null) {
                builder.field(WRITTEN_BY, file.metadata.writtenBy());
            }

            if (file.metadata.hash() != null && file.metadata().hash().length > 0) {
                BytesRef br = file.metadata.hash();
                builder.field(META_HASH, br.bytes, br.offset, br.length);
            }
            builder.endObject();
        }

        /**
         * Parses JSON that represents file info
         *
         * @param parser parser
         * @return file info
         */
        public static FileInfo fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            String name = null;
            String physicalName = null;
            long length = -1;
            String checksum = null;
            ByteSizeValue partSize = null;
            String writtenBy = null;
            BytesRef metaHash = new BytesRef();
            XContentParserUtils.ensureExpectedToken(token, XContentParser.Token.START_OBJECT, parser);
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if (NAME.equals(currentFieldName)) {
                            name = parser.text();
                        } else if (PHYSICAL_NAME.equals(currentFieldName)) {
                            physicalName = parser.text();
                        } else if (LENGTH.equals(currentFieldName)) {
                            length = parser.longValue();
                        } else if (CHECKSUM.equals(currentFieldName)) {
                            checksum = parser.text();
                        } else if (PART_SIZE.equals(currentFieldName)) {
                            partSize = new ByteSizeValue(parser.longValue());
                        } else if (WRITTEN_BY.equals(currentFieldName)) {
                            writtenBy = parser.text();
                        } else if (META_HASH.equals(currentFieldName)) {
                            metaHash.bytes = parser.binaryValue();
                            metaHash.offset = 0;
                            metaHash.length = metaHash.bytes.length;
                        } else {
                            XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                        }
                    } else {
                        XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
                    }
                } else {
                    XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
                }
            }

            // Verify that file information is complete
            if (name == null || Strings.validFileName(name) == false) {
                throw new ElasticsearchParseException("missing or invalid file name [" + name + "]");
            } else if (physicalName == null || Strings.validFileName(physicalName) == false) {
                throw new ElasticsearchParseException("missing or invalid physical file name [" + physicalName + "]");
            } else if (length < 0) {
                throw new ElasticsearchParseException("missing or invalid file length");
            } else if (writtenBy == null) {
                throw new ElasticsearchParseException("missing or invalid written_by [" + writtenBy + "]");
            } else if (checksum == null) {
                throw new ElasticsearchParseException("missing checksum for name [" + name + "]");
            }
            return new FileInfo(name, new StoreFileMetadata(physicalName, length, checksum, writtenBy, metaHash), partSize);
        }

        @Override
        public String toString() {
            return "[name: "
                + name
                + ", numberOfParts: "
                + numberOfParts
                + ", partSize: "
                + partSize
                + ", partBytes: "
                + partBytes
                + ", metadata: "
                + metadata
                + "]";
        }
    }

    /**
     * Snapshot name
     */
    private final String snapshot;

    private final long indexVersion;

    private final long startTime;

    private final long time;

    private final int incrementalFileCount;

    private final long incrementalSize;

    private final List<FileInfo> indexFiles;

    /**
     * Constructs new shard snapshot metadata from snapshot metadata
     *
     * @param snapshot              snapshot name
     * @param indexVersion          index version
     * @param indexFiles            list of files in the shard
     * @param startTime             snapshot start time
     * @param time                  snapshot running time
     * @param incrementalFileCount  incremental of files that were snapshotted
     * @param incrementalSize       incremental size of snapshot
     */
    public BlobStoreIndexShardSnapshot(
        String snapshot,
        long indexVersion,
        List<FileInfo> indexFiles,
        long startTime,
        long time,
        int incrementalFileCount,
        long incrementalSize
    ) {
        assert snapshot != null;
        assert indexVersion >= 0;
        this.snapshot = snapshot;
        this.indexVersion = indexVersion;
        this.indexFiles = org.elasticsearch.core.List.copyOf(indexFiles);
        this.startTime = startTime;
        this.time = time;
        this.incrementalFileCount = incrementalFileCount;
        this.incrementalSize = incrementalSize;
    }

    /**
     * Creates a new instance has a different name and zero incremental file counts but is identical to this instance in terms of the files
     * it references.
     *
     * @param targetSnapshotName target snapshot name
     * @param startTime          time the clone operation on the repository was started
     * @param time               time it took to create the clone
     */
    public BlobStoreIndexShardSnapshot asClone(String targetSnapshotName, long startTime, long time) {
        return new BlobStoreIndexShardSnapshot(targetSnapshotName, indexVersion, indexFiles, startTime, time, 0, 0);
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * Returns list of files in the shard
     *
     * @return list of files
     */
    public List<FileInfo> indexFiles() {
        return indexFiles;
    }

    /**
     * Returns snapshot start time
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Returns snapshot running time
     */
    public long time() {
        return time;
    }

    /**
     * Returns incremental of files that were snapshotted
     */
    public int incrementalFileCount() {
        return incrementalFileCount;
    }

    /**
     * Returns total number of files that are referenced by this snapshot
     */
    public int totalFileCount() {
        return indexFiles.size();
    }

    /**
     * Returns incremental of files size that were snapshotted
     */
    public long incrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns total size of all files that where snapshotted
     */
    public long totalSize() {
        return totalSize(indexFiles);
    }

    public static long totalSize(List<FileInfo> indexFiles) {
        return indexFiles.stream().mapToLong(fi -> fi.metadata().length()).sum();
    }

    private static final String NAME = "name";
    private static final String INDEX_VERSION = "index_version";
    private static final String START_TIME = "start_time";
    private static final String TIME = "time";
    private static final String FILES = "files";
    // for the sake of BWC keep the actual property names as in 6.x
    // + there is a constraint in #fromXContent() that leads to ElasticsearchParseException("unknown parameter [incremental_file_count]");
    private static final String INCREMENTAL_FILE_COUNT = "number_of_files";
    private static final String INCREMENTAL_SIZE = "total_size";

    private static final ParseField PARSE_NAME = new ParseField(NAME);
    private static final ParseField PARSE_INDEX_VERSION = new ParseField(INDEX_VERSION, "index-version");
    private static final ParseField PARSE_START_TIME = new ParseField(START_TIME);
    private static final ParseField PARSE_TIME = new ParseField(TIME);
    private static final ParseField PARSE_INCREMENTAL_FILE_COUNT = new ParseField(INCREMENTAL_FILE_COUNT);
    private static final ParseField PARSE_INCREMENTAL_SIZE = new ParseField(INCREMENTAL_SIZE);
    private static final ParseField PARSE_FILES = new ParseField(FILES);

    /**
     * Serializes shard snapshot metadata info into JSON
     *
     * @param builder  XContent builder
     * @param params   parameters
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, snapshot);
        builder.field(INDEX_VERSION, indexVersion);
        builder.field(START_TIME, startTime);
        builder.field(TIME, time);
        builder.field(INCREMENTAL_FILE_COUNT, incrementalFileCount);
        builder.field(INCREMENTAL_SIZE, incrementalSize);
        builder.startArray(FILES);
        for (FileInfo fileInfo : indexFiles) {
            FileInfo.toXContent(fileInfo, builder, params);
        }
        builder.endArray();
        return builder;
    }

    /**
     * Parses shard snapshot metadata
     *
     * @param parser parser
     * @return shard snapshot metadata
     */
    public static BlobStoreIndexShardSnapshot fromXContent(XContentParser parser) throws IOException {
        String snapshot = null;
        long indexVersion = -1;
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        long incrementalSize = 0;

        List<FileInfo> indexFiles = null;
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            final String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token.isValue()) {
                if (PARSE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    snapshot = parser.text();
                } else if (PARSE_INDEX_VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                    // The index-version is needed for backward compatibility with v 1.0
                    indexVersion = parser.longValue();
                } else if (PARSE_START_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    startTime = parser.longValue();
                } else if (PARSE_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else if (PARSE_INCREMENTAL_FILE_COUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                    incrementalFileCount = parser.intValue();
                } else if (PARSE_INCREMENTAL_SIZE.match(currentFieldName, parser.getDeprecationHandler())) {
                    incrementalSize = parser.longValue();
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (PARSE_FILES.match(currentFieldName, parser.getDeprecationHandler())) {
                    indexFiles = XContentParserUtils.parseList(parser, FileInfo::fromXContent);
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
            }
        }

        return new BlobStoreIndexShardSnapshot(
            snapshot,
            indexVersion,
            indexFiles == null ? org.elasticsearch.core.List.of() : indexFiles,
            startTime,
            time,
            incrementalFileCount,
            incrementalSize
        );
    }
}
