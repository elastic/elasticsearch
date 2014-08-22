/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.snapshots.blobstore;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Shard snapshot metadata
 */
public class BlobStoreIndexShardSnapshot {

    /**
     * Information about snapshotted file
     */
    public static class FileInfo {
        private final String name;
        private final ByteSizeValue partSize;
        private final long partBytes;
        private final long numberOfParts;
        private final StoreFileMetaData metadata;

        /**
         * Constructs a new instance of file info
         *
         * @param name         file name as stored in the blob store
         * @param metaData  the files meta data
         * @param partSize     size of the single chunk
         */
        public FileInfo(String name, StoreFileMetaData metaData, ByteSizeValue partSize) {
            this.name = name;
            this.metadata = metaData;

            long partBytes = Long.MAX_VALUE;
            if (partSize != null) {
                partBytes = partSize.bytes();
            }

            long totalLength = metaData.length();
            long numberOfParts = totalLength / partBytes;
            if (totalLength % partBytes > 0) {
                numberOfParts++;
            }
            if (numberOfParts == 0) {
                numberOfParts++;
            }
            this.numberOfParts = numberOfParts;
            this.partSize = partSize;
            this.partBytes = partBytes;
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
        public String partName(long part) {
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
         * Return maximum number of bytes in a part
         *
         * @return maximum number of bytes in a part
         */
        public long partBytes() {
            return partBytes;
        }

        /**
         * Returns number of parts
         *
         * @return number of parts
         */
        public long numberOfParts() {
            return numberOfParts;
        }

        /**
         * Returns file md5 checksum provided by {@link org.elasticsearch.index.store.Store}
         *
         * @return file checksum
         */
        @Nullable
        public String checksum() {
            return metadata.checksum();
        }

        /**
         * Returns the StoreFileMetaData for this file info.
         */
        public StoreFileMetaData metadata() {
            return metadata;
        }

        /**
         * Checks if a file in a store is the same file
         *
         * @param md file in a store
         * @return true if file in a store this this file have the same checksum and length
         */
        public boolean isSame(StoreFileMetaData md) {
            return metadata.isSame(md);
        }

        static final class Fields {
            static final XContentBuilderString NAME = new XContentBuilderString("name");
            static final XContentBuilderString PHYSICAL_NAME = new XContentBuilderString("physical_name");
            static final XContentBuilderString LENGTH = new XContentBuilderString("length");
            static final XContentBuilderString CHECKSUM = new XContentBuilderString("checksum");
            static final XContentBuilderString PART_SIZE = new XContentBuilderString("part_size");
            static final XContentBuilderString WRITTEN_BY = new XContentBuilderString("written_by");
            static final XContentBuilderString META_HASH = new XContentBuilderString("meta_hash");
        }

        /**
         * Serializes file info into JSON
         *
         * @param file    file info
         * @param builder XContent builder
         * @param params  parameters
         * @throws IOException
         */
        public static void toXContent(FileInfo file, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, file.name);
            builder.field(Fields.PHYSICAL_NAME, file.metadata.name());
            builder.field(Fields.LENGTH, file.metadata.length());
            if (file.metadata.checksum() != null) {
                builder.field(Fields.CHECKSUM, file.metadata.checksum());
            }
            if (file.partSize != null) {
                builder.field(Fields.PART_SIZE, file.partSize.bytes());
            }

            if (file.metadata.writtenBy() != null) {
                builder.field(Fields.WRITTEN_BY, file.metadata.writtenBy());
            }

            if (file.metadata.hash() != null && file.metadata().hash().length > 0) {
                builder.field(Fields.META_HASH, file.metadata.hash());
            }
            builder.endObject();
        }

        /**
         * Parses JSON that represents file info
         *
         * @param parser parser
         * @return file info
         * @throws IOException
         */
        public static FileInfo fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            String name = null;
            String physicalName = null;
            long length = -1;
            String checksum = null;
            ByteSizeValue partSize = null;
            Version writtenBy = null;
            BytesRef metaHash = new BytesRef();
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token.isValue()) {
                            if ("name".equals(currentFieldName)) {
                                name = parser.text();
                            } else if ("physical_name".equals(currentFieldName)) {
                                physicalName = parser.text();
                            } else if ("length".equals(currentFieldName)) {
                                length = parser.longValue();
                            } else if ("checksum".equals(currentFieldName)) {
                                checksum = parser.text();
                            } else if ("part_size".equals(currentFieldName)) {
                                partSize = new ByteSizeValue(parser.longValue());
                            } else if ("written_by".equals(currentFieldName)) {
                                writtenBy = Lucene.parseVersionLenient(parser.text(), null);
                            } else if ("meta_hash".equals(currentFieldName)) {
                                metaHash.bytes = parser.binaryValue();
                                metaHash.offset = 0;
                                metaHash.length = metaHash.bytes.length;
                            } else {
                                throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "]");
                            }
                        } else {
                            throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                        }
                    } else {
                        throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                    }
                }
            }
            // TODO: Verify???
            return new FileInfo(name, new StoreFileMetaData(physicalName, length, checksum, writtenBy, metaHash), partSize);
        }

    }

    private final String snapshot;

    private final long indexVersion;

    private final long startTime;

    private final long time;

    private final int numberOfFiles;

    private final long totalSize;

    private final ImmutableList<FileInfo> indexFiles;

    /**
     * Constructs new shard snapshot metadata from snapshot metadata
     *
     * @param snapshot      snapshot id
     * @param indexVersion  index version
     * @param indexFiles    list of files in the shard
     * @param startTime     snapshot start time
     * @param time          snapshot running time
     * @param numberOfFiles number of files that where snapshotted
     * @param totalSize     total size of all files snapshotted
     */
    public BlobStoreIndexShardSnapshot(String snapshot, long indexVersion, List<FileInfo> indexFiles, long startTime, long time,
                                       int numberOfFiles, long totalSize) {
        assert snapshot != null;
        assert indexVersion >= 0;
        this.snapshot = snapshot;
        this.indexVersion = indexVersion;
        this.indexFiles = ImmutableList.copyOf(indexFiles);
        this.startTime = startTime;
        this.time = time;
        this.numberOfFiles = numberOfFiles;
        this.totalSize = totalSize;
    }

    /**
     * Returns index version
     *
     * @return index version
     */
    public long indexVersion() {
        return indexVersion;
    }

    /**
     * Returns snapshot id
     *
     * @return snapshot id
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * Returns list of files in the shard
     *
     * @return list of files
     */
    public ImmutableList<FileInfo> indexFiles() {
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
     * Returns number of files that where snapshotted
     */
    public int numberOfFiles() {
        return numberOfFiles;
    }

    /**
     * Returns total size of all files that where snapshotted
     */
    public long totalSize() {
        return totalSize;
    }

    static final class Fields {
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString INDEX_VERSION = new XContentBuilderString("index_version");
        static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString NUMBER_OF_FILES = new XContentBuilderString("number_of_files");
        static final XContentBuilderString TOTAL_SIZE = new XContentBuilderString("total_size");
        static final XContentBuilderString FILES = new XContentBuilderString("files");
    }

    static final class ParseFields {
        static final ParseField NAME = new ParseField("name");
        static final ParseField INDEX_VERSION = new ParseField("index_version", "index-version");
        static final ParseField START_TIME = new ParseField("start_time");
        static final ParseField TIME = new ParseField("time");
        static final ParseField NUMBER_OF_FILES = new ParseField("number_of_files");
        static final ParseField TOTAL_SIZE = new ParseField("total_size");
        static final ParseField FILES = new ParseField("files");
    }


    /**
     * Serializes shard snapshot metadata info into JSON
     *
     * @param snapshot shard snapshot metadata
     * @param builder  XContent builder
     * @param params   parameters
     * @throws IOException
     */
    public static void toXContent(BlobStoreIndexShardSnapshot snapshot, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.NAME, snapshot.snapshot);
        builder.field(Fields.INDEX_VERSION, snapshot.indexVersion);
        builder.field(Fields.START_TIME, snapshot.startTime);
        builder.field(Fields.TIME, snapshot.time);
        builder.field(Fields.NUMBER_OF_FILES, snapshot.numberOfFiles);
        builder.field(Fields.TOTAL_SIZE, snapshot.totalSize);
        builder.startArray(Fields.FILES);
        for (FileInfo fileInfo : snapshot.indexFiles) {
            FileInfo.toXContent(fileInfo, builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    /**
     * Parses shard snapshot metadata
     *
     * @param parser parser
     * @return shard snapshot metadata
     * @throws IOException
     */
    public static BlobStoreIndexShardSnapshot fromXContent(XContentParser parser) throws IOException {

        String snapshot = null;
        long indexVersion = -1;
        long startTime = 0;
        long time = 0;
        int numberOfFiles = 0;
        long totalSize = 0;

        List<FileInfo> indexFiles = newArrayList();

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if (ParseFields.NAME.match(currentFieldName)) {
                            snapshot = parser.text();
                        } else if (ParseFields.INDEX_VERSION.match(currentFieldName)) {
                            // The index-version is needed for backward compatibility with v 1.0
                            indexVersion = parser.longValue();
                        } else if (ParseFields.START_TIME.match(currentFieldName)) {
                            startTime = parser.longValue();
                        } else if (ParseFields.TIME.match(currentFieldName)) {
                            time = parser.longValue();
                        } else if (ParseFields.NUMBER_OF_FILES.match(currentFieldName)) {
                            numberOfFiles = parser.intValue();
                        } else if (ParseFields.TOTAL_SIZE.match(currentFieldName)) {
                            totalSize = parser.longValue();
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "]");
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (ParseFields.FILES.match(currentFieldName)) {
                            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                indexFiles.add(FileInfo.fromXContent(parser));
                            }
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                }
            }
        }
        return new BlobStoreIndexShardSnapshot(snapshot, indexVersion, ImmutableList.<FileInfo>copyOf(indexFiles),
                startTime, time, numberOfFiles, totalSize);
    }

    /**
     * Returns true if this snapshot contains a file with a given original name
     *
     * @param physicalName original file name
     * @return true if the file was found, false otherwise
     */
    public boolean containPhysicalIndexFile(String physicalName) {
        return findPhysicalIndexFile(physicalName) != null;
    }

    public FileInfo findPhysicalIndexFile(String physicalName) {
        for (FileInfo file : indexFiles) {
            if (file.physicalName().equals(physicalName)) {
                return file;
            }
        }
        return null;
    }

    /**
     * Returns true if this snapshot contains a file with a given name
     *
     * @param name file name
     * @return true if file was found, false otherwise
     */
    public FileInfo findNameFile(String name) {
        for (FileInfo file : indexFiles) {
            if (file.name().equals(name)) {
                return file;
            }
        }
        return null;
    }

}
