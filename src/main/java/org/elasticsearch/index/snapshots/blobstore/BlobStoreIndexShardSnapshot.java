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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
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
        private final String physicalName;
        private final long length;
        private final String checksum;
        private final ByteSizeValue partSize;
        private final long partBytes;
        private final long numberOfParts;

        /**
         * Constructs a new instance of file info
         *
         * @param name         file name as stored in the blob store
         * @param physicalName original file name
         * @param length       total length of the file
         * @param partSize     size of the single chunk
         * @param checksum     checksum for the file
         */
        public FileInfo(String name, String physicalName, long length, ByteSizeValue partSize, String checksum) {
            this.name = name;
            this.physicalName = physicalName;
            this.length = length;
            this.checksum = checksum;

            long partBytes = Long.MAX_VALUE;
            if (partSize != null) {
                partBytes = partSize.bytes();
            }

            long totalLength = length;
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
            return this.physicalName;
        }

        /**
         * File length
         *
         * @return file length
         */
        public long length() {
            return length;
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
            return checksum;
        }

        /**
         * Checks if a file in a store is the same file
         *
         * @param md file in a store
         * @return true if file in a store this this file have the same checksum and length
         */
        public boolean isSame(StoreFileMetaData md) {
            if (checksum == null || md.checksum() == null) {
                return false;
            }
            return length == md.length() && checksum.equals(md.checksum());
        }

        static final class Fields {
            static final XContentBuilderString NAME = new XContentBuilderString("name");
            static final XContentBuilderString PHYSICAL_NAME = new XContentBuilderString("physical_name");
            static final XContentBuilderString LENGTH = new XContentBuilderString("length");
            static final XContentBuilderString CHECKSUM = new XContentBuilderString("checksum");
            static final XContentBuilderString PART_SIZE = new XContentBuilderString("part_size");
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
            builder.field(Fields.PHYSICAL_NAME, file.physicalName);
            builder.field(Fields.LENGTH, file.length);
            if (file.checksum != null) {
                builder.field(Fields.CHECKSUM, file.checksum);
            }
            if (file.partSize != null) {
                builder.field(Fields.PART_SIZE, file.partSize.bytes());
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
            return new FileInfo(name, physicalName, length, partSize, checksum);
        }

    }

    private final String snapshot;

    private final long indexVersion;

    private final ImmutableList<FileInfo> indexFiles;

    /**
     * Constructs new shard snapshot metadata from snapshot metadata
     *
     * @param snapshot     snapshot id
     * @param indexVersion index version
     * @param indexFiles   list of files in the shard
     */
    public BlobStoreIndexShardSnapshot(String snapshot, long indexVersion, List<FileInfo> indexFiles) {
        assert snapshot != null;
        assert indexVersion >= 0;
        this.snapshot = snapshot;
        this.indexVersion = indexVersion;
        this.indexFiles = ImmutableList.copyOf(indexFiles);
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
     * Serializes shard snapshot metadata info into JSON
     *
     * @param snapshot shard snapshot metadata
     * @param builder  XContent builder
     * @param params   parameters
     * @throws IOException
     */
    public static void toXContent(BlobStoreIndexShardSnapshot snapshot, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("name", snapshot.snapshot);
        builder.field("index-version", snapshot.indexVersion);
        builder.startArray("files");
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

        List<FileInfo> indexFiles = newArrayList();

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if ("name".equals(currentFieldName)) {
                            snapshot = parser.text();
                        } else if ("index-version".equals(currentFieldName)) {
                            indexVersion = parser.longValue();
                        } else {
                            throw new ElasticsearchParseException("unknown parameter [" + currentFieldName + "]");
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            indexFiles.add(FileInfo.fromXContent(parser));
                        }
                    } else {
                        throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected token  [" + token + "]");
                }
            }
        }
        return new BlobStoreIndexShardSnapshot(snapshot, indexVersion, ImmutableList.<FileInfo>copyOf(indexFiles));
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
