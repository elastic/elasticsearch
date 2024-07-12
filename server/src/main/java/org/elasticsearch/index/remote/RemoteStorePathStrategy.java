/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.remote;


import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * This class wraps internal details on the remote store path for an index.
 *
 * @opensearch.internal
 */
@SuppressWarnings("checkstyle:OuterTypeFilename")
@PublicApi(since = "2.14.0")
public class RemoteStorePathStrategy {

    private final RemoteStoreEnums.PathType type;

    @Nullable
    private final RemoteStoreEnums.PathHashAlgorithm hashAlgorithm;

    public RemoteStorePathStrategy(RemoteStoreEnums.PathType type) {
        this(type, null);
    }

    public RemoteStorePathStrategy(RemoteStoreEnums.PathType type, RemoteStoreEnums.PathHashAlgorithm hashAlgorithm) {
        Objects.requireNonNull(type, "pathType can not be null");
        if (isCompatible(type, hashAlgorithm) == false) {
            throw new IllegalArgumentException(
                new ParameterizedMessage("pathType={} pathHashAlgorithm={} are incompatible", type, hashAlgorithm).getFormattedMessage()
            );
        }
        this.type = type;
        this.hashAlgorithm = hashAlgorithm;
    }

    public static boolean isCompatible(RemoteStoreEnums.PathType type, RemoteStoreEnums.PathHashAlgorithm hashAlgorithm) {
        return (type.requiresHashAlgorithm() == false && Objects.isNull(hashAlgorithm))
            || (type.requiresHashAlgorithm() && Objects.nonNull(hashAlgorithm));
    }

    public RemoteStoreEnums.PathType getType() {
        return type;
    }

    public RemoteStoreEnums.PathHashAlgorithm getHashAlgorithm() {
        return hashAlgorithm;
    }

    @Override
    public String toString() {
        return "RemoteStorePathStrategy{" + "type=" + type + ", hashAlgorithm=" + hashAlgorithm + '}';
    }

    public BlobPath generatePath(PathInput pathInput) {
        return type.path(pathInput, hashAlgorithm);
    }

    /**
     * Wrapper class for the path input required to generate path for remote store uploads. This input is composed of
     * basePath and indexUUID.
     *
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    public static class PathInput {
        private final BlobPath basePath;
        private final String indexUUID;

        public PathInput(Builder<?> builder) {
            this.basePath = Objects.requireNonNull(builder.basePath);
            this.indexUUID = Objects.requireNonNull(builder.indexUUID);
        }

        BlobPath basePath() {
            return basePath;
        }

        String indexUUID() {
            return indexUUID;
        }

        BlobPath fixedSubPath() {
            return BlobPath.cleanPath().add(indexUUID);
        }

        /**
         * Returns a new builder for {@link PathInput}.
         */
        public static Builder<?> builder() {
            return new Builder<>();
        }

        public void assertIsValid() {
            // Input is always valid here.
        }

        /**
         * Builder for {@link PathInput}.
         *
         * @opensearch.internal
         */
        @PublicApi(since = "2.14.0")
        public static class Builder<T extends Builder<T>> {
            private BlobPath basePath;
            private String indexUUID;

            public T basePath(BlobPath basePath) {
                this.basePath = basePath;
                return self();
            }

            public Builder indexUUID(String indexUUID) {
                this.indexUUID = indexUUID;
                return self();
            }

            protected T self() {
                return (T) this;
            }

            public PathInput build() {
                return new PathInput(this);
            }
        }
    }

    /**
     * Wrapper class for the data aware path input required to generate path for remote store uploads. This input is
     * composed of the parent inputs, shard id, data category and data type.
     *
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    public static class ShardDataPathInput extends PathInput {
        private final String shardId;
        private final RemoteStoreEnums.DataCategory dataCategory;
        private final RemoteStoreEnums.DataType dataType;

        public ShardDataPathInput(Builder builder) {
            super(builder);
            this.shardId = Objects.requireNonNull(builder.shardId);
            this.dataCategory = Objects.requireNonNull(builder.dataCategory);
            this.dataType = Objects.requireNonNull(builder.dataType);
            assert dataCategory.isSupportedDataType(dataType) : "category:"
                + dataCategory
                + " type:"
                + dataType
                + " are not supported together";

        }

        String shardId() {
            return shardId;
        }

        RemoteStoreEnums.DataCategory dataCategory() {
            return dataCategory;
        }

        RemoteStoreEnums.DataType dataType() {
            return dataType;
        }

        @Override
        BlobPath fixedSubPath() {
            return super.fixedSubPath().add(shardId).add(dataCategory.getName()).add(dataType.getName());
        }

        /**
         * Returns a new builder for {@link ShardDataPathInput}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for {@link ShardDataPathInput}.
         *
         * @opensearch.internal
         */
        @PublicApi(since = "2.14.0")
        public static class Builder extends PathInput.Builder<Builder> {
            private String shardId;
            private RemoteStoreEnums.DataCategory dataCategory;
            private RemoteStoreEnums.DataType dataType;

            public Builder basePath(BlobPath basePath) {
                super.basePath = basePath;
                return this;
            }

            public Builder indexUUID(String indexUUID) {
                super.indexUUID = indexUUID;
                return this;
            }

            public Builder shardId(String shardId) {
                this.shardId = shardId;
                return this;
            }

            public Builder dataCategory(RemoteStoreEnums.DataCategory dataCategory) {
                this.dataCategory = dataCategory;
                return this;
            }

            public Builder dataType(RemoteStoreEnums.DataType dataType) {
                this.dataType = dataType;
                return this;
            }

            @Override
            protected Builder self() {
                return this;
            }

            public ShardDataPathInput build() {
                return new ShardDataPathInput(this);
            }
        }
    }

}
