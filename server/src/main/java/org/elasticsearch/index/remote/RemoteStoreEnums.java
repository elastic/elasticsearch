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
import org.elasticsearch.common.hash.FNV1a;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

/**
 * This class contains the different enums related to remote store like data categories and types, path types
 * and hashing algorithm.
 *
 * @opensearch.api
 */
public class RemoteStoreEnums {

    /**
     * Categories of the data in Remote store.
     */
    @PublicApi(since = "2.14.0")
    public enum DataCategory {
        SEGMENTS("segments", Set.of(DataType.values())),
        TRANSLOG("translog", Set.of(DataType.DATA, DataType.METADATA));

        private final String name;
        private final Set<DataType> supportedDataTypes;

        DataCategory(String name, Set<DataType> supportedDataTypes) {
            this.name = name;
            this.supportedDataTypes = supportedDataTypes;
        }

        public boolean isSupportedDataType(DataType dataType) {
            return supportedDataTypes.contains(dataType);
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Types of data in remote store.
     */
    @PublicApi(since = "2.14.0")
    public enum DataType {
        DATA("data"),
        METADATA("metadata"),
        LOCK_FILES("lock_files");

        private final String name;

        DataType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Enumerates the types of remote store paths resolution techniques supported by OpenSearch.
     * For more information, see <a href="https://github.com/opensearch-project/OpenSearch/issues/12567">Github issue #12567</a>.
     */
    @PublicApi(since = "2.14.0")
    public enum PathType {
        FIXED(0) {
            @Override
            public BlobPath generatePath(RemoteStorePathStrategy.PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
                assert Objects.isNull(hashAlgorithm) : "hashAlgorithm is expected to be null with fixed remote store path type";
                // Hash algorithm is not used in FIXED path type
                return pathInput.basePath().add(String.valueOf(pathInput.fixedSubPath()));
            }


            @Override
            public boolean requiresHashAlgorithm() {
                return false;
            }
        },
        HASHED_PREFIX(1) {
            @Override
            public BlobPath generatePath(RemoteStorePathStrategy.PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
                assert Objects.nonNull(hashAlgorithm) : "hashAlgorithm is expected to be non-null";
                return BlobPath.cleanPath().add(hashAlgorithm.hash(pathInput)).add(pathInput.basePath()).add(pathInput.fixedSubPath());
            }

            @Override
            public boolean requiresHashAlgorithm() {
                return true;
            }
        },
        HASHED_INFIX(2) {
            @Override
            protected BlobPath generatePath(RemoteStorePathStrategy.PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
                assert Objects.nonNull(hashAlgorithm) : "hashAlgorithm is expected to be non-null";
                return pathInput.basePath().add(hashAlgorithm.hash(pathInput)).add(String.valueOf(pathInput.fixedSubPath()));
            }

            @Override
            public boolean requiresHashAlgorithm() {
                return true;
            }
        };

        private final int code;

        PathType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        private static final Map<Integer, PathType> CODE_TO_ENUM;

        static {
            PathType[] values = values();
            Map<Integer, PathType> codeToStatus = new HashMap<>(values.length);
            for (PathType value : values) {
                int code = value.code;
                if (codeToStatus.containsKey(code)) {
                    throw new IllegalStateException(
                        new ParameterizedMessage("{} has same code as {}", codeToStatus.get(code), value).getFormattedMessage()
                    );
                }
                codeToStatus.put(code, value);
            }
            CODE_TO_ENUM = unmodifiableMap(codeToStatus);
        }

        /**
         * Turn a status code into a {@link PathType}.
         */
        public static PathType fromCode(int code) {
            return CODE_TO_ENUM.get(code);
        }

        /**
         * This method generates the path for the given path input which constitutes multiple fields and characteristics
         * of the data.
         *
         * @param pathInput     input.
         * @param hashAlgorithm hashing algorithm.
         * @return the blob path for the path input.
         */
        public BlobPath path(RemoteStorePathStrategy.PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
            pathInput.assertIsValid();
            return generatePath(pathInput, hashAlgorithm);
        }

        protected abstract BlobPath generatePath(RemoteStorePathStrategy.PathInput pathInput, PathHashAlgorithm hashAlgorithm);

        public abstract BlobPath generatePath(RemoteStorePathStrategy.PathInput pathInput, PathHashAlgorithm hashAlgorithm);

        public abstract boolean requiresHashAlgorithm();

        public static PathType parseString(String pathType) {
            try {
                return PathType.valueOf(pathType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                // IllegalArgumentException is thrown when the input does not match any enum name
                // NullPointerException is thrown when the input is null
                throw new IllegalArgumentException("Could not parse PathType for [" + pathType + "]");
            }
        }

        /**
         * This string is used as key for storing information in the custom data in index settings.
         */
        public static final String NAME = "path_type";

    }

    /**
     * Type of hashes supported for path types that have hashing.
     */
    @PublicApi(since = "2.14.0")
    public enum PathHashAlgorithm {

        FNV_1A_BASE64(0) {
            @Override
            String hash(RemoteStorePathStrategy.PathInput pathInput) {
                StringBuilder input = new StringBuilder();
                for (String path : pathInput.fixedSubPath().toArray()) {
                    input.append(path);
                }
                long hash = FNV1a.hash64(input.toString());
                return longToUrlBase64(hash);
            }

        },
        /**
         * This hash algorithm will generate a hash value which will use 1st 6 bits to create bas64 character and next 14
         * bits to create binary string.
         */
        FNV_1A_COMPOSITE_1(1) {
            @Override
            String hash(RemoteStorePathStrategy.PathInput pathInput) {
                StringBuilder input = new StringBuilder();
                for (String path : pathInput.fixedSubPath().toArray()) {
                    input.append(path);
                }
                long hash = FNV1a.hash64(input.toString());
                return longToCompositeBase64AndBinaryEncoding(hash, 20);
            }
        };

        private final int code;

        PathHashAlgorithm(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        private static final Map<Integer, PathHashAlgorithm> CODE_TO_ENUM;

        static {
            PathHashAlgorithm[] values = values();
            Map<Integer, PathHashAlgorithm> codeToStatus = new HashMap<>(values.length);
            for (PathHashAlgorithm value : values) {
                int code = value.code;
                if (codeToStatus.containsKey(code)) {
                    throw new IllegalStateException(
                        new ParameterizedMessage("{} has same code as {}", codeToStatus.get(code), value).getFormattedMessage()
                    );
                }
                codeToStatus.put(code, value);
            }
            CODE_TO_ENUM = unmodifiableMap(codeToStatus);
        }

        /**
         * Turn a status code into a {@link PathHashAlgorithm}.
         */
        public static PathHashAlgorithm fromCode(int code) {
            return CODE_TO_ENUM.get(code);
        }

        abstract String hash(RemoteStorePathStrategy.PathInput pathInput);

        public static PathHashAlgorithm parseString(String pathHashAlgorithm) {
            try {
                return PathHashAlgorithm.valueOf(pathHashAlgorithm.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                // IllegalArgumentException is thrown when the input does not match any enum name
                // NullPointerException is thrown when the input is null
                throw new IllegalArgumentException("Could not parse PathHashAlgorithm for [" + pathHashAlgorithm + "]");
            }
        }

        /**
         * This string is used as key for storing information in the custom data in index settings.
         */
        public static final String NAME = "path_hash_algorithm";
    }
}
