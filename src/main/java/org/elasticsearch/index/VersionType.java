/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.index;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.lucene.uid.Versions;

/**
 *
 */
public enum VersionType {
    INTERNAL((byte) 0) {
        /**
         * - always returns false if currentVersion == {@link Versions#NOT_SET}
         * - always accepts expectedVersion == {@link Versions#MATCH_ANY}
         * - if expectedVersion is set, always conflict if currentVersion == {@link Versions#NOT_FOUND}
         */
        @Override
        public boolean isVersionConflict(long currentVersion, long expectedVersion) {
            return currentVersion != Versions.NOT_SET && expectedVersion != Versions.MATCH_ANY
                    && (currentVersion == Versions.NOT_FOUND || currentVersion != expectedVersion);
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return (currentVersion == Versions.NOT_SET || currentVersion == Versions.NOT_FOUND) ? 1 : currentVersion + 1;
        }

    },
    EXTERNAL((byte) 1) {
        /**
         * - always returns false if currentVersion == {@link Versions#NOT_SET}
         * - always conflict if expectedVersion == {@link Versions#MATCH_ANY} (we need something to set)
         * - accepts currentVersion == {@link Versions#NOT_FOUND}
         */
        @Override
        public boolean isVersionConflict(long currentVersion, long expectedVersion) {
            return currentVersion != Versions.NOT_SET && currentVersion != Versions.NOT_FOUND
                    && (expectedVersion == Versions.MATCH_ANY || currentVersion >= expectedVersion);
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return expectedVersion;
        }
    };

    private final byte value;

    VersionType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    /**
     * Checks whether the current version conflicts with the expected version, based on the current version type.
     *
     * @return true if versions conflict false o.w.
     */
    public abstract boolean isVersionConflict(long currentVersion, long expectedVersion);

    /**
     * Returns the new version for a document, based on it's current one and the specified in the request
     *
     * @return new version
     */
    public abstract long updateVersion(long currentVersion, long expectedVersion);

    public static VersionType fromString(String versionType) {
        if ("internal".equals(versionType)) {
            return INTERNAL;
        } else if ("external".equals(versionType)) {
            return EXTERNAL;
        }
        throw new ElasticSearchIllegalArgumentException("No version type match [" + versionType + "]");
    }

    public static VersionType fromString(String versionType, VersionType defaultVersionType) {
        if (versionType == null) {
            return defaultVersionType;
        }
        if ("internal".equals(versionType)) {
            return INTERNAL;
        } else if ("external".equals(versionType)) {
            return EXTERNAL;
        }
        throw new ElasticSearchIllegalArgumentException("No version type match [" + versionType + "]");
    }

    public static VersionType fromValue(byte value) {
        if (value == 0) {
            return INTERNAL;
        } else if (value == 1) {
            return EXTERNAL;
        }
        throw new ElasticSearchIllegalArgumentException("No version type match [" + value + "]");
    }
}