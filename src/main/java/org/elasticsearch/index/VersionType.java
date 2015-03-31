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
package org.elasticsearch.index;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lucene.uid.Versions;

/**
 *
 */
public enum VersionType {
    INTERNAL((byte) 0) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion) {
            return isVersionConflict(currentVersion, expectedVersion);
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            return isVersionConflict(currentVersion, expectedVersion);
        }

        private boolean isVersionConflict(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_SET) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return true;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return (currentVersion == Versions.NOT_SET || currentVersion == Versions.NOT_FOUND) ? 1 : currentVersion + 1;
        }

        @Override
        public boolean validateVersionForWrites(long version) {
            // not allowing Versions.NOT_FOUND as it is not a valid input value.
            return version > 0L || version == Versions.MATCH_ANY;
        }

        @Override
        public boolean validateVersionForReads(long version) {
            // not allowing Versions.NOT_FOUND as it is not a valid input value.
            return version > 0L || version == Versions.MATCH_ANY;
        }

        @Override
        public VersionType versionTypeForReplicationAndRecovery() {
            // replicas get the version from the primary after increment. The same version is stored in
            // the transaction log. -> the should use the external semantics.
            return EXTERNAL;
        }
    },
    EXTERNAL((byte) 1) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_SET) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return true;
            }
            if (currentVersion >= expectedVersion) {
                return true;
            }
            return false;
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_SET) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return true;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return expectedVersion;
        }

        @Override
        public boolean validateVersionForWrites(long version) {
            return version >= 0L;
        }

        @Override
        public boolean validateVersionForReads(long version) {
            return version >= 0L || version == Versions.MATCH_ANY;
        }

    },
    EXTERNAL_GTE((byte) 2) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_SET) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return true;
            }
            if (currentVersion > expectedVersion) {
                return true;
            }
            return false;
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_SET) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return true;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return expectedVersion;
        }

        @Override
        public boolean validateVersionForWrites(long version) {
            return version >= 0L;
        }

        @Override
        public boolean validateVersionForReads(long version) {
            return version >= 0L || version == Versions.MATCH_ANY;
        }

    },
    /**
     * Warning: this version type should be used with care. Concurrent indexing may result in loss of data on replicas
     */
    FORCE((byte) 3) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_SET) {
                return false;
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_ANY) {
                return true;
            }
            return false;
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            return false;
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return expectedVersion;
        }

        @Override
        public boolean validateVersionForWrites(long version) {
            return version >= 0L;
        }

        @Override
        public boolean validateVersionForReads(long version) {
            return version >= 0L || version == Versions.MATCH_ANY;
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
    public abstract boolean isVersionConflictForWrites(long currentVersion, long expectedVersion);

    /**
     * Checks whether the current version conflicts with the expected version, based on the current version type.
     *
     * @return true if versions conflict false o.w.
     */
    public abstract boolean isVersionConflictForReads(long currentVersion, long expectedVersion);

    /**
     * Returns the new version for a document, based on its current one and the specified in the request
     *
     * @return new version
     */
    public abstract long updateVersion(long currentVersion, long expectedVersion);

    /**
     * validate the version is a valid value for this type when writing.
     *
     * @return true if valid, false o.w
     */
    public abstract boolean validateVersionForWrites(long version);

    /**
     * validate the version is a valid value for this type when reading.
     *
     * @return true if valid, false o.w
     */
    public abstract boolean validateVersionForReads(long version);

    /**
     * Some version types require different semantics for primary and replicas. This version allows
     * the type to override the default behavior.
     */
    public VersionType versionTypeForReplicationAndRecovery() {
        return this;
    }

    public static VersionType fromString(String versionType) {
        if ("internal".equals(versionType)) {
            return INTERNAL;
        } else if ("external".equals(versionType)) {
            return EXTERNAL;
        } else if ("external_gt".equals(versionType)) {
            return EXTERNAL;
        } else if ("external_gte".equals(versionType)) {
            return EXTERNAL_GTE;
        } else if ("force".equals(versionType)) {
            return FORCE;
        }
        throw new ElasticsearchIllegalArgumentException("No version type match [" + versionType + "]");
    }

    public static VersionType fromString(String versionType, VersionType defaultVersionType) {
        if (versionType == null) {
            return defaultVersionType;
        }
        return fromString(versionType);
    }

    public static VersionType fromValue(byte value) {
        if (value == 0) {
            return INTERNAL;
        } else if (value == 1) {
            return EXTERNAL;
        } else if (value == 2) {
            return EXTERNAL_GTE;
        } else if (value == 3) {
            return FORCE;
        }
        throw new ElasticsearchIllegalArgumentException("No version type match [" + value + "]");
    }
}
