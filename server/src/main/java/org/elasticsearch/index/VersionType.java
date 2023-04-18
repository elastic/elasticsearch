/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;
import java.util.Locale;

public enum VersionType implements Writeable {
    INTERNAL((byte) 0) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            return isVersionConflict(currentVersion, expectedVersion, deleted);
        }

        @Override
        public String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            if (expectedVersion == Versions.MATCH_DELETED) {
                return "document already exists (current version [" + currentVersion + "])";
            }
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
            return isVersionConflict(currentVersion, expectedVersion, false);
        }

        @Override
        public String explainConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
        }

        private static boolean isVersionConflict(long currentVersion, long expectedVersion, boolean deleted) {
            if (expectedVersion == Versions.MATCH_ANY) {
                return false;
            }
            if (expectedVersion == Versions.MATCH_DELETED) {
                return deleted == false;
            }
            if (currentVersion != expectedVersion) {
                return true;
            }
            return false;
        }

        @Override
        public long updateVersion(long currentVersion, long expectedVersion) {
            return currentVersion == Versions.NOT_FOUND ? 1 : currentVersion + 1;
        }

        @Override
        public boolean validateVersionForWrites(long version) {
            return version > 0L || version == Versions.MATCH_ANY || version == Versions.MATCH_DELETED;
        }

        @Override
        public boolean validateVersionForReads(long version) {
            // not allowing Versions.NOT_FOUND as it is not a valid input value.
            return version > 0L || version == Versions.MATCH_ANY;
        }
    },
    EXTERNAL((byte) 1) {
        @Override
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
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
        public String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            return "current version [" + currentVersion + "] is higher or equal to the one provided [" + expectedVersion + "]";
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
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
        public String explainConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
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
        public boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
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
        public String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted) {
            return "current version [" + currentVersion + "] is higher than the one provided [" + expectedVersion + "]";
        }

        @Override
        public boolean isVersionConflictForReads(long currentVersion, long expectedVersion) {
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
        public String explainConflictForReads(long currentVersion, long expectedVersion) {
            if (currentVersion == Versions.NOT_FOUND) {
                return "document does not exist (expected version [" + expectedVersion + "])";
            }
            return "current version [" + currentVersion + "] is different than the one provided [" + expectedVersion + "]";
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
     * @param currentVersion  the current version for the document
     * @param expectedVersion the version specified for the write operation
     * @param deleted         true if the document is currently deleted (note that #currentVersion will typically be
     *                        {@link Versions#NOT_FOUND}, but may be something else if the document was recently deleted
     * @return true if versions conflict false o.w.
     */
    public abstract boolean isVersionConflictForWrites(long currentVersion, long expectedVersion, boolean deleted);

    /**
     * Returns a human readable explanation for a version conflict on write.
     *
     * Note that this method is only called if {@link #isVersionConflictForWrites(long, long, boolean)} returns true;
     *
     * @param currentVersion  the current version for the document
     * @param expectedVersion the version specified for the write operation
     * @param deleted         true if the document is currently deleted (note that #currentVersion will typically be
     *                        {@link Versions#NOT_FOUND}, but may be something else if the document was recently deleted
     */
    public abstract String explainConflictForWrites(long currentVersion, long expectedVersion, boolean deleted);

    /**
     * Checks whether the current version conflicts with the expected version, based on the current version type.
     *
     * @param currentVersion  the current version for the document
     * @param expectedVersion the version specified for the read operation
     * @return true if versions conflict false o.w.
     */
    public abstract boolean isVersionConflictForReads(long currentVersion, long expectedVersion);

    /**
     * Returns a human readable explanation for a version conflict on read.
     *
     * Note that this method is only called if {@link #isVersionConflictForReads(long, long)} returns true;
     *
     * @param currentVersion  the current version for the document
     * @param expectedVersion the version specified for the read operation
     */
    public abstract String explainConflictForReads(long currentVersion, long expectedVersion);

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

    public static VersionType fromString(String versionType) {
        if ("internal".equals(versionType)) {
            return INTERNAL;
        } else if ("external".equals(versionType)) {
            return EXTERNAL;
        } else if ("external_gt".equals(versionType)) {
            return EXTERNAL;
        } else if ("external_gte".equals(versionType)) {
            return EXTERNAL_GTE;
        }
        throw new IllegalArgumentException("No version type match [" + versionType + "]");
    }

    public static VersionType fromString(String versionType, VersionType defaultVersionType) {
        if (versionType == null) {
            return defaultVersionType;
        }
        return fromString(versionType);
    }

    public static String toString(VersionType versionType) {
        return versionType.name().toLowerCase(Locale.ROOT);
    }

    public static VersionType fromValue(byte value) {
        if (value == 0) {
            return INTERNAL;
        } else if (value == 1) {
            return EXTERNAL;
        } else if (value == 2) {
            return EXTERNAL_GTE;
        }
        throw new IllegalArgumentException("No version type match [" + value + "]");
    }

    public static VersionType readFromStream(StreamInput in) throws IOException {
        return in.readEnum(VersionType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }
}
