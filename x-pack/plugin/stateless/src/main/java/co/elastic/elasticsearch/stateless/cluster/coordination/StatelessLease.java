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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Strings;

import java.util.Optional;

public record StatelessLease(int formatVersion, long currentTerm, long nodeLeftGeneration, long projectsUnderDeletedGeneration) {

    public static final int LEGACY_FORMAT_VERSION = 0;
    public static final int LEGACY_FORMAT_VERSION_LENGTH = 2 * Long.BYTES;
    public static final int V1_FORMAT_VERSION = 1;
    private static final int V1_FORMAT_VERSION_LENGTH = Integer.BYTES + Long.BYTES * 3;
    public static final StatelessLease ZERO = new StatelessLease(V1_FORMAT_VERSION, 0, 0, 0);

    public StatelessLease(long currentTerm, long nodeLeftGeneration, long projectsUnderDeletedGeneration) {
        this(V1_FORMAT_VERSION, currentTerm, nodeLeftGeneration, projectsUnderDeletedGeneration);
    }

    public BytesReference asBytes() {
        if (currentTerm == 0) {
            return BytesArray.EMPTY;
        }
        final byte[] bytes;
        assert nodeLeftGeneration >= 0;
        if (formatVersion == LEGACY_FORMAT_VERSION) {
            assert projectsUnderDeletedGeneration == 0 : "projectsUnderDeletedGeneration should be 0 for legacy format";
            bytes = new byte[LEGACY_FORMAT_VERSION_LENGTH];
            ByteUtils.writeLongBE(currentTerm, bytes, 0);
            ByteUtils.writeLongBE(nodeLeftGeneration, bytes, Long.BYTES);
        } else if (formatVersion == V1_FORMAT_VERSION) {
            bytes = new byte[V1_FORMAT_VERSION_LENGTH];
            ByteUtils.writeIntBE(formatVersion, bytes, 0);
            ByteUtils.writeLongBE(currentTerm, bytes, Integer.BYTES);
            ByteUtils.writeLongBE(nodeLeftGeneration, bytes, Integer.BYTES + Long.BYTES);
            ByteUtils.writeLongBE(projectsUnderDeletedGeneration, bytes, Integer.BYTES + Long.BYTES * 2);
        } else {
            throw new IllegalArgumentException("unknown format version: " + formatVersion);
        }
        return new BytesArray(bytes);
    }

    public static Optional<StatelessLease> fromBytes(OptionalBytesReference optionalBytesReference) {
        if (optionalBytesReference.isPresent() == false) {
            return Optional.empty();
        }
        StatelessLease result;
        BytesReference bytesReference = optionalBytesReference.bytesReference();
        if (bytesReference.length() == 0) {
            return Optional.of(StatelessLease.ZERO);
        }
        assert bytesReference.length() >= LEGACY_FORMAT_VERSION_LENGTH;
        // Legacy format is detected based on the length of the byte array
        if (bytesReference.length() == LEGACY_FORMAT_VERSION_LENGTH) {
            result = new StatelessLease(
                LEGACY_FORMAT_VERSION,
                Long.reverseBytes(bytesReference.getLongLE(0)),
                Long.reverseBytes(bytesReference.getLongLE(Long.BYTES)),
                0L
            );
        } else {
            // Any new format must have a format version at the start
            int version = Integer.reverseBytes(bytesReference.getIntLE(0));
            if (version == V1_FORMAT_VERSION && bytesReference.length() == V1_FORMAT_VERSION_LENGTH) {
                result = new StatelessLease(
                    version,
                    Long.reverseBytes(bytesReference.getLongLE(Integer.BYTES)),
                    Long.reverseBytes(bytesReference.getLongLE(Integer.BYTES + Long.BYTES)),
                    Long.reverseBytes(bytesReference.getLongLE(Integer.BYTES + 2 * Long.BYTES))
                );
            } else {
                throw new IllegalArgumentException(
                    Strings.format(
                        "cannot read lease format version [%d] from byte arrays of length [%d]",
                        version,
                        bytesReference.length()
                    )
                );
            }
        }
        return Optional.of(result);
    }

    // Intentionally not using a Comparator here as the comparison here is not consistent with the equals method.
    public static int compare(StatelessLease lease1, StatelessLease lease2) {
        int result = Long.compare(lease1.currentTerm, lease2.currentTerm);
        if (result == 0) {
            result = Long.compare(lease1.nodeLeftGeneration, lease2.nodeLeftGeneration);
        }
        if (result == 0 && lease1.formatVersion == V1_FORMAT_VERSION && lease2.formatVersion == V1_FORMAT_VERSION) {
            result = Long.compare(lease1.projectsUnderDeletedGeneration, lease2.projectsUnderDeletedGeneration);
        }
        return result;
    }

    public static long getProjectsMarkedForDeletionGeneration(ClusterState clusterState) {
        return clusterState.custom(ProjectStateRegistry.TYPE, ProjectStateRegistry.EMPTY).getProjectsMarkedForDeletionGeneration();
    }
}
