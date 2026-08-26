/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Strings;

import java.util.Optional;

/**
 * @param formatVersion Currently the only acceptable/existing {@code formatVersion} is {@code V1_FORMAT_VERSION}
 */
public record StatelessLease(int formatVersion, long currentTerm, long nodeLeftGeneration, long projectsUnderDeletionGeneration) {

    public static final int V1_FORMAT_VERSION = 1;
    private static final int V1_FORMAT_VERSION_LENGTH = Integer.BYTES + Long.BYTES * 3;
    public static final StatelessLease ZERO = new StatelessLease(0, 0, 0);

    public StatelessLease {
        assert formatVersion == V1_FORMAT_VERSION;
    }

    public StatelessLease(long currentTerm, long nodeLeftGeneration, long projectsUnderDeletionGeneration) {
        this(V1_FORMAT_VERSION, currentTerm, nodeLeftGeneration, projectsUnderDeletionGeneration);
    }

    public BytesReference asBytes() {
        if (currentTerm == 0) {
            return BytesArray.EMPTY;
        }
        final byte[] bytes;
        assert nodeLeftGeneration >= 0;
        assert projectsUnderDeletionGeneration >= 0;
        if (formatVersion == V1_FORMAT_VERSION) {
            bytes = new byte[V1_FORMAT_VERSION_LENGTH];
            ByteUtils.writeIntBE(formatVersion, bytes, 0);
            ByteUtils.writeLongBE(currentTerm, bytes, Integer.BYTES);
            ByteUtils.writeLongBE(nodeLeftGeneration, bytes, Integer.BYTES + Long.BYTES);
            ByteUtils.writeLongBE(projectsUnderDeletionGeneration, bytes, Integer.BYTES + Long.BYTES * 2);
        } else {
            throw new IllegalStateException("unknown format version: " + formatVersion);
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
        // Any lease format must have a format version at the start
        assert bytesReference.length() >= Integer.BYTES;
        int version = bytesReference.getInt(0);
        if (version == V1_FORMAT_VERSION && bytesReference.length() == V1_FORMAT_VERSION_LENGTH) {
            result = new StatelessLease(
                Long.reverseBytes(bytesReference.getLongLE(Integer.BYTES)),
                Long.reverseBytes(bytesReference.getLongLE(Integer.BYTES + Long.BYTES)),
                Long.reverseBytes(bytesReference.getLongLE(Integer.BYTES + 2 * Long.BYTES))
            );
        } else {
            throw new IllegalStateException(
                Strings.format("cannot read lease format version [%d] from byte arrays of length [%d]", version, bytesReference.length())
            );
        }
        return Optional.of(result);
    }

    // Intentionally not using a Comparator here as the comparison here is not consistent with the equals method.
    public static int compare(StatelessLease lease1, StatelessLease lease2) {
        int result = Long.compare(lease1.currentTerm, lease2.currentTerm);
        if (result == 0) {
            result = Long.compare(lease1.nodeLeftGeneration, lease2.nodeLeftGeneration);
        }
        if (result == 0) {
            result = Long.compare(lease1.projectsUnderDeletionGeneration, lease2.projectsUnderDeletionGeneration);
        }
        return result;
    }

    public static long getProjectsMarkedForDeletionGeneration(ClusterState clusterState) {
        return clusterState.custom(ProjectStateRegistry.TYPE, ProjectStateRegistry.EMPTY).getProjectsMarkedForDeletionGeneration();
    }
}
