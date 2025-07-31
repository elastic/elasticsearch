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

package co.elastic.elasticsearch.stateless.multiproject;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * A lease file for a project that must be acquired by the cluster hosting that project.
 *
 * @param formatVersion lease format version
 * @param leaseVersion the lease version which is incremented each time the lease is acquired/released
 * @param clusterUuid the owning cluster's UUID encoded as bytes. {@code NIL_UUID} if unassigned.
 */
public record ProjectLease(int formatVersion, long leaseVersion, byte[] clusterUuid) {

    public static final int CURRENT_FORMAT_VERSION = 1;
    public static final int UUID_SIZE_IN_BYTES = 16;
    public static final int LEASE_SIZE_IN_BYTES = Integer.BYTES + Long.BYTES + UUID_SIZE_IN_BYTES;
    public static final byte[] NIL_UUID = new byte[UUID_SIZE_IN_BYTES];
    public static final ProjectLease EMPTY_LEASE = new ProjectLease(CURRENT_FORMAT_VERSION, 0L, NIL_UUID);

    public BytesReference asBytes() {
        if (this.equals(EMPTY_LEASE)) {
            return BytesArray.EMPTY;
        }
        var bytes = new byte[LEASE_SIZE_IN_BYTES];
        ByteUtils.writeIntBE(formatVersion, bytes, 0);
        ByteUtils.writeLongBE(leaseVersion, bytes, Integer.BYTES);
        System.arraycopy(clusterUuid, 0, bytes, Integer.BYTES + Long.BYTES, UUID_SIZE_IN_BYTES);
        return new BytesArray(bytes);
    }

    public static ProjectLease fromBytes(BytesReference bytesReference) throws IOException {
        if (bytesReference.length() == 0) {
            return ProjectLease.EMPTY_LEASE;
        }
        try (StreamInput input = bytesReference.streamInput()) {
            return new ProjectLease(input.readInt(), input.readLong(), input.readNBytes(UUID_SIZE_IN_BYTES));
        }
    }

    public boolean isAssigned() {
        return Arrays.equals(clusterUuid, NIL_UUID) == false;
    }

    public String clusterUuidAsString() {
        return uuidAsString(clusterUuid);
    }

    static String uuidAsString(byte[] uuidBytes) {
        if (Arrays.equals(uuidBytes, NIL_UUID)) {
            return "unassigned";
        }
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(uuidBytes);
    }

    public static String leaseBlobName(ProjectId projectId) {
        return "project-" + projectId + "_lease";
    }

    @Override
    public String toString() {
        return "ProjectLease{"
            + "formatVersion="
            + formatVersion
            + ", leaseVersion="
            + leaseVersion
            + ", clusterUuid="
            + clusterUuidAsString()
            + '}';
    }
}
