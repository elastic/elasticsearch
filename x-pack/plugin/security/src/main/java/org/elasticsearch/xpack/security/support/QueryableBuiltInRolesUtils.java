/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Utility class which provides helper method for calculating the hash of a role descriptor.
 */
public final class QueryableBuiltInRolesUtils {

    /**
     * Calculates the hash of the given role descriptor by serializing it by calling {@link RoleDescriptor#writeTo(StreamOutput)} method
     * and then SHA256 hashing the bytes.
     *
     * @param roleDescriptor the role descriptor to hash
     * @return the base64 encoded SHA256 hash of the role descriptor
     */
    public static String calculateHash(final RoleDescriptor roleDescriptor) {
        final MessageDigest hash = MessageDigests.sha256();
        try (BytesStreamOutput out = new BytesStreamOutput();) {
            try {
                roleDescriptor.writeTo(out);
            } catch (IOException e) {
                throw new IllegalStateException("failed to serialize role descriptor", e);
            }
            hash.update(BytesReference.toBytes(out.bytes()));
        }
        // HEX vs Base64 encoding is a trade-off between readability and space efficiency
        // opting for Base64 here to reduce the size of the cluster state
        return Base64.getEncoder().encodeToString(hash.digest());
    }

    private QueryableBuiltInRolesUtils() {
        throw new IllegalAccessError("not allowed");
    }
}
