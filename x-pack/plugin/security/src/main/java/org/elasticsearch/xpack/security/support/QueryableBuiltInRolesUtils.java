/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

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
        try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
            roleDescriptor.toXContent(jsonBuilder, EMPTY_PARAMS);
            final Map<String, Object> flattenMap = Maps.flatten(
                XContentHelper.convertToMap(BytesReference.bytes(jsonBuilder), true, XContentType.JSON).v2(),
                false,
                true
            );
            hash.update(flattenMap.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("failed to compute digest for [" + roleDescriptor.getName() + "] role", e);
        }
        // HEX vs Base64 encoding is a trade-off between readability and space efficiency
        // opting for Base64 here to reduce the size of the cluster state
        return Base64.getEncoder().encodeToString(hash.digest());
    }

    private QueryableBuiltInRolesUtils() {
        throw new IllegalAccessError("not allowed");
    }
}
