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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Utility class which provides helper method for calculating the hash of a role descriptor,
 * determining the roles to upsert and the roles to delete.
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

    /**
     * Determines the roles to delete by comparing the indexed roles with the roles in the built-in roles.
     * @return the set of roles to delete
     */
    public static Set<String> determineRolesToDelete(final QueryableBuiltInRoles roles, final Map<String, String> indexedRolesDigests) {
        assert roles != null;
        if (indexedRolesDigests == null) {
            // nothing indexed, nothing to delete
            return Set.of();
        }
        final Set<String> rolesToDelete = Sets.difference(indexedRolesDigests.keySet(), roles.rolesDigest().keySet());
        return Collections.unmodifiableSet(rolesToDelete);
    }

    /**
     * Determines the roles to upsert by comparing the indexed roles and their digests with the current built-in roles.
     * @return the set of roles to upsert (create or update)
     */
    public static Set<RoleDescriptor> determineRolesToUpsert(
        final QueryableBuiltInRoles roles,
        final Map<String, String> indexedRolesDigests
    ) {
        assert roles != null;
        final Set<RoleDescriptor> rolesToUpsert = new HashSet<>();
        for (RoleDescriptor role : roles.roleDescriptors()) {
            final String roleDigest = roles.rolesDigest().get(role.getName());
            if (indexedRolesDigests == null || indexedRolesDigests.containsKey(role.getName()) == false) {
                rolesToUpsert.add(role);  // a new role to create
            } else if (indexedRolesDigests.get(role.getName()).equals(roleDigest) == false) {
                rolesToUpsert.add(role);  // an existing role that needs to be updated
            }
        }
        return Collections.unmodifiableSet(rolesToUpsert);
    }

    private QueryableBuiltInRolesUtils() {
        throw new IllegalAccessError("not allowed");
    }
}
