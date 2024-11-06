/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class QueryableReservedRolesProvider implements QueryableRolesProvider {

    private final Supplier<QueryableRoles> roles;

    public QueryableReservedRolesProvider(final ReservedRolesStore reservedRolesStore) {
        this.roles = CachedSupplier.wrap(() -> {
            final Map<String, RoleDescriptor> roleDescriptors = ReservedRolesStore.roleDescriptorsMap();
            final String version = calculateHash(roleDescriptors.values()).get();
            final Map<String, String> rolesVersions = roleDescriptors.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> version));
            return new QueryableRoles(rolesVersions, roleDescriptors);
        });
    }

    @Override
    public QueryableRoles roles() {
        return roles.get();
    }

    private Supplier<String> calculateHash(final Collection<RoleDescriptor> roleDescriptors) {
        return CachedSupplier.wrap(() -> {
            final MessageDigest hash = MessageDigests.sha256();
            try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
                // sorting the roles by name to ensure we generate a consistent hash version
                roleDescriptors.stream().sorted(Comparator.comparing(RoleDescriptor::getName)).forEach(role -> {
                    try {
                        role.toXContent(jsonBuilder, EMPTY_PARAMS);
                    } catch (IOException e) {
                        throw new RuntimeException("failed to ", e);
                    }
                });
                hash.update(BytesReference.bytes(jsonBuilder).array());
            } catch (IOException e) {
                throw new RuntimeException("failed to compute queryable roles version", e);
            }

            // HEX vs Base64 encoding is a trade-off between readability and space efficiency
            // opting for Base64 here to reduce the size of the cluster state
            return Base64.getEncoder().encodeToString(hash.digest());
        });
    }

}
