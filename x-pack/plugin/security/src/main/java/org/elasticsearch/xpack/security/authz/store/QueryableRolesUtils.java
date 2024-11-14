/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collection;

import static java.util.Comparator.comparing;

public final class QueryableRolesUtils {

    public static String calculateHash(final Collection<RoleDescriptor> roleDescriptors) {
        final MessageDigest hash = MessageDigests.sha256();
        try (BytesStreamOutput out = new BytesStreamOutput();) {
            roleDescriptors.stream().sorted(comparing(RoleDescriptor::getName)).forEach(role -> {
                try {
                    role.writeTo(out);
                } catch (IOException e) {
                    throw new RuntimeException("failed to serialize role", e);
                }
            });
            hash.update(BytesReference.toBytes(out.bytes()));
        }
        // HEX vs Base64 encoding is a trade-off between readability and space efficiency
        // opting for Base64 here to reduce the size of the cluster state
        return Base64.getEncoder().encodeToString(hash.digest());
    }

}
