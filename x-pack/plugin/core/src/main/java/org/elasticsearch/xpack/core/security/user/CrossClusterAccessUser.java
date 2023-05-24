/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

public class CrossClusterAccessUser extends InternalUser {

    private static final RoleDescriptor REMOTE_ACCESS_ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.CROSS_CLUSTER_ACCESS_ROLE,
        new String[] { "cross_cluster_search", "cross_cluster_replication" },
        // Needed for CCR background jobs (with system user)
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("*")
                .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
                .allowRestrictedIndices(true)
                .build() },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    /**
     * Package protected to enforce a singleton (private constructor) - use {@link InternalUsers#CROSS_CLUSTER_ACCESS_USER} instead
     */
    static final InternalUser INSTANCE = new CrossClusterAccessUser();

    private CrossClusterAccessUser() {
        super(
            UsernamesField.CROSS_CLUSTER_ACCESS_NAME,
            /**
             *  this user is not permitted to execute actions that originate on the local cluster,
             *  its only purpose is to execute actions from a remote cluster
             */
            Optional.empty(),
            Optional.of(REMOTE_ACCESS_ROLE_DESCRIPTOR)
        );
    }

    /**
     * The role descriptor intersection in the returned subject info is always empty. Because the privileges of the cross cluster access
     * internal user are static, we set them during role reference resolution instead of needlessly deserializing the role descriptor
     * intersection (see flow starting at {@link Subject#getRoleReferenceIntersection(AnonymousUser)})
     */
    public static CrossClusterAccessSubjectInfo subjectInfo(TransportVersion transportVersion, String nodeName) {
        try {
            return new CrossClusterAccessSubjectInfo(
                Authentication.newInternalAuthentication(InternalUsers.CROSS_CLUSTER_ACCESS_USER, transportVersion, nodeName),
                RoleDescriptorsIntersection.EMPTY
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
