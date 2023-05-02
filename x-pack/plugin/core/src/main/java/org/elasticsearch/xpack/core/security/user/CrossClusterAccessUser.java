/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

public class CrossClusterAccessUser extends InternalUser {
    public static final String NAME = UsernamesField.CROSS_CLUSTER_ACCESS_NAME;

    public static final RoleDescriptor REMOTE_ACCESS_ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.CROSS_CLUSTER_ACCESS_ROLE,
        new String[] {
            "cross_cluster_access",
            // TODO: add a named cluster privilege to cover the CCR cluster actions
            ClusterStateAction.NAME },
        // Needed for CCR background jobs (with system user)
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("*")
                .privileges(
                    RetentionLeaseActions.Add.ACTION_NAME,
                    RetentionLeaseActions.Remove.ACTION_NAME,
                    RetentionLeaseActions.Renew.ACTION_NAME,
                    "indices:monitor/stats",
                    "indices:internal/admin/ccr/restore/session/put",
                    "indices:internal/admin/ccr/restore/session/clear",
                    "internal:transport/proxy/indices:internal/admin/ccr/restore/session/clear",
                    "indices:internal/admin/ccr/restore/file_chunk/get",
                    "internal:transport/proxy/indices:internal/admin/ccr/restore/file_chunk/get"
                )
                .allowRestrictedIndices(true)
                .build() },
        null,
        null,
        null,
        null,
        null,
        null
    );

    public static final InternalUser INSTANCE = new CrossClusterAccessUser();

    private CrossClusterAccessUser() {
        super(NAME);
    }

    /**
     * @return {@link Optional#empty()} because this user is not permitted to execute actions that originate on the local cluster,
     * its only purpose is to execute actions from a remote cluster
     * @see #getRemoteAccessRole()
     */
    @Override
    public Optional<RoleDescriptor> getLocalClusterRole() {
        // This user has no role for actions that originate from the current cluster
        // It does, however, have a descriptor for the role to be applied when actions originate from another cluster
        return Optional.empty();
    }

    @Override
    public Optional<RoleDescriptor> getRemoteAccessRole() {
        return Optional.of(REMOTE_ACCESS_ROLE_DESCRIPTOR);
    }

    public static boolean is(User user) {
        return INSTANCE.equals(user);
    }

    /**
     * The role descriptor intersection in the returned subject info is always empty. Because the privileges of the cross cluster access
     * internal user are static, we set them during role reference resolution instead of needlessly deserializing the role descriptor
     * intersection (see flow starting at {@link Subject#getRoleReferenceIntersection(AnonymousUser)})
     */
    public static CrossClusterAccessSubjectInfo subjectInfo(TransportVersion transportVersion, String nodeName) {
        try {
            return new CrossClusterAccessSubjectInfo(
                Authentication.newInternalAuthentication(INSTANCE, transportVersion, nodeName),
                RoleDescriptorsIntersection.EMPTY
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
