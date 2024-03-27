/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a group of permissions for a remote cluster. This is intended to be the model for both the {@link RoleDescriptor}
 * and {@link Role}. This model is not intended to be sent to a remote cluster, but can be (wire) serialized within a single cluster
 * as well as the Xcontent serialization for the REST API and persistence of the role in the security index. The privileges modeled here
 * will be converted to the appropriate cluster privileges when sent to a remote cluster.
 * For example, on the local/querying cluster this model represents the following:
 * <code>
 * "remote_cluster" : [
 *         {
 *             "privileges" : ["foo"],
 *             "clusters" : ["clusterA"]
 *         },
 *         {
 *             "privileges" : ["bar"],
 *             "clusters" : ["clusterB"]
 *         }
 *     ]
 * </code>
 * when sent to the remote cluster "clusterA", the privileges will be converted to the appropriate cluster privileges. For example:
 * <code>
 *   "cluster": ["foo"]
 * </code>
 * and when sent to the remote cluster "clusterB", the privileges will be converted to the appropriate cluster privileges. For example:
 * <code>
 *   "cluster": ["bar"]
 * </code>
 */
public class RemoteClusterPermissions implements Writeable, ToXContentObject {

    private final List<RemoteClusterPermissionGroup> remoteClusterPermissionGroups ;

    public static final RemoteClusterPermissions NONE = new RemoteClusterPermissions();

    public RemoteClusterPermissions(StreamInput in) throws IOException {
        remoteClusterPermissionGroups = in.readCollectionAsList(RemoteClusterPermissionGroup::new);
    }
    public RemoteClusterPermissions(){
        remoteClusterPermissionGroups = new ArrayList<>();
    }

    public RemoteClusterPermissions addGroup(RemoteClusterPermissionGroup remoteClusterPermissionGroup) {
        Objects.requireNonNull(remoteClusterPermissionGroup, "remoteClusterPermissionGroup must not be null");
        if(this == NONE) {
            throw new IllegalArgumentException("Cannot add a group to the `NONE` instance");
        }
        remoteClusterPermissionGroups.add(remoteClusterPermissionGroup);
        return this;
    }

    public String[] privilegeNames(final String remoteClusterAlias) {
        return
            remoteClusterPermissionGroups.stream()
                .filter(group -> group.hasPrivileges(remoteClusterAlias))
                .flatMap(groups -> Arrays.stream(groups.clusterPrivileges())).distinct().sorted().toArray(String[]::new);
    }

    public boolean hasPrivileges(final String remoteClusterAlias) {
        return remoteClusterPermissionGroups.stream()
            .anyMatch(remoteIndicesGroup -> remoteIndicesGroup.hasPrivileges(remoteClusterAlias));
    }

    public boolean hasPrivileges(){
        return remoteClusterPermissionGroups.isEmpty() == false;
    }

    public List<RemoteClusterPermissionGroup> groups() {
        return Collections.unmodifiableList(remoteClusterPermissionGroups);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (RemoteClusterPermissionGroup remoteClusterPermissionGroup : remoteClusterPermissionGroups) {
            builder.value(remoteClusterPermissionGroup);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(remoteClusterPermissionGroups);
    }
}
