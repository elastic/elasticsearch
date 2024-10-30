/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.Fields.CLUSTERS;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.Fields.PRIVILEGES;

/**
 * Represents a group of permissions for a remote cluster. For example:
 * <code>
 {
    "privileges" : ["monitor_enrich"],
    "clusters" : ["*"]
 }
 * </code>
 */
public class RemoteClusterPermissionGroup implements NamedWriteable, ToXContentObject {

    public static final String NAME = "remote_cluster_permission_group";
    private final String[] clusterPrivileges;
    private final String[] remoteClusterAliases;
    private final StringMatcher remoteClusterAliasMatcher;

    public RemoteClusterPermissionGroup(StreamInput in) throws IOException {
        clusterPrivileges = in.readStringArray();
        remoteClusterAliases = in.readStringArray();
        remoteClusterAliasMatcher = StringMatcher.of(remoteClusterAliases);
    }

    public RemoteClusterPermissionGroup(Map<String, List<String>> remoteClusterGroup) {
        assert remoteClusterGroup.get(PRIVILEGES.getPreferredName()) != null : "privileges must be non-null";
        assert remoteClusterGroup.get(CLUSTERS.getPreferredName()) != null : "clusters must be non-null";
        clusterPrivileges = remoteClusterGroup.get(PRIVILEGES.getPreferredName()).toArray(new String[0]);
        remoteClusterAliases = remoteClusterGroup.get(CLUSTERS.getPreferredName()).toArray(new String[0]);
        remoteClusterAliasMatcher = StringMatcher.of(remoteClusterAliases);
    }

    /**
     * @param clusterPrivileges The list of cluster privileges that are allowed for the remote cluster. must not be null or empty.
     * @param remoteClusterAliases The list of remote clusters that the privileges apply to. must not be null or empty.
     */
    public RemoteClusterPermissionGroup(String[] clusterPrivileges, String[] remoteClusterAliases) {
        if (clusterPrivileges == null
            || remoteClusterAliases == null
            || clusterPrivileges.length <= 0
            || remoteClusterAliases.length <= 0) {
            throw new IllegalArgumentException("remote cluster groups must not be null or empty");
        }
        if (Arrays.stream(clusterPrivileges).anyMatch(s -> Strings.hasText(s) == false)) {
            throw new IllegalArgumentException(
                "remote_cluster privileges must contain valid non-empty, non-null values " + Arrays.toString(clusterPrivileges)
            );
        }
        if (Arrays.stream(remoteClusterAliases).anyMatch(s -> Strings.hasText(s) == false)) {
            throw new IllegalArgumentException(
                "remote_cluster clusters aliases must contain valid non-empty, non-null values " + Arrays.toString(remoteClusterAliases)
            );
        }

        this.clusterPrivileges = clusterPrivileges;
        this.remoteClusterAliases = remoteClusterAliases;
        this.remoteClusterAliasMatcher = StringMatcher.of(remoteClusterAliases);
    }

    /**
     * @param remoteClusterAlias The remote cluster alias to check to see if has privileges defined in this group.
     * @return true if the remote cluster alias has privileges defined in this group, false otherwise.
     */
    public boolean hasPrivileges(final String remoteClusterAlias) {
        return remoteClusterAliasMatcher.test(remoteClusterAlias);
    }

    /**
     * @return A copy of the cluster privileges.
     */
    public String[] clusterPrivileges() {
        return Arrays.copyOf(clusterPrivileges, clusterPrivileges.length);
    }

    /**
     * @return A copy of the cluster aliases.
     */
    public String[] remoteClusterAliases() {
        return Arrays.copyOf(remoteClusterAliases, remoteClusterAliases.length);
    }

    /**
     * Converts the group to a map representation.
     * @return A map representation of the group.
     */
    public Map<String, List<String>> toMap() {
        return Map.of(
            PRIVILEGES.getPreferredName(),
            Arrays.asList(clusterPrivileges),
            CLUSTERS.getPreferredName(),
            Arrays.asList(remoteClusterAliases)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(PRIVILEGES.getPreferredName(), clusterPrivileges);
        builder.array(CLUSTERS.getPreferredName(), remoteClusterAliases);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(clusterPrivileges);
        out.writeStringArray(remoteClusterAliases);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteClusterPermissionGroup that = (RemoteClusterPermissionGroup) o;
        // remoteClusterAliasMatcher property is intentionally omitted
        return Arrays.equals(clusterPrivileges, that.clusterPrivileges) && Arrays.equals(remoteClusterAliases, that.remoteClusterAliases);
    }

    @Override
    public int hashCode() {
        // remoteClusterAliasMatcher property is intentionally omitted
        int result = Arrays.hashCode(clusterPrivileges);
        result = 31 * result + Arrays.hashCode(remoteClusterAliases);
        return result;
    }

    @Override
    public String toString() {
        return "RemoteClusterPermissionGroup{"
            + "privileges="
            + Arrays.toString(clusterPrivileges)
            + ", clusters="
            + Arrays.toString(remoteClusterAliases)
            + '}';
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
