/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the set of permissions for remote clusters. This is intended to be the model for both the {@link RoleDescriptor}
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
 * If the remote cluster does not support the privilege, as determined by the remote cluster version, the privilege will be not be sent.
 */
public class RemoteClusterPermissions implements NamedWriteable, ToXContentObject {

    public static final TransportVersion ROLE_REMOTE_CLUSTER_PRIVS = TransportVersions.V_8_15_0;

    public static final String NAME = "remote_cluster_permissions";
    private static final Logger logger = LogManager.getLogger(RemoteClusterPermissions.class);
    private final List<RemoteClusterPermissionGroup> remoteClusterPermissionGroups;

    // package private non-final for testing
    static Map<TransportVersion, Set<String>> allowedRemoteClusterPermissions = Map.of(
        ROLE_REMOTE_CLUSTER_PRIVS,
        Set.of(ClusterPrivilegeResolver.MONITOR_ENRICH.name())
    );

    public static final RemoteClusterPermissions NONE = new RemoteClusterPermissions();

    public static Set<String> getSupportedRemoteClusterPermissions() {
        return allowedRemoteClusterPermissions.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    public RemoteClusterPermissions(StreamInput in) throws IOException {
        remoteClusterPermissionGroups = in.readNamedWriteableCollectionAsList(RemoteClusterPermissionGroup.class);
    }

    public RemoteClusterPermissions() {
        remoteClusterPermissionGroups = new ArrayList<>();
    }

    public RemoteClusterPermissions addGroup(RemoteClusterPermissionGroup remoteClusterPermissionGroup) {
        Objects.requireNonNull(remoteClusterPermissionGroup, "remoteClusterPermissionGroup must not be null");
        if (this == NONE) {
            throw new IllegalArgumentException("Cannot add a group to the `NONE` instance");
        }
        remoteClusterPermissionGroups.add(remoteClusterPermissionGroup);
        return this;
    }

    /**
     * Gets the privilege names for the remote cluster. This method will collapse all groups to single String[] all lowercase
     * and will only return the appropriate privileges for the provided remote cluster version.
     */
    public String[] privilegeNames(final String remoteClusterAlias, TransportVersion remoteClusterVersion) {

        // get all privileges for the remote cluster
        Set<String> groupPrivileges = remoteClusterPermissionGroups.stream()
            .filter(group -> group.hasPrivileges(remoteClusterAlias))
            .flatMap(groups -> Arrays.stream(groups.clusterPrivileges()))
            .distinct()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());

        // find all the privileges that are allowed for the remote cluster version
        Set<String> allowedPermissionsPerVersion = allowedRemoteClusterPermissions.entrySet()
            .stream()
            .filter((entry) -> entry.getKey().onOrBefore(remoteClusterVersion))
            .map(Map.Entry::getValue)
            .flatMap(Set::stream)
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());

        // intersect the two sets to get the allowed privileges for the remote cluster version
        Set<String> allowedPrivileges = new HashSet<>(groupPrivileges);
        boolean hasRemovedPrivileges = allowedPrivileges.retainAll(allowedPermissionsPerVersion);
        if (hasRemovedPrivileges) {
            HashSet<String> removedPrivileges = new HashSet<>(groupPrivileges);
            removedPrivileges.removeAll(allowedPermissionsPerVersion);
            logger.info(
                "Removed unsupported remote cluster permissions {} for remote cluster [{}]. "
                    + "Due to the remote cluster version, only the following permissions are allowed: {}",
                removedPrivileges,
                remoteClusterAlias,
                allowedPrivileges
            );
        }

        return allowedPrivileges.stream().sorted().toArray(String[]::new);
    }

    /**
     * Validates the remote cluster permissions (regardless of remote cluster version).
     * This method will throw an {@link IllegalArgumentException} if the permissions are invalid.
     * Generally, this method is just a safety check and validity should be checked before adding the permissions to this class.
     */
    public void validate() {
        assert hasPrivileges();
        Set<String> invalid = getUnsupportedPrivileges();
        if (invalid.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Invalid remote_cluster permissions found. Please remove the following: "
                    + invalid
                    + " Only "
                    + getSupportedRemoteClusterPermissions()
                    + " are allowed"
            );
        }
    }

    /**
     * Returns the unsupported privileges in the remote cluster permissions (regardless of remote cluster version).
     * Empty set if all privileges are supported.
     */
    private Set<String> getUnsupportedPrivileges() {
        Set<String> invalid = new HashSet<>();
        for (RemoteClusterPermissionGroup group : remoteClusterPermissionGroups) {
            for (String namedPrivilege : group.clusterPrivileges()) {
                String toCheck = namedPrivilege.toLowerCase(Locale.ROOT);
                if (getSupportedRemoteClusterPermissions().contains(toCheck) == false) {
                    invalid.add(namedPrivilege);
                }
            }
        }
        return invalid;
    }

    public boolean hasPrivileges(final String remoteClusterAlias) {
        return remoteClusterPermissionGroups.stream().anyMatch(remoteIndicesGroup -> remoteIndicesGroup.hasPrivileges(remoteClusterAlias));
    }

    public boolean hasPrivileges() {
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
        out.writeNamedWriteableCollection(remoteClusterPermissionGroups);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteClusterPermissions that = (RemoteClusterPermissions) o;
        return Objects.equals(remoteClusterPermissionGroups, that.remoteClusterPermissionGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteClusterPermissionGroups);
    }

    @Override
    public String toString() {
        return "RemoteClusterPermissions{" + "remoteClusterPermissionGroups=" + remoteClusterPermissionGroups + '}';
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
