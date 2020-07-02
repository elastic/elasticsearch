/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for creating {@link Settings} instances defining a set of {@link DiscoveryNodeRole}.
 */
public class NodeRoles {

    public static Settings onlyRole(final DiscoveryNodeRole role) {
        return onlyRole(Settings.EMPTY, role);
    }

    public static Settings onlyRole(final Settings settings, final DiscoveryNodeRole role) {
        return onlyRoles(settings, Set.of(role));
    }

    public static Settings onlyRoles(final Set<DiscoveryNodeRole> roles) {
        return onlyRoles(Settings.EMPTY, roles);
    }

    public static Settings onlyRoles(final Settings settings, final Set<DiscoveryNodeRole> roles) {
        return Settings.builder()
            .put(settings)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toUnmodifiableList()))
            .build();
    }

    public static Settings removeRoles(final Set<DiscoveryNodeRole> roles) {
        return removeRoles(Settings.EMPTY, roles);
    }

    public static Settings removeRoles(final Settings settings, final Set<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder().put(settings);
        builder.putList(
            NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
            NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(Predicate.not(roles::contains))
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toUnmodifiableList())
        );
        return builder.build();
    }

    public static Settings addRoles(final Set<DiscoveryNodeRole> roles) {
        return addRoles(Settings.EMPTY, roles);
    }

    public static Settings addRoles(final Settings settings, final Set<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder().put(settings);
        builder.putList(
            NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
            Stream.concat(NodeRoleSettings.NODE_ROLES_SETTING.get(settings).stream(), roles.stream())
                .map(DiscoveryNodeRole::roleName)
                .distinct()
                .collect(Collectors.toUnmodifiableList())
        );
        return builder.build();
    }

    public static Settings noRoles() {
        return noRoles(Settings.EMPTY);
    }

    public static Settings noRoles(final Settings settings) {
        return Settings.builder().put(settings).putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of()).build();
    }

    public static Settings dataNode() {
        return dataNode(Settings.EMPTY);
    }

    public static Settings dataNode(final Settings settings) {
        return addRoles(settings, Set.of(DiscoveryNodeRole.DATA_ROLE));
    }

    public static Settings dataOnlyNode() {
        return dataOnlyNode(Settings.EMPTY);
    }

    public static Settings dataOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_ROLE);
    }

    public static Settings nonDataNode() {
        return nonDataNode(Settings.EMPTY);
    }

    public static Settings nonDataNode(final Settings settings) {
        return removeRoles(settings, Set.of(DiscoveryNodeRole.DATA_ROLE));
    }

    public static Settings ingestNode() {
        return ingestNode(Settings.EMPTY);
    }

    public static Settings ingestNode(final Settings settings) {
        return addRoles(settings, Set.of(DiscoveryNodeRole.INGEST_ROLE));
    }

    public static Settings ingestOnlyNode() {
        return ingestOnlyNode(Settings.EMPTY);
    }

    public static Settings ingestOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.INGEST_ROLE);
    }

    public static Settings nonIngestNode() {
        return nonIngestNode(Settings.EMPTY);
    }

    public static Settings nonIngestNode(final Settings settings) {
        return removeRoles(settings, Set.of(DiscoveryNodeRole.INGEST_ROLE));
    }

    public static Settings masterNode() {
        return masterNode(Settings.EMPTY);
    }

    public static Settings masterNode(final Settings settings) {
        return addRoles(settings, Set.of(DiscoveryNodeRole.MASTER_ROLE));
    }

    public static Settings masterOnlyNode() {
        return masterOnlyNode(Settings.EMPTY);
    }

    public static Settings masterOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.MASTER_ROLE);
    }

    public static Settings nonMasterNode() {
        return nonMasterNode(Settings.EMPTY);
    }

    public static Settings nonMasterNode(final Settings settings) {
        return removeRoles(settings, Set.of(DiscoveryNodeRole.MASTER_ROLE));
    }

    public static Settings remoteClusterClientNode() {
        return remoteClusterClientNode(Settings.EMPTY);
    }

    public static Settings remoteClusterClientNode(final Settings settings) {
        return addRoles(settings, Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

    public static Settings nonRemoteClusterClientNode() {
        return nonRemoteClusterClientNode(Settings.EMPTY);
    }

    public static Settings nonRemoteClusterClientNode(final Settings settings) {
        return removeRoles(settings, Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

}
