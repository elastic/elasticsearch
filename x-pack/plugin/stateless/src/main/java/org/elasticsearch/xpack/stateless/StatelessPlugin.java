/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterModule.DESIRED_BALANCE_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING;
import static org.elasticsearch.common.settings.Setting.boolSetting;

public class StatelessPlugin extends Plugin implements ClusterCoordinationPlugin, ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(StatelessPlugin.class);

    public static final LicensedFeature.Persistent STATELESS_FEATURE = LicensedFeature.persistent(
        null,
        "stateless",
        License.OperationMode.ENTERPRISE
    );

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = boolSetting(
        DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME,
        true,
        Property.NodeScope
    );
    public static final Setting<TimeValue> FAILURE_STORE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        MetadataCreateDataStreamService.FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME,
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    public static final String NAME = "stateless";

    private final boolean enabled;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(STATELESS_ENABLED, DATA_STREAMS_LIFECYCLE_ONLY_MODE, FAILURE_STORE_REFRESH_INTERVAL_SETTING);
    }

    public StatelessPlugin(Settings settings) {
        enabled = STATELESS_ENABLED.get(settings);
        if (enabled) {
            var nonStatelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (nonStatelessDataNodeRoles.isEmpty() == false) {
                throw new IllegalArgumentException(NAME + " does not support node roles " + nonStatelessDataNodeRoles);
            }
            var statelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(STATELESS_ROLES::contains)
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (statelessDataNodeRoles.size() > 1) {
                throw new IllegalArgumentException(NAME + " does not support a node with more than 1 role of " + statelessDataNodeRoles);
            }
            if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.exists(settings)) {
                if (CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings)) {
                    throw new IllegalArgumentException(
                        NAME + " does not support " + CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()
                    );
                }
            }
            if (Objects.equals(SHARDS_ALLOCATOR_TYPE_SETTING.get(settings), DESIRED_BALANCE_ALLOCATOR) == false) {
                throw new IllegalArgumentException(
                    NAME + " can only be used with " + SHARDS_ALLOCATOR_TYPE_SETTING.getKey() + "=" + DESIRED_BALANCE_ALLOCATOR
                );
            }

            logger.info("[{}] is enabled", NAME);
        } else {
            var statelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                .stream()
                .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r))
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.toSet());
            if (statelessDataNodeRoles.isEmpty() == false) {
                throw new IllegalArgumentException(
                    NAME + " is not enabled, but stateless-only node roles are configured: " + statelessDataNodeRoles
                );
            }
        }
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (enabled) {
            final var licenseState = getLicenseState();
            if (STATELESS_FEATURE.checkAndStartTracking(licenseState, NAME) == false) {
                throw new IllegalStateException(
                    NAME
                        + " cannot be enabled with a ["
                        + licenseState.getOperationMode()
                        + "] license. It is only allowed with an Enterprise license."
                );
            }
        }
        return Collections.emptyList();
    }

    // overridable by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Settings additionalSettings() {
        if (enabled) {
            return Settings.builder()
                .put(super.additionalSettings())
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                .put(DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey(), true)
                .put(FAILURE_STORE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30))
                .build();
        } else {
            return super.additionalSettings();
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (enabled) {
            STATELESS_FEATURE.stopTracking(getLicenseState(), NAME);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }
}
