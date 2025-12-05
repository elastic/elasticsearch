/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.StatelessPlugin.STATELESS_ENABLED;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.STATELESS_ROLES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class StatelessPluginTests extends ESTestCase {

    private static StatelessPlugin createStatelessPlugin(Settings settings, License.OperationMode mode, boolean active) {
        final var plugin = new StatelessPlugin(settings) {
            protected XPackLicenseState getLicenseState() {
                return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(mode, active, null));
            }
        };
        plugin.createComponents(mock(Plugin.PluginServices.class));
        return plugin;
    }

    private static StatelessPlugin createStatelessPlugin(Settings settings) {
        return createStatelessPlugin(settings, randomBoolean() ? License.OperationMode.ENTERPRISE : License.OperationMode.TRIAL, true);
    }

    public void testValidLicense() throws Exception {
        final var enabled = randomBoolean();
        final var settings = enabled ? Settings.builder().put(STATELESS_ENABLED.getKey(), true).build() : Settings.EMPTY;
        final var licenseActive = randomBoolean();
        final Runnable runnable = () -> createStatelessPlugin(
            settings,
            randomBoolean() ? License.OperationMode.ENTERPRISE : License.OperationMode.TRIAL,
            licenseActive
        );
        if (enabled && licenseActive == false) {
            expectThrows(IllegalStateException.class, runnable::run);
        } else {
            runnable.run();
        }
    }

    public void testInvalidLicense() throws Exception {
        final var enabled = randomBoolean();
        final var settings = enabled ? Settings.builder().put(STATELESS_ENABLED.getKey(), true).build() : Settings.EMPTY;
        final var licenseActive = randomBoolean();
        final License.OperationMode invalidMode = randomFrom(
            License.OperationMode.PLATINUM,
            License.OperationMode.GOLD,
            License.OperationMode.STANDARD,
            License.OperationMode.BASIC
        );
        final Runnable runnable = () -> createStatelessPlugin(settings, invalidMode, licenseActive);
        if (enabled) {
            expectThrows(IllegalStateException.class, runnable::run);
        } else {
            runnable.run();
        }
    }

    public void testSettingsWithValidStatelessRole() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(randomFrom(STATELESS_ROLES).roleName()))
            .build();
        createStatelessPlugin(nodeSettings);
    }

    public void testSettingsWithBothIndexAndSearchRolesFail() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                List.of(DiscoveryNodeRole.INDEX_ROLE.roleName(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            )
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeSettings));
        assertThat(ex.getMessage(), containsString("does not support a node with more than 1 role of"));
    }

    public void testSettingsWithStatelessRoleAndStatelessDisabledFail() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), false)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(randomFrom(STATELESS_ROLES).roleName()))
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeSettings));
        assertThat(ex.getMessage(), containsString("but stateless-only node roles are configured"));
    }

    public void testSettingsWithNonStatelessRoleAndStatelessEnabledFail() throws Exception {
        final var nonStatelessRoles = DiscoveryNodeRole.roles()
            .stream()
            .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
            .collect(Collectors.toSet());
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(randomFrom(nonStatelessRoles).roleName()))
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeSettings));
        assertThat(ex.getMessage(), containsString("does not support node roles"));
    }

    public void testSettingsWithDefaultDiskThresholdEnabled() throws Exception {
        final var nodeSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                randomFrom(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE).roleName()
            )
            .build();
        final var plugin = createStatelessPlugin(nodeSettings);
        assertThat(
            plugin.additionalSettings().get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()),
            equalTo("false")
        );

        final var nodeInvalidSettings = Settings.builder()
            .put(STATELESS_ENABLED.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> createStatelessPlugin(nodeInvalidSettings));
        assertThat(ex.getMessage(), containsString("does not support cluster.routing.allocation.disk.threshold_enabled"));
    }

}
