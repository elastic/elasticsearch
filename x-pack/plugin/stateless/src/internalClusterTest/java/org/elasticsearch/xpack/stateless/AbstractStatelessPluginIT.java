/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.STATELESS_ENABLED;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AbstractStatelessPluginIT extends ESIntegTestCase {

    public static class StatelessPluginWithTrialLicense extends StatelessPlugin {
        public StatelessPluginWithTrialLicense(Settings settings) {
            super(settings);
        }

        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.TRIAL, true, null));
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<Class<? extends Plugin>>();
        plugins.addAll(super.nodePlugins());
        plugins.addAll(getRequiredNodePlugins());
        return List.copyOf(plugins);
    }

    public static Collection<Class<? extends Plugin>> getRequiredNodePlugins() {
        var plugins = new ArrayList<Class<? extends Plugin>>();
        plugins.add(BlobCachePlugin.class);
        plugins.add(StatelessPluginWithTrialLicense.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return List.copyOf(plugins);
    }

    protected Settings.Builder nodeSettings() {
        final Settings.Builder builder = Settings.builder().put(STATELESS_ENABLED.getKey(), true);
        return builder;
    }

    protected String startIndexNode() {
        return startIndexNode(Settings.EMPTY);
    }

    protected String startIndexNode(Settings extraSettings) {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.INDEX_ROLE).put(extraSettings));
    }

    protected String startSearchNode() {
        return startSearchNode(Settings.EMPTY);
    }

    protected String startSearchNode(Settings extraSettings) {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.SEARCH_ROLE).put(extraSettings));
    }

    protected Settings.Builder settingsForRoles(DiscoveryNodeRole... roles) {
        var builder = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Arrays.stream(roles).map(DiscoveryNodeRole::roleName).toList());

        // when changing those values, keep in mind that multiple nodes with their own caches can be created by integration tests which can
        // also be executed concurrently.
        if (frequently()) {
            if (randomBoolean()) {
                // region is between 1 page (4kb) to 8 pages (32kb)
                var regionPages = randomIntBetween(1, 8);
                builder.put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes((long) regionPages * SharedBytes.PAGE_SIZE));
                // cache is between 1 and 256 regions (max. possible cache size is 8mb)
                builder.put(
                    SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.ofBytes((long) randomIntBetween(regionPages, 256) * SharedBytes.PAGE_SIZE)
                );
            } else {
                if (randomBoolean()) {
                    // region is between 8 pages (32kb) to 256 pages (1mb)
                    var regionPages = randomIntBetween(8, 256);
                    builder.put(
                        SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                        ByteSizeValue.ofBytes((long) regionPages * SharedBytes.PAGE_SIZE)
                    );
                }
                // cache only uses up to 0.1% disk to be friendly with default region size
                builder.put(SHARED_CACHE_SIZE_SETTING.getKey(), new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString());
            }
        } else {
            // no cache (a single region does not even fit in the cache)
            builder.put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1L));
        }

        // Add settings from nodeSettings last, allowing them to take precedence over the randomly generated values
        return builder.put(nodeSettings().build());
    }

    protected String startMasterOnlyNode() {
        return startMasterOnlyNode(Settings.EMPTY);
    }

    protected String startMasterOnlyNode(Settings extraSettings) {
        return internalCluster().startMasterOnlyNode(nodeSettings().put(extraSettings).build());
    }

    protected String startMasterAndIndexNode() {
        return startMasterAndIndexNode(Settings.EMPTY);
    }

    protected String startMasterAndIndexNode(Settings extraSettings) {
        return internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE).put(extraSettings)
        );
    }

    protected List<String> startIndexNodes(int numOfNodes) {
        return startIndexNodes(numOfNodes, Settings.EMPTY);
    }

    protected List<String> startIndexNodes(int numOfNodes, Settings extraSettings) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startIndexNode(extraSettings));
        }
        return List.copyOf(nodes);
    }

    protected List<String> startSearchNodes(int numOfNodes) {
        return startSearchNodes(numOfNodes, Settings.EMPTY);
    }

    protected List<String> startSearchNodes(int numOfNodes, Settings extraSettings) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startSearchNode(extraSettings));
        }
        return List.copyOf(nodes);
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false;
    }

    @Override
    protected boolean autoManageVotingExclusions() {
        return false;
    }
}
