/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

// TODO: Move this test to the Serverless repo once the IT framework is ready there.
public class ServerlessClusterStatsShouldNotExposeSkipUnavailableIT extends AbstractMultiClustersTestCase {
    private static final String LINKED_CLUSTER_1 = "cluster-a";

    public static class CpsPlugin extends Plugin implements ClusterPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(CpsEnableSetting);
        }
    }

    private static final Setting<String> CpsEnableSetting = Setting.simpleString(
        "serverless.cross_project.enabled",
        Setting.Property.NodeScope
    );

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(LINKED_CLUSTER_1);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CpsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(LINKED_CLUSTER_1, randomBoolean());
    }

    public void testSkipUnavailableShouldNotBeExposed() throws Exception {
        assertResponse(
            client().execute(TransportClusterStatsAction.TYPE, new ClusterStatsRequest(/* Do include remotes */ true)),
            result -> {
                // In the Serverless environment, skip_unavailable should map to `Optional.empty()`.
                assertThat(result.getRemoteClustersStats().get(LINKED_CLUSTER_1).skipUnavailable(), Matchers.is(Optional.empty()));
                // When this result is serialised to JSON, it should not mention `skip_unavailable`.
                assertThat(result.toString().contains("skip_unavailable"), Matchers.is(false));
            }
        );
    }
}
