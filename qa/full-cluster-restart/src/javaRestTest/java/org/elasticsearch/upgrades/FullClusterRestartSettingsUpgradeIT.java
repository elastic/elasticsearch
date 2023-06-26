/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.transport.RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartSettingsUpgradeIT extends ParameterizedFullClusterRestartTestCase {

    protected static LocalClusterConfigProvider clusterConfig = c -> {};

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .apply(() -> clusterConfig)
        .build();

    public FullClusterRestartSettingsUpgradeIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testRemoteClusterSettingsUpgraded() throws IOException {
        assumeTrue("skip_unavailable did not exist until 6.1.0", getOldClusterVersion().onOrAfter(Version.V_6_1_0));
        assumeTrue("settings automatically upgraded since 6.5.0", getOldClusterVersion().before(Version.V_6_5_0));
        if (isRunningAgainstOldCluster()) {
            final Request putSettingsRequest = new Request("PUT", "/_cluster/settings");
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.startObject("persistent");
                    {
                        builder.field("search.remote.foo.skip_unavailable", true);
                        builder.field("search.remote.foo.seeds", Collections.singletonList("localhost:9200"));
                    }
                    builder.endObject();
                }
                builder.endObject();
                putSettingsRequest.setJsonEntity(Strings.toString(builder));
            }
            client().performRequest(putSettingsRequest);

            final Request getSettingsRequest = new Request("GET", "/_cluster/settings");
            final Response response = client().performRequest(getSettingsRequest);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, response.getEntity().getContent())) {
                final ClusterGetSettingsResponse clusterGetSettingsResponse = ClusterGetSettingsResponse.fromXContent(parser);
                final Settings settings = clusterGetSettingsResponse.getPersistentSettings();

                assertTrue(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").get(settings));
                assertTrue(SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                assertThat(
                    SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").get(settings),
                    equalTo(Collections.singletonList("localhost:9200"))
                );
            }

            assertSettingDeprecationsAndWarnings(
                new Setting<?>[] {
                    SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo"),
                    SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo") }
            );
        } else {
            final Request getSettingsRequest = new Request("GET", "/_cluster/settings");
            final Response getSettingsResponse = client().performRequest(getSettingsRequest);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, getSettingsResponse.getEntity().getContent())) {
                final ClusterGetSettingsResponse clusterGetSettingsResponse = ClusterGetSettingsResponse.fromXContent(parser);
                final Settings settings = clusterGetSettingsResponse.getPersistentSettings();

                assertFalse(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(
                    settings.toString(),
                    RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings)
                );
                assertTrue(RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").get(settings));
                assertFalse(SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                assertThat(
                    SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").get(settings),
                    equalTo(Collections.singletonList("localhost:9200"))
                );
            }
        }
    }

}
