/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.PluginsStat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.plugins.PluginStat;
import org.elasticsearch.plugins.PluginStatsService;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class NodeStatsPluginIT extends ESIntegTestCase {

    @Test
    public void testPluginStats() throws Exception {
        logger.debug("--> starting first node with no custom stats");
        String node1 = internalCluster().startNode();

        logger.debug("--> checking that plugins stats are empty");
        NodesStatsResponse response = client().admin().cluster().prepareNodesStats().setPlugins(true).get();
        assertNodesCount(response.getNodes(), 1);
        for (NodeStats node : response.getNodes()) {
            assertNull(node.getPluginsStat());
        }

        logger.debug("--> starting second node with custom stats");
        String node2 = internalCluster().startNode(Settings.builder().put("plugin.types", FirstPlugin.class.getName()));
        response = client().admin().cluster().prepareNodesStats().setPlugins(true).get();
        assertNodesCount(response.getNodes(), 2);

        logger.debug("--> checking that plugins stats now contain the 'foo' stat with expected values");
        assertPluginStat(response.getNodes(), "foo", "foo_1", 123, "foo_2", 1437580442979L);

        logger.debug("--> starting third node with custom stats");
        String node3 = internalCluster().startNode(Settings.builder().put("plugin.types", SecondPlugin.class.getName()));
        response = client().admin().cluster().prepareNodesStats().setPlugins(true).get();
        assertNodesCount(response.getNodes(), 3);

        logger.debug("--> checking that plugins stats now containall custom stats with expected values");
        assertPluginStat(response.getNodes(), "foo", "foo_1", 123, "foo_2", 1437580442979L);
        assertPluginStat(response.getNodes(), "bar", "bar_1", 1437580443456L);
        assertPluginStat(response.getNodes(), "baz", "baz_1", true, "baz_2", "value");
        assertPluginStat(response.getNodes(), "qux", "id", "qux_1", "count", 456);

        logger.debug("--> checking that plugins stats are not returned when plugins parameter is set to false");
        response = client().admin().cluster().prepareNodesStats().setPlugins(false).get();
        assertNotPluginStat(response.getNodes(), "foo");
        assertNotPluginStat(response.getNodes(), "bar");
        assertNotPluginStat(response.getNodes(), "baz");
        assertNotPluginStat(response.getNodes(), "qux");

        logger.debug("--> checking that only node2 stats are returned");
        response = client().admin().cluster().prepareNodesStats(node2).all().get();
        assertPluginStat(response.getNodes(), "foo", "foo_1", 123, "foo_2", 1437580442979L);
        assertNotPluginStat(response.getNodes(), "bar");
        assertNotPluginStat(response.getNodes(), "baz");
        assertNotPluginStat(response.getNodes(), "qux");

        logger.debug("--> checking that only node3 stats are returned, whichever node is requested");
        for (String nodeId : Arrays.asList(node1, node2, node3)) {
            response = client(nodeId).admin().cluster().prepareNodesStats(node3).all().get();
            assertNotPluginStat(response.getNodes(), "foo");
            assertPluginStat(response.getNodes(), "bar", "bar_1", 1437580443456L);
            assertPluginStat(response.getNodes(), "baz", "baz_1", true, "baz_2", "value");
            assertPluginStat(response.getNodes(), "qux", "id", "qux_1", "count", 456);
        }

        logger.debug("--> checking that only 'bar' stats is returned");
        response = client().admin().cluster().prepareNodesStats().setPlugins(false).setCustom("bar", "none").get();
        assertNotPluginStat(response.getNodes(), "foo");
        assertPluginStat(response.getNodes(), "bar", "bar_1", 1437580443456L);
        assertNotPluginStat(response.getNodes(), "baz");
        assertNotPluginStat(response.getNodes(), "qux");

        logger.debug("--> checking that only 'ba*' stats are returned");
        response = client().admin().cluster().prepareNodesStats().setPlugins(false).setCustom("ba*").get();
        assertNotPluginStat(response.getNodes(), "foo");
        assertPluginStat(response.getNodes(), "bar", "bar_1", 1437580443456L);
        assertPluginStat(response.getNodes(), "baz", "baz_1", true, "baz_2", "value");
        assertNotPluginStat(response.getNodes(), "qux");

        logger.debug("--> checking that 'bar' stats can't be returned when only node 1 & 2 are requested");
        response = client().admin().cluster().prepareNodesStats(node1, node2).setCustom("bar", "none").get();
        assertNotPluginStat(response.getNodes(), "foo");
        assertNotPluginStat(response.getNodes(), "bar");
        assertNotPluginStat(response.getNodes(), "baz");
        assertNotPluginStat(response.getNodes(), "qux");
    }

    private void assertNodesCount(NodeStats[] nodes, int count) {
        assertNotNull(nodes);
        assertThat(nodes.length, equalTo(count));
    }

    private void assertPluginStat(NodeStats[] nodes, String category, Object... values) {
        assertTrue("plugin stat should exist", containsPluginStat(nodes, category, values));
    }

    private void assertNotPluginStat(NodeStats[] nodes, String category, Object... values) {
        assertFalse("plugin stat should not exist", containsPluginStat(nodes, category, values));
    }

    private boolean containsPluginStat(NodeStats[] nodes, String category, Object... values) {
        try {
            for (NodeStats node : nodes) {
                PluginsStat stats = node.getPluginsStat();
                if (stats != null) {
                    for (PluginStat stat : stats.getStats()) {
                        Map<String, Object> statsAsMap = stat.getStatsAsMap();
                        if (statsAsMap.containsKey(category)) {
                            if (!CollectionUtils.isEmpty(values)) {
                                // Builds a map with the expected values
                                Map<String, Object> expected = new HashMap<>();
                                for (int i = 0; i < values.length; i++) {
                                    expected.put((String) values[i], values[++i]);
                                }

                                // Compare the plugin stat with the expected map values
                                Map<String, Object> data = (Map<String, Object>) statsAsMap.get(category);
                                assertThat(data, Matchers.equalTo(expected));
                                return true;
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            // Ok
        }
        return false;
    }

    public static class FirstPlugin extends AbstractPlugin {
        @Override
        public String name() {
            return "first-plugin";
        }

        @Override
        public String description() {
            return "A plugin that provides 1 custom statistic";
        }

        public void onModule(PluginsModule pluginsModule) {
            pluginsModule.registerStatsService(FirstStatsService.class);
        }
    }

    public static class FirstStatsService implements PluginStatsService {
        @Override
        public Collection<PluginStat> stats() {
            List<PluginStat> stats = new ArrayList<>();
            try {
                PluginStat stat = new PluginStat("foo", jsonBuilder()
                                                            .startObject()
                                                                .field("foo_1", 123)
                                                                .field("foo_2", 1437580442979L)
                                                            .endObject()
                                                        .bytes());
                stats.add(stat);
            } catch (IOException e) {
                // We don't care
            }
            return stats;
        }
    }

    public static class SecondPlugin extends AbstractPlugin {

        public SecondPlugin() {
        }

        @Override
        public String name() {
            return "second-plugin";
        }

        @Override
        public String description() {
            return "Another plugin that provides custom stats";
        }

        public void onModule(PluginsModule pluginsModule) {
            pluginsModule.registerStatsService(SecondStatsService.class);
        }
    }

    public static class SecondStatsService implements PluginStatsService {
        @Override
        public Collection<PluginStat> stats() {
            List<PluginStat> stats = new ArrayList<>();
            try {
                PluginStat stat1 = new PluginStat("bar", smileBuilder()
                                                            .startObject()
                                                                .field("bar_1", 1437580443456L)
                                                            .endObject()
                                                         .bytes());

                PluginStat stat2 = new PluginStat("baz", jsonBuilder()
                                                            .startObject()
                                                                .field("baz_1", true)
                                                                .field("baz_2", "value")
                                                            .endObject()
                                                        .bytes());

                PluginStat stat3 = new PluginStat("qux", new CustomStat("qux_1", 456), randomFrom(XContentType.values()), ToXContent.EMPTY_PARAMS);

                stats.add(stat1);
                stats.add(stat2);
                stats.add(stat3);
            } catch (IOException e) {
                // We don't care
            }
            return stats;
        }
    }

    public static class CustomStat implements ToXContent {

        private String id;
        private int count;

        public CustomStat(String id, int count) {
            this.id = id;
            this.count = count;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("id", id);
            builder.field("count", count);
            return builder;
        }
    }
}
