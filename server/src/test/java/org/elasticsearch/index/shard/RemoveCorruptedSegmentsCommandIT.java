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
package org.elasticsearch.index.shard;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.shard.RemoveCorruptedSegmentsCommandTests.corruptIndexFiles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoveCorruptedSegmentsCommandIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    public void testCorruptIndex() throws Exception {
        final String node = internalCluster().startNode();

        final String indexName = "index42";
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")
        ));

        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
            }

            numDocs += numExtraDocs;

            indexRandom(false, false, false, Arrays.asList(builders));
            flush(indexName);
        }

        logger.info("--> indexed {} docs", numDocs);

        final RemoveCorruptedSegmentsCommand command = new RemoveCorruptedSegmentsCommand();
        final MockTerminal t = new MockTerminal();
        final OptionParser parser = command.getParser();

        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());

        // Try running it before the node is stopped (and shard is closed)
        try {
            final OptionSet options = parser.parse("-index", indexName, "-shard-id", "0");
            command.execute(t, options, environment);
            fail("expected the command to fail as there is no corruption file marker");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Failed to lock node's directory"));
        }

        final Set<Path> indexDirs = getIndexDirs(indexName);
        assertThat(indexDirs, hasSize(1));

        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // Try running it before the shard is corrupted, it should flip out because there is no corruption file marker
                try {
                    final OptionSet options = parser.parse("-index", indexName, "-shard-id", "0");
                    command.execute(t, options, environment);
                    fail("expected the command to fail as there is no corruption file marker");
                } catch (Exception e) {
                    assertThat(e.getMessage(), containsString("There is no corruption file marker"));
                }

                corruptIndexFiles(indexDirs.iterator().next(), false);
                return super.onNodeStopped(nodeName);
            }
        });

        ensureStableCluster(1);

        // shard should be failed due to a corrupted index
        final SearchPhaseExecutionException searchPhaseExecutionException = expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearch(indexName).setQuery(matchAllQuery()).get());
        assertThat(searchPhaseExecutionException.getMessage(), equalTo("all shards failed"));

        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                t.addTextInput("y");
                t.addTextInput("y");

                // Try running it before the shard is corrupted, it should flip out because there is no corruption file marker
                final OptionSet options = parser.parse("-index", indexName, "-shard-id", "0");
                command.execute(t, options, environment);

                return super.onNodeStopped(nodeName);
            }
        });

        ensureStableCluster(1);
        String nodeId = null;
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getNodes()) {
            final String name = cursor.value.getName();
            if (name.equals(node)) {
                nodeId = cursor.key;
                break;
            }
        }
        assertThat(nodeId, notNullValue());

        assertThat(t.getOutput(), containsString("allocate_stale_primary"));
        assertThat(t.getOutput(), containsString("\"node\" : \"" + nodeId + "\""));

        final ClusterAllocationExplanation explanation =
            client().admin().cluster().prepareAllocationExplain()
                .setIndex(indexName).setShard(0).setPrimary(true)
                .get().getExplanation();
        // there is only _stale_ primary (due to new allocation id)
        assertThat(explanation.getCurrentNode(), is(nullValue()));
        assertThat(explanation.getShardState(), equalTo(ShardRoutingState.UNASSIGNED));

        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, nodeId, true))
            .get();

        final ClusterAllocationExplanation explanation2 =
            client().admin().cluster().prepareAllocationExplain()
                .setIndex(indexName).setShard(0).setPrimary(true)
                .get().getExplanation();

        assertThat(explanation2.getCurrentNode(), notNullValue());
        assertThat(explanation2.getShardState(), isOneOf(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED));

        final Pattern pattern = Pattern.compile("Corrupted segments found -\\s+(?<docs>\\d+) documents will be lost.");
        final Matcher matcher = pattern.matcher(t.getOutput());
        assertThat(matcher.find(), equalTo(true));
        final int expectedNumDocs = numDocs - Integer.parseInt(matcher.group("docs"));

        ensureGreen(indexName);
        // Run a search and make sure it succeeds
        assertHitCount(client().prepareSearch(indexName).setQuery(matchAllQuery()).get(), expectedNumDocs);
    }

    private Set<Path> getIndexDirs(String indexName) throws IOException {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[]{indexName}, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        ShardId shardId = shardRouting.shardId();
        return getIndexDirs(nodeId, shardId);
    }

    private Set<Path> getIndexDirs(String nodeId, ShardId shardId) {
        final NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        final Set<Path> translogDirs = new TreeSet<>();
        final NodeStats nodeStats = nodeStatses.getNodes().get(0);
        for (FsInfo.Path fsPath : nodeStats.getFs()) {
            String path = fsPath.getPath();
            final String relativeDataLocationPath = "indices/" + shardId.getIndex().getUUID() + "/" + Integer.toString(shardId.getId())
                + "/" + ShardPath.INDEX_FOLDER_NAME;
            Path translogPath = PathUtils.get(path).resolve(relativeDataLocationPath);
            if (Files.isDirectory(translogPath)) {
                translogDirs.add(translogPath);
            }
        }
        return translogDirs;
    }
}
