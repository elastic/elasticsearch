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

package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration test for corrupted translog files
 */
@ESIntegTestCase.ClusterScope(scope= ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class CorruptedTranslogIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // we really need local GW here since this also checks for corruption etc.
        // and we need to make sure primaries are not just trashed if we don't have replicas
        return pluginList(MockTransportService.TestPlugin.class);
    }

    public void testCorruptTranslogFiles() throws Exception {
        internalCluster().startNodesAsync(1, Settings.EMPTY).get();

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "-1")
                .put(MockEngineSupport.FLUSH_ON_CLOSE_RATIO, 0.0d) // never flush - always recover from translog
                .put(IndexShard.INDEX_FLUSH_ON_CLOSE, false) // never flush - always recover from translog
        ));
        ensureYellow();

        // Index some documents
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("foo", "bar");
        }
        disableTranslogFlush("test");
        indexRandom(false, false, false, Arrays.asList(builders));  // this one

        // Corrupt the translog file(s)
        corruptRandomTranslogFiles();

        // Restart the single node
        internalCluster().fullRestart();
        client().admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout(new TimeValue(1000, TimeUnit.MILLISECONDS)).setWaitForEvents(Priority.LANGUID).get();

        try {
            client().prepareSearch("test").setQuery(matchAllQuery()).get();
            fail("all shards should be failed due to a corrupted translog");
        } catch (SearchPhaseExecutionException e) {
            e.printStackTrace();
            // Good, all shards should be failed because there is only a
            // single shard and its translog is corrupt
        }
    }


    private void corruptRandomTranslogFiles() throws IOException {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(new String[]{"test"}, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(getRandom(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        Set<Path> files = new TreeSet<>(); // treeset makes sure iteration order is deterministic
        for (FsInfo.Path fsPath : nodeStatses.getNodes()[0].getFs()) {
            String path = fsPath.getPath();
            final String relativeDataLocationPath =  "indices/test/" + Integer.toString(shardRouting.getId()) + "/translog";
            Path file = PathUtils.get(path).resolve(relativeDataLocationPath);
            if (Files.exists(file)) {
                logger.info("--> path: {}", file);
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(file)) {
                    for (Path item : stream) {
                        logger.info("--> File: {}", item);
                        if (Files.isRegularFile(item) && item.getFileName().toString().startsWith("translog-")) {
                            files.add(item);
                        }
                    }
                }
            }
        }
        Path fileToCorrupt = null;
        if (!files.isEmpty()) {
            int corruptions = randomIntBetween(5, 20);
            for (int i = 0; i < corruptions; i++) {
                fileToCorrupt = RandomPicks.randomFrom(getRandom(), files);
                try (FileChannel raf = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    // read
                    raf.position(randomIntBetween(0, (int) Math.min(Integer.MAX_VALUE, raf.size() - 1)));
                    long filePointer = raf.position();
                    ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
                    raf.read(bb);
                    bb.flip();

                    // corrupt
                    byte oldValue = bb.get(0);
                    byte newValue = (byte) (oldValue + 1);
                    bb.put(0, newValue);

                    // rewrite
                    raf.position(filePointer);
                    raf.write(bb);
                    logger.info("--> corrupting file {} --  flipping at position {} from {} to {} file: {}",
                            fileToCorrupt, filePointer, Integer.toHexString(oldValue),
                            Integer.toHexString(newValue), fileToCorrupt);
                }
            }
        }
        assertThat("no file corrupted", fileToCorrupt, notNullValue());
    }

    /** Disables translog flushing for the specified index */
    private static void disableTranslogFlush(String index) {
        Settings settings = Settings.builder().put(IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, new ByteSizeValue(1, ByteSizeUnit.PB)).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    /** Enables translog flushing for the specified index */
    private static void enableTranslogFlush(String index) {
        Settings settings = Settings.builder().put(IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, new ByteSizeValue(512, ByteSizeUnit.MB)).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }
}
