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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsTests;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link RestIndicesAction}
 */
public class RestIndicesActionTests extends ESTestCase {

    public void testBuildTable() {
        final Settings settings = Settings.EMPTY;
        UsageService usageService = new UsageService(settings);
        final RestController restController = new RestController(settings, Collections.emptySet(), null, null, null, usageService);
        final RestIndicesAction action = new RestIndicesAction(settings, restController, new IndexNameExpressionResolver(settings));

        // build a (semi-)random table
        final int numIndices = randomIntBetween(0, 5);
        Index[] indices = new Index[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = new Index(randomAlphaOfLength(5), UUIDs.randomBase64UUID());
        }

        final MetaData.Builder metaDataBuilder = MetaData.builder();
        for (final Index index : indices) {
            metaDataBuilder.put(IndexMetaData.builder(index.getName())
                                    .settings(Settings.builder()
                                                  .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                                  .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID()))
                                    .creationDate(System.currentTimeMillis())
                                    .numberOfShards(1)
                                    .numberOfReplicas(1)
                                    .state(IndexMetaData.State.OPEN));
        }
        final MetaData metaData = metaDataBuilder.build();

        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                                              .metaData(metaData)
                                              .build();
        final String[] indicesStr = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            indicesStr[i] = indices[i].getName();
        }
        final ClusterHealthResponse clusterHealth = new ClusterHealthResponse(
            clusterState.getClusterName().value(), indicesStr, clusterState, 0, 0, 0, TimeValue.timeValueMillis(1000L)
        );

        final Table table = action.buildTable(new FakeRestRequest(), indices, clusterHealth, randomIndicesStatsResponse(indices), metaData);

        // now, verify the table is correct
        int count = 0;
        List<Table.Cell> headers = table.getHeaders();
        assertThat(headers.get(count++).value, equalTo("health"));
        assertThat(headers.get(count++).value, equalTo("status"));
        assertThat(headers.get(count++).value, equalTo("index"));
        assertThat(headers.get(count++).value, equalTo("uuid"));

        List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(indices.length));
        // TODO: more to verify (e.g. randomize cluster health, num primaries, num replicas, etc)
        for (int i = 0; i < rows.size(); i++) {
            count = 0;
            final List<Table.Cell> row = rows.get(i);
            assertThat(row.get(count++).value, equalTo("red*")); // all are red because cluster state doesn't have routing entries
            assertThat(row.get(count++).value, equalTo("open")); // all are OPEN for now
            assertThat(row.get(count++).value, equalTo(indices[i].getName()));
            assertThat(row.get(count++).value, equalTo(indices[i].getUUID()));
        }
    }

    private IndicesStatsResponse randomIndicesStatsResponse(final Index[] indices) {
        List<ShardStats> shardStats = new ArrayList<>();
        for (final Index index : indices) {
            int numShards = randomInt(5);
            int primaryIdx = randomIntBetween(-1, numShards - 1); // -1 means there is no primary shard.
            for (int i = 0; i < numShards; i++) {
                ShardId shardId = new ShardId(index, i);
                boolean primary = (i == primaryIdx);
                Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(i));
                ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, primary,
                    primary ? StoreRecoverySource.EMPTY_STORE_INSTANCE : PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                    );
                shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                shardRouting = shardRouting.moveToStarted();
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats();
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();
                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null));
            }
        }
        return IndicesStatsTests.newIndicesStatsResponse(
            shardStats.toArray(new ShardStats[shardStats.size()]), shardStats.size(), shardStats.size(), 0, emptyList()
        );
    }
}
