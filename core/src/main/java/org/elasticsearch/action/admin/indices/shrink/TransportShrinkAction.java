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

package org.elasticsearch.action.admin.indices.shrink;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.function.IntFunction;

/**
 * Main class to initiate shrinking an index into a new index with a single shard
 */
public class TransportShrinkAction extends TransportMasterNodeAction<ShrinkRequest, ShrinkResponse> {

    private final MetaDataCreateIndexService createIndexService;
    private final Client client;

    @Inject
    public TransportShrinkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                 ThreadPool threadPool, MetaDataCreateIndexService createIndexService,
                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        super(settings, ShrinkAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            ShrinkRequest::new);
        this.createIndexService = createIndexService;
        this.client = client;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ShrinkResponse newResponse() {
        return new ShrinkResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(ShrinkRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getShrinkIndexRequest().index());
    }

    @Override
    protected void masterOperation(final ShrinkRequest shrinkRequest, final ClusterState state,
                                   final ActionListener<ShrinkResponse> listener) {
        final String sourceIndex = indexNameExpressionResolver.resolveDateMathExpression(shrinkRequest.getSourceIndex());
        client.admin().indices().prepareStats(sourceIndex).clear().setDocs(true).execute(new ActionListener<IndicesStatsResponse>() {
            @Override
            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(shrinkRequest, state,
                    (i) -> {
                        IndexShardStats shard = indicesStatsResponse.getIndex(sourceIndex).getIndexShards().get(i);
                        return shard == null ? null : shard.getPrimary().getDocs();
                    }, indexNameExpressionResolver);
                createIndexService.createIndex(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
                    @Override
                    public void onResponse(ClusterStateUpdateResponse response) {
                        listener.onResponse(new ShrinkResponse(response.isAcknowledged()));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (t instanceof IndexAlreadyExistsException) {
                            logger.trace("[{}] failed to create shrink index", t, updateRequest.index());
                        } else {
                            logger.debug("[{}] failed to create shrink index", t, updateRequest.index());
                        }
                        listener.onFailure(t);
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });

    }

    // static for unittesting this method
    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final ShrinkRequest shrinkReqeust, final ClusterState state
        , final IntFunction<DocsStats> perShardDocStats, IndexNameExpressionResolver indexNameExpressionResolver) {
        final String sourceIndex = indexNameExpressionResolver.resolveDateMathExpression(shrinkReqeust.getSourceIndex());
        final CreateIndexRequest targetIndex = shrinkReqeust.getShrinkIndexRequest();
        final String targetIndexName = indexNameExpressionResolver.resolveDateMathExpression(targetIndex.index());
        final IndexMetaData metaData = state.metaData().index(sourceIndex);
        final Settings targetIndexSettings = Settings.builder().put(targetIndex.settings())
            .normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
        int numShards = 1;
        if (IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            numShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings);
        }
        for (int i = 0; i < numShards; i++) {
            Set<ShardId> shardIds = IndexMetaData.selectShrinkShards(i, metaData, numShards);
            long count = 0;
            for (ShardId id : shardIds) {
                DocsStats docsStats = perShardDocStats.apply(id.id());
                if (docsStats != null) {
                    count += docsStats.getCount();
                }
                if (count > IndexWriter.MAX_DOCS) {
                    throw new IllegalStateException("Can't merge index with more than [" + IndexWriter.MAX_DOCS
                        + "] docs - too many documents in shards " + shardIds);
                }
            }

        }
        targetIndex.cause("shrink_index");
        Settings.Builder settingsBuilder = Settings.builder().put(targetIndexSettings);
        settingsBuilder.put("index.number_of_shards", numShards);
        targetIndex.settings(settingsBuilder);

        return new CreateIndexClusterStateUpdateRequest(targetIndex,
            "shrink_index", targetIndexName, true)
            // mappings are updated on the node when merging in the shards, this prevents race-conditions since all mapping must be
            // applied once we took the snapshot and if somebody fucks things up and switches the index read/write and adds docs we miss
            // the mappings for everything is corrupted and hard to debug
            .ackTimeout(targetIndex.timeout())
            .masterNodeTimeout(targetIndex.masterNodeTimeout())
            .settings(targetIndex.settings())
            .aliases(targetIndex.aliases())
            .customs(targetIndex.customs())
            .shrinkFrom(metaData.getIndex());
    }

}
