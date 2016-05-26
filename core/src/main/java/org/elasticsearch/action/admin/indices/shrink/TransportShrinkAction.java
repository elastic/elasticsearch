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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getShrinkIndexReqeust().index());
    }

    @Override
    protected void masterOperation(final ShrinkRequest shrinkReqeust, final ClusterState state,
                                   final ActionListener<ShrinkResponse> listener) {
        final String sourceIndex = indexNameExpressionResolver.resolveDateMathExpression(shrinkReqeust.getSourceIndex());
        client.admin().indices().prepareStats(sourceIndex).clear().setDocs(true).execute(new ActionListener<IndicesStatsResponse>() {
            @Override
            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(shrinkReqeust, state,
                    indicesStatsResponse.getTotal().getDocs(), indexNameExpressionResolver);
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
        , final DocsStats docsStats, IndexNameExpressionResolver indexNameExpressionResolver) {
        final String sourceIndex = indexNameExpressionResolver.resolveDateMathExpression(shrinkReqeust.getSourceIndex());
        final CreateIndexRequest targetIndex = shrinkReqeust.getShrinkIndexReqeust();
        final String targetIndexName = indexNameExpressionResolver.resolveDateMathExpression(targetIndex.index());
        final IndexMetaData metaData = state.metaData().index(sourceIndex);
        if (metaData == null) {
            throw new IndexNotFoundException(sourceIndex);
        }
        // ensure index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException("index " + sourceIndex + " must be read-only to shrink index. use \"index.blocks.write=true\"");
        }
        if (state.metaData().hasIndex(targetIndexName)) {
            throw new IndexAlreadyExistsException(state.metaData().index(targetIndexName).getIndex());
        }
        if (metaData.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }
        long count = docsStats.getCount();
        if (count >= IndexWriter.MAX_DOCS) {
            throw new IllegalStateException("Can't merge index with more than [" + IndexWriter.MAX_DOCS
                + "] docs -  too many documents");
        }
        if (targetIndex.mappings().isEmpty() == false) {
            throw new IllegalArgumentException("mappings are not allowed when shrinking indices" +
                ", all mappings are copied from the source index");
        }
        final Settings tagetIndexSettings = Settings.builder().put(targetIndex.settings())
            .normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
        if (IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.exists(tagetIndexSettings)
            && IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(tagetIndexSettings) > 1) {
            throw new IllegalArgumentException("can not shrink index into more than one shard");
        }
        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndex);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = metaData.getNumberOfShards();
        for (ShardRouting routing : table.shardsWithState(ShardRoutingState.STARTED)) {
            nodesToNumRouting.computeIfAbsent(routing.currentNodeId(), (s) -> new AtomicInteger(0)).incrementAndGet();
        }
        List<String> nodesToAllocateOn = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entries : nodesToNumRouting.entrySet()) {
            int numAllocations = entries.getValue().get();
            assert numAllocations <= numShards : "wait what? " + numAllocations + " is > than num shards " + numShards;
            if (numAllocations == numShards) {
                nodesToAllocateOn.add(entries.getKey());
            }
        }
        if (nodesToAllocateOn.isEmpty()) {
            throw new IllegalStateException("index " + sourceIndex +
                " must have all shards allocated on the same node to shrink index");
        }
        targetIndex.cause("shrink_index");

        targetIndex.settings(Settings.builder()
            .put(tagetIndexSettings)
            // we can only shrink to 1 index so far!
            .put("index.number_of_shards", 1)
            // we set default to 0 only if there is nothing explicitly set
            .put("index.number_of_replicas", IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(tagetIndexSettings) ?
                IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(tagetIndexSettings) : 0)
            .put("index.routing.allocation.require._id", Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()))
            // now copy all similarity / analysis settings
            .put(metaData.getSettings().filter((s) -> s.startsWith("index.similarity.") || s.startsWith("index.analysis.")))
        );

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
