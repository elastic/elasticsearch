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

package org.elasticsearch.action.admin.indices.fieldcaps;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TransportFieldCapabilitiesIndexAction
    extends TransportSingleShardAction<FieldCapabilitiesIndexRequest, FieldCapabilitiesResponse> {

    private static final String ACTION_NAME = FieldCapabilitiesAction.NAME + "[index]";

    protected final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportFieldCapabilitiesIndexAction(Settings settings, ClusterService clusterService,
                                                 TransportService transportService, IndicesService indicesService,
                                                 ThreadPool threadPool, ActionFilters actionFilters,
                                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, FieldCapabilitiesIndexRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(FieldCapabilitiesIndexRequest request) {
        //internal action, index already resolved
        return false;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // Will balance requests between shards
        // Resolve patterns and deduplicate
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected FieldCapabilitiesResponse shardOperation(final FieldCapabilitiesIndexRequest request, ShardId shardId) {
        MapperService mapperService = indicesService.indexService(shardId.getIndex()).mapperService();
        Set<String> fieldNames = new HashSet<>();
        for (String field : request.fields()) {
            fieldNames.addAll(mapperService.simpleMatchToIndexNames(field));
        }
        Map<String, Map<String, FieldCapabilities> > fieldCaps = new HashMap<>();
        for (String field : fieldNames) {
            MappedFieldType ft = mapperService.fullName(field);
            FieldCapabilities fieldCap =
                new FieldCapabilities(field, ft.isSearchable(), ft.isAggregatable(), ft.typeName());
            fieldCaps.put(field, Collections.singletonMap(shardId.getIndex().getName(), fieldCap));
        }
        return new FieldCapabilitiesResponse(fieldCaps);
    }

    @Override
    protected FieldCapabilitiesResponse newResponse() {
        return new FieldCapabilitiesResponse();
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }
}
