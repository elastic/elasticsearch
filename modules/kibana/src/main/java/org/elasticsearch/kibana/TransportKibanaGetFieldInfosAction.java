/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;

/**
 * Reads {@link FieldInfos} field names from a shard of a Kibana system index. Routed to any active copy of shard 0;
 * field infos are identical across all copies because segments are never replaced on these indices.
 */
public class TransportKibanaGetFieldInfosAction extends TransportSingleShardAction<
    KibanaGetFieldInfosAction.Request,
    KibanaGetFieldInfosAction.Response> {

    private final IndicesService indicesService;

    @Inject
    public TransportKibanaGetFieldInfosAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService
    ) {
        super(
            KibanaGetFieldInfosAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            projectResolver,
            indexNameExpressionResolver,
            KibanaGetFieldInfosAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indicesService = indicesService;
    }

    @Override
    protected Writeable.Reader<KibanaGetFieldInfosAction.Response> getResponseReader() {
        return KibanaGetFieldInfosAction.Response::new;
    }

    @Override
    protected boolean resolveIndex(KibanaGetFieldInfosAction.Request request) {
        return true;
    }

    @Override
    protected ShardsIterator shards(ProjectState state, InternalRequest request) {
        return state.routingTable().index(request.concreteIndex()).shard(0).activeInitializingShardsRandomIt();
    }

    @Override
    protected KibanaGetFieldInfosAction.Response shardOperation(KibanaGetFieldInfosAction.Request request, ShardId shardId) {
        IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        FieldInfos fieldInfos = shard.getFieldInfos();
        Set<String> names = new HashSet<>(fieldInfos.size());
        for (FieldInfo fi : fieldInfos) {
            names.add(fi.name);
        }
        return new KibanaGetFieldInfosAction.Response(names);
    }
}
