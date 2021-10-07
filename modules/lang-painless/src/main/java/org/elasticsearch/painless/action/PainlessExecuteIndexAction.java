/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.painless.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * A concrete implementation of {@link AbstractPainlessExecuteAction} that executes
 * scripts that require an index context. Unlike the {@link PainlessExecuteAction},
 * this action doesn't require any cluster privilege to run, only the read privilege on
 * the provided index is needed.
 */
public class PainlessExecuteIndexAction extends AbstractPainlessExecuteAction {

    public static final PainlessExecuteIndexAction INSTANCE = new PainlessExecuteIndexAction();
    private static final String NAME = "indices:data/read/scripts/painless/execute";

    private PainlessExecuteIndexAction() {
        super(NAME);
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final ScriptService scriptService;
        private final IndicesService indicesServices;

        @Inject
        public TransportAction(ThreadPool threadPool, TransportService transportService,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    ScriptService scriptService, ClusterService clusterService, IndicesService indicesServices) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                // Forking a thread here, because only light weight operations should happen on network thread and
                // Creating a in-memory index is not light weight
                // TODO: is MANAGEMENT TP the right TP? Right now this is an admin api (see action name).
                Request::new, ThreadPool.Names.MANAGEMENT);
            this.scriptService = scriptService;
            this.indicesServices = indicesServices;
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
        }

        @Override
        protected void resolveRequest(ClusterState state, InternalRequest internalRequest) {
            Request request = internalRequest.request();
            ScriptContext<?> scriptContext = request.getContext();
            if (needDocumentAndIndex(scriptContext) == false) {
                throw new IllegalArgumentException("[" + scriptContext.name + "] is not supported when an index is provided.");
            }
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            ClusterState clusterState = clusterService.state();
            IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();
            String indexExpression = request.index();
            Index[] concreteIndices =
                indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions, indexExpression);
            if (concreteIndices.length != 1) {
                throw new IllegalArgumentException("[" + indexExpression + "] does not resolve to a single index");
            }
            Index concreteIndex = concreteIndices[0];
            IndexService indexService = indicesServices.indexServiceSafe(concreteIndex);
            return innerShardOperation(request, scriptService, indexService);
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(
                new Route(GET, "{index}/_scripts/painless/_execute"),
                new Route(POST, "{index}/_scripts/painless/_execute"));
        }

        @Override
        public String getName() {
            return "_scripts_painless_index_execute";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = Request.parse(restRequest.contentOrSourceParamParser());
            request.index(restRequest.param("index"));
            return channel -> client.executeLocally(INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }
}
