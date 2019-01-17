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

package org.elasticsearch.painless;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class PainlessContextAction extends Action<PainlessContextAction.Response> {

    static final PainlessContextAction INSTANCE = new PainlessContextAction();
    private static final String NAME = "cluster:admin/scripts/painless/context";

    private PainlessContextAction() {
        super(NAME);
    }

    @Override
    public PainlessContextAction.Response newResponse() {
        return new PainlessContextAction.Response();
    }

    public static class Request extends SingleShardRequest<PainlessContextAction.Request> {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("test", "HERE");
            builder.endObject();

            return builder;
        }
    }

    public static class TransportAction extends TransportSingleShardAction<PainlessContextAction.Request, PainlessContextAction.Response> {

        PainlessScriptEngine painlessScriptEngine;

        @Inject
        public TransportAction(ThreadPool threadPool, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, PainlessScriptEngine painlessScriptEngine) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                    PainlessContextAction.Request::new, ThreadPool.Names.MANAGEMENT);
            this.painlessScriptEngine = painlessScriptEngine;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            return new Response();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return null;
        }
    }

    static class RestAction extends BaseRestHandler {

        RestAction(Settings settings, RestController controller, PainlessScriptEngine painlessScriptEngine) {
            super(settings);
            controller.registerHandler(GET, "/_scripts/painless/_context", this);
        }

        @Override
        public String getName() {
            return "_scripts_painless_context";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = new Request();
            return channel -> client.executeLocally(INSTANCE, request, new RestBuilderListener<PainlessContextAction.Response>(channel) {
                @Override
                public RestResponse buildResponse(PainlessContextAction.Response response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(OK, builder);
                }
            });
        }
    }
}
