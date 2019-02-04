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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
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
    public Response newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class Request extends ActionRequest {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public Response(StreamInput input) {

        }

        public Response() {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("test", "HERE");
            builder.endObject();

            return builder;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        PainlessScriptEngine painlessScriptEngine;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, PainlessScriptEngine painlessScriptEngine) {
            super(NAME, transportService, actionFilters, Request::new);
            this.painlessScriptEngine = painlessScriptEngine;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            listener.onResponse(new Response());
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
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
            return channel -> client.executeLocally(INSTANCE, new Request(), new RestToXContentListener<>(channel));
        }
    }
}
