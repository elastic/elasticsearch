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
package org.elasticsearch.ingest.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.ingest.common.IngestCommonPlugin.GROK_PATTERNS;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class GrokProcessorGetAction extends ActionType<GrokProcessorGetAction.Response> {

    static final GrokProcessorGetAction INSTANCE = new GrokProcessorGetAction();
    static final String NAME = "cluster:admin/ingest/processor/grok/get";

    private GrokProcessorGetAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final Map<String, String> grokPatterns;

        Response(Map<String, String> grokPatterns) {
            this.grokPatterns = grokPatterns;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            grokPatterns = in.readMap(StreamInput::readString, StreamInput::readString);
        }

        public Map<String, String> getGrokPatterns() {
            return grokPatterns;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("patterns");
            builder.map(grokPatterns);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(grokPatterns, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters) {
            super(NAME, transportService, actionFilters, Request::new);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            try {
                listener.onResponse(new Response(GROK_PATTERNS));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return singletonList(new Route(GET, "/_ingest/processor/grok"));
        }

        @Override
        public String getName() {
            return "ingest_processor_grok_get";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            return channel -> client.executeLocally(INSTANCE, new Request(), new RestToXContentListener<>(channel));
        }
    }
}
