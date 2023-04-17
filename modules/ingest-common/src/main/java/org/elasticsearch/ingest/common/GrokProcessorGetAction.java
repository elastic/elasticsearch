/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.PatternBank;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.grok.GrokBuiltinPatterns.ECS_COMPATIBILITY_DISABLED;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class GrokProcessorGetAction extends ActionType<GrokProcessorGetAction.Response> {

    static final GrokProcessorGetAction INSTANCE = new GrokProcessorGetAction();
    static final String NAME = "cluster:admin/ingest/processor/grok/get";

    private GrokProcessorGetAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final boolean sorted;
        private final String ecsCompatibility;

        public Request(boolean sorted, String ecsCompatibility) {
            this.sorted = sorted;
            this.ecsCompatibility = ecsCompatibility;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.sorted = in.readBoolean();
            this.ecsCompatibility = in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)
                ? in.readString()
                : GrokProcessor.DEFAULT_ECS_COMPATIBILITY_MODE;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(sorted);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
                out.writeString(ecsCompatibility);
            }
        }

        public boolean sorted() {
            return sorted;
        }

        public String getEcsCompatibility() {
            return ecsCompatibility;
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

        private final Map<String, String> legacyGrokPatterns;
        private final Map<String, String> sortedLegacyGrokPatterns;
        private final Map<String, String> ecsV1GrokPatterns;
        private final Map<String, String> sortedEcsV1GrokPatterns;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters) {
            this(transportService, actionFilters, GrokBuiltinPatterns.legacyPatterns(), GrokBuiltinPatterns.ecsV1Patterns());
        }

        // visible for testing
        TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            PatternBank legacyGrokPatterns,
            PatternBank ecsV1GrokPatterns
        ) {
            super(NAME, transportService, actionFilters, Request::new);
            this.legacyGrokPatterns = legacyGrokPatterns.bank();
            this.sortedLegacyGrokPatterns = new TreeMap<>(this.legacyGrokPatterns);
            this.ecsV1GrokPatterns = ecsV1GrokPatterns.bank();
            this.sortedEcsV1GrokPatterns = new TreeMap<>(this.ecsV1GrokPatterns);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ActionListener.completeWith(
                listener,
                () -> new Response(
                    request.getEcsCompatibility().equals(ECS_COMPATIBILITY_DISABLED)
                        ? request.sorted() ? sortedLegacyGrokPatterns : legacyGrokPatterns
                        : request.sorted() ? sortedEcsV1GrokPatterns
                        : ecsV1GrokPatterns
                )
            );
        }
    }

    @ServerlessScope(Scope.PUBLIC)
    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new Route(GET, "/_ingest/processor/grok"));
        }

        @Override
        public String getName() {
            return "ingest_processor_grok_get";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            boolean sorted = request.paramAsBoolean("s", false);
            String ecsCompatibility = request.param("ecs_compatibility", GrokProcessor.DEFAULT_ECS_COMPATIBILITY_MODE);
            if (GrokBuiltinPatterns.isValidEcsCompatibilityMode(ecsCompatibility) == false) {
                throw new IllegalArgumentException("unsupported ECS compatibility mode [" + ecsCompatibility + "]");
            }
            Request grokPatternsRequest = new Request(sorted, ecsCompatibility);
            return channel -> client.executeLocally(INSTANCE, grokPatternsRequest, new RestToXContentListener<>(channel));
        }
    }
}
