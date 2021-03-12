/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import org.elasticsearch.painless.action.PainlessSuggest.Suggestion;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Internal REST API for providing auto-complete suggestions for a
 * partially completed Painless script.
 * {@code
 * Request:
 *
 * GET _scripts/painless/suggestions
 * {
 *     "context": "score", // optional
 *     "script": {
 *         "source": "partially completed script", // required
 *     }
 * }
 *
 * Response:
 * {
 *     "suggestions": [
 *         {
 *             "type": "variable/method/field/type",
 *             "text": "suggested text"
 *         },
 *         ...
 *     ]
 * }
 * }
 */
public class PainlessSuggestAction extends ActionType<PainlessSuggestAction.Response> {

    public static final PainlessSuggestAction INSTANCE = new PainlessSuggestAction();
    private static final String NAME = "cluster:admin/scripts/painless/suggestions";

    private PainlessSuggestAction() {
        super(NAME, PainlessSuggestAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ParseField CONTEXT_FIELD = new ParseField("context");
        private static final ConstructingObjectParser<PainlessSuggestAction.Request, Void> PARSER = new ConstructingObjectParser<>(
                "painless_suggestions_request", args -> new PainlessSuggestAction.Request((String)args[0], (Script)args[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CONTEXT_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
        }

        static Request parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private final String context;
        private final Script script;

        public Request(String context, Script script) {
            this.context = context;
            this.script = Objects.requireNonNull(script);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.context = in.readOptionalString();
            this.script = new Script(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(this.context);
            this.script.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Script getScript() {
            return script;
        }

        public String getContext() {
            return context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request)o;
            return Objects.equals(context, request.context) && Objects.equals(script, request.script);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, script);
        }

        @Override
        public String toString() {
            return "Request{" +
                    "context='" + context + '\'' +
                    ", script=" + script +
                    '}';
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField SUGGESTIONS = new ParseField("suggestions");

        private final List<Suggestion> suggestions;

        public Response(List<Suggestion> suggestions) {
            this.suggestions = Collections.unmodifiableList(Objects.requireNonNull(suggestions));
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            suggestions = Collections.unmodifiableList(in.readList(Suggestion::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(suggestions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SUGGESTIONS.getPreferredName(), suggestions);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response)o;
            return Objects.equals(suggestions, response.suggestions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(suggestions);
        }

        @Override
        public String toString() {
            return "Response{" +
                    "suggestions=" + suggestions +
                    '}';
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final PainlessScriptEngine painlessScriptEngine;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, PainlessScriptEngine painlessScriptEngine) {
            super(NAME, transportService, actionFilters, Request::new);
            this.painlessScriptEngine = painlessScriptEngine;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            String source = request.getScript().getIdOrCode();
            String context = request.getContext();

            if (request.getContext() == null) {
                context = PainlessTestScript.CONTEXT.name;
            }

            Class<?> script = null;
            PainlessLookup lookup = null;

            for (Map.Entry<ScriptContext<?>, PainlessLookup> contextLookupEntry : painlessScriptEngine.getContextsToLookups().entrySet()) {
                if (contextLookupEntry.getKey().name.equals(context)) {
                    script = contextLookupEntry.getKey().instanceClazz;
                    lookup = contextLookupEntry.getValue();
                    break;
                }
            }

            if (script == null || lookup == null) {
                throw new IllegalArgumentException("script context [" + request.getContext() + "] not found");
            }

            ScriptClassInfo info = new ScriptClassInfo(lookup, script);
            List<Suggestion> suggestions = PainlessSuggest.suggest(lookup, info, source).stream()
                    .map(suggestion -> new Suggestion(suggestion.type, suggestion.text)).collect(Collectors.toList());

            listener.onResponse(new Response(suggestions));
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new Route(GET, "/_scripts/painless/_suggestions"));
        }

        @Override
        public String getName() {
            return "_scripts_painless_suggestions";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            Request request = Request.parse(restRequest.contentOrSourceParamParser());
            return channel -> client.executeLocally(INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }

    public static class Suggestion implements Writeable, ToXContentObject {

        private static final ParseField TYPE_FIELD = new ParseField("type");
        private static final ParseField TEXT_FIELD = new ParseField("text");

        private final String type;
        private final String text;

        public Suggestion(String type, String text) {
            this.type = type;
            this.text = text;
        }

        public Suggestion(StreamInput in) throws IOException {
            this.type = in.readString();
            this.text = in.readString();
        }

        public String getType() {
            return type;
        }

        public String getText() {
            return text;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.type);
            out.writeString(this.text);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(TEXT_FIELD.getPreferredName(), text);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Suggestion that = (Suggestion)o;
            return Objects.equals(type, that.type) && Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, text);
        }

        @Override
        public String toString() {
            return "Suggestion{" +
                    "type='" + type + '\'' +
                    ", text='" + text + '\'' +
                    '}';
        }
    }
}
