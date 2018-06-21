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
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class PainlessExecuteAction extends Action<PainlessExecuteAction.Response> {

    static final PainlessExecuteAction INSTANCE = new PainlessExecuteAction();
    private static final String NAME = "cluster:admin/scripts/painless/execute";

    private PainlessExecuteAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {

        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ParseField CONTEXT_FIELD = new ParseField("context");
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "painless_execute_request", args -> new Request((Script) args[0], (SupportedContext) args[1]));

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                // For now only accept an empty json object:
                XContentParser.Token token = p.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String contextType = p.currentName();
                token = p.nextToken();
                assert token == XContentParser.Token.START_OBJECT;
                token = p.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                token = p.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return SupportedContext.valueOf(contextType.toUpperCase(Locale.ROOT));
            }, CONTEXT_FIELD);
        }

        private Script script;
        private SupportedContext context;

        static Request parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        Request(Script script, SupportedContext context) {
            this.script = Objects.requireNonNull(script);
            this.context = context != null ? context : SupportedContext.PAINLESS_TEST;
        }

        Request() {
        }

        public Script getScript() {
            return script;
        }

        public SupportedContext getContext() {
            return context;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (script.getType() != ScriptType.INLINE) {
                validationException = addValidationError("only inline scripts are supported", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            script = new Script(in);
            context = SupportedContext.fromId(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            script.writeTo(out);
            out.writeByte(context.id);
        }

        // For testing only:
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.startObject(CONTEXT_FIELD.getPreferredName());
            {
                builder.startObject(context.name());
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(script, request.script) &&
                context == request.context;
        }

        @Override
        public int hashCode() {
            return Objects.hash(script, context);
        }

        public enum SupportedContext {

            PAINLESS_TEST((byte) 0);

            private final byte id;

            SupportedContext(byte id) {
                this.id = id;
            }

            public static SupportedContext fromId(byte id) {
                switch (id) {
                    case 0:
                        return PAINLESS_TEST;
                    default:
                        throw new IllegalArgumentException("unknown context [" + id + "]");
                }
            }
        }

    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Object result;

        Response() {}

        Response(Object result) {
            this.result = result;
        }

        public Object getResult() {
            return result;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            result = in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeGenericValue(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", result);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(result, response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }

    public abstract static class PainlessTestScript {

        private final Map<String, Object> params;

        public PainlessTestScript(Map<String, Object> params) {
            this.params = params;
        }

        /** Return the parameters for this script. */
        public Map<String, Object> getParams() {
            return params;
        }

        public abstract Object execute();

        public interface Factory {

            PainlessTestScript newInstance(Map<String, Object> params);

        }

        public static final String[] PARAMETERS = {};
        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("painless_test", Factory.class);

    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {


        private final ScriptService scriptService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService,
                               ActionFilters actionFilters, ScriptService scriptService) {
            super(settings, NAME, transportService, actionFilters, Request::new);
            this.scriptService = scriptService;
        }
        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            switch (request.context) {
                case PAINLESS_TEST:
                    PainlessTestScript.Factory factory = scriptService.compile(request.script, PainlessTestScript.CONTEXT);
                    PainlessTestScript painlessTestScript = factory.newInstance(request.script.getParams());
                    String result = Objects.toString(painlessTestScript.execute());
                    listener.onResponse(new Response(result));
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported context [" + request.context + "]");
            }
        }

    }

    static class RestAction extends BaseRestHandler {

        RestAction(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(GET, "/_scripts/painless/_execute", this);
            controller.registerHandler(POST, "/_scripts/painless/_execute", this);
        }

        @Override
        public String getName() {
            return "_scripts_painless_execute";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = Request.parse(restRequest.contentOrSourceParamParser());
            return channel -> client.executeLocally(INSTANCE, request, new RestBuilderListener<Response>(channel) {
                @Override
                public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(OK, builder);
                }
            });
        }
    }

}
