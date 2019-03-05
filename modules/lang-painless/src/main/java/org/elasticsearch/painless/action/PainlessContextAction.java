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

package org.elasticsearch.painless.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Internal REST API for querying context information about Painless whitelists.
 * Commands include the following:
 * <ul>
 *     <li> GET /_scripts/painless/_context -- retrieves a list of contexts </li>
 *     <li> GET /_scripts/painless/_context?context=%name% --
 *     retrieves all available information about the API for this specific context</li>
 * </ul>
 */
public class PainlessContextAction extends Action<PainlessContextAction.Response> {

    public static final PainlessContextAction INSTANCE = new PainlessContextAction();
    private static final String NAME = "cluster:admin/scripts/painless/context";

    private static final String SCRIPT_CONTEXT_NAME_PARAM = "context";

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

        private String scriptContextName;

        public Request() {
            scriptContextName = null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            scriptContextName = in.readString();
        }

        public void setScriptContextName(String scriptContextName) {
            this.scriptContextName = scriptContextName;
        }

        public String getScriptContextName() {
            return scriptContextName;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(scriptContextName);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField CONTEXTS = new ParseField("contexts");

        private final Map<String, PainlessContextInfo> scriptContextNamesToPainlessContextInfos;
        private final String scriptContextName;

        public Response(Map<String, PainlessContextInfo> scriptContextNamesToPainlessContextInfos, String scriptContextName) {
            this.scriptContextNamesToPainlessContextInfos = Collections.unmodifiableMap(scriptContextNamesToPainlessContextInfos);
            this.scriptContextName = scriptContextName;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            scriptContextNamesToPainlessContextInfos =
                    Collections.unmodifiableMap(in.readMap(StreamInput::readString, PainlessContextInfo::new));
            scriptContextName = in.readString();
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(scriptContextNamesToPainlessContextInfos, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            out.writeString(scriptContextName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (scriptContextName == null) {
                builder.startObject();
                builder.field(CONTEXTS.getPreferredName(), scriptContextNamesToPainlessContextInfos.keySet());
                builder.endObject();
            } else {
                scriptContextNamesToPainlessContextInfos.get(scriptContextName).toXContent(builder, params);
            }

            return builder;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        PainlessScriptEngine painlessScriptEngine;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, PainlessScriptEngine painlessScriptEngine) {
            super(NAME, transportService, actionFilters, (Writeable.Reader<Request>)Request::new);
            this.painlessScriptEngine = painlessScriptEngine;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            Map<String, PainlessContextInfo> scriptContextNamesToPainlessContextInfos = new HashMap<>();
            String scriptContextName = request.getScriptContextName();

            if (request.scriptContextName == null) {
                for (ScriptContext<?> scriptContext : painlessScriptEngine.getContextsToLookups().keySet()) {
                    scriptContextNamesToPainlessContextInfos.put(scriptContext.name, null);
                }
            } else {
                ScriptContext<?> scriptContext = null;
                PainlessLookup painlessLookup = null;

                for (Map.Entry<ScriptContext<?>, PainlessLookup> contextLookupEntry :
                        painlessScriptEngine.getContextsToLookups().entrySet()) {
                    if (contextLookupEntry.getKey().name.equals(scriptContextName)) {
                        scriptContext = contextLookupEntry.getKey();
                        painlessLookup = contextLookupEntry.getValue();
                        break;
                    }
                }

                if (scriptContext == null || painlessLookup == null) {
                    throw new IllegalArgumentException("script context [" + scriptContextName + "] not found");
                }

                scriptContextNamesToPainlessContextInfos.put(scriptContext.name, new PainlessContextInfo(scriptContext, painlessLookup));
            }

            listener.onResponse(new Response(scriptContextNamesToPainlessContextInfos, scriptContextName));
        }
    }

    public static class RestAction extends BaseRestHandler {

        public RestAction(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(GET, "/_scripts/painless/_context", this);
        }

        @Override
        public String getName() {
            return "_scripts_painless_context";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
            Request request = new Request();
            request.setScriptContextName(restRequest.param(SCRIPT_CONTEXT_NAME_PARAM));
            return channel -> client.executeLocally(INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }
}
