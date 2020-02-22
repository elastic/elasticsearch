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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
public class PainlessContextAction extends ActionType<PainlessContextAction.Response> {

    public static final PainlessContextAction INSTANCE = new PainlessContextAction();
    private static final String NAME = "cluster:admin/scripts/painless/context";

    private static final String SCRIPT_CONTEXT_NAME_PARAM = "context";

    private PainlessContextAction() {
        super(NAME, PainlessContextAction.Response::new);
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
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(scriptContextName);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField CONTEXTS = new ParseField("contexts");

        private final List<String> scriptContextNames;
        private final PainlessContextInfo painlessContextInfo;

        public Response(List<String> scriptContextNames, PainlessContextInfo painlessContextInfo) {
            Objects.requireNonNull(scriptContextNames);
            scriptContextNames = new ArrayList<>(scriptContextNames);
            scriptContextNames.sort(String::compareTo);
            this.scriptContextNames = Collections.unmodifiableList(scriptContextNames);
            this.painlessContextInfo = painlessContextInfo;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            scriptContextNames = in.readStringList();
            painlessContextInfo = in.readOptionalWriteable(PainlessContextInfo::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(scriptContextNames);
            out.writeOptionalWriteable(painlessContextInfo);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (painlessContextInfo == null) {
                builder.startObject();
                builder.field(CONTEXTS.getPreferredName(), scriptContextNames);
                builder.endObject();
            } else {
                painlessContextInfo.toXContent(builder, params);
            }

            return builder;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final PainlessScriptEngine painlessScriptEngine;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, PainlessScriptEngine painlessScriptEngine) {
            super(NAME, transportService, actionFilters, (Writeable.Reader<Request>)Request::new);
            this.painlessScriptEngine = painlessScriptEngine;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            List<String> scriptContextNames;
            PainlessContextInfo painlessContextInfo;

            if (request.scriptContextName == null) {
                scriptContextNames =
                        painlessScriptEngine.getContextsToLookups().keySet().stream().map(v -> v.name).collect(Collectors.toList());
                painlessContextInfo = null;
            } else {
                ScriptContext<?> scriptContext = null;
                PainlessLookup painlessLookup = null;

                for (Map.Entry<ScriptContext<?>, PainlessLookup> contextLookupEntry :
                        painlessScriptEngine.getContextsToLookups().entrySet()) {
                    if (contextLookupEntry.getKey().name.equals(request.getScriptContextName())) {
                        scriptContext = contextLookupEntry.getKey();
                        painlessLookup = contextLookupEntry.getValue();
                        break;
                    }
                }

                if (scriptContext == null || painlessLookup == null) {
                    throw new IllegalArgumentException("script context [" + request.getScriptContextName() + "] not found");
                }

                scriptContextNames = Collections.emptyList();
                painlessContextInfo = new PainlessContextInfo(scriptContext, painlessLookup);
            }

            listener.onResponse(new Response(scriptContextNames, painlessContextInfo));
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new Route(GET, "/_scripts/painless/_context"));
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
