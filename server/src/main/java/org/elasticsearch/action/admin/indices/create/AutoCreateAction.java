/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Proxy action for auto creating an indexable resource.
 * Currently only auto creates indices by redirecting to {@link AutoCreateIndexAction}.
 */
public final class AutoCreateAction extends ActionType<AutoCreateAction.Response> {

    public static final AutoCreateAction INSTANCE = new AutoCreateAction();
    public static final String NAME = "indices:admin/auto_create";

    private AutoCreateAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest {

        private final Set<String> names;
        private final String cause;
        private final Boolean preferV2Templates;

        public Request(Set<String> names, String cause, Boolean preferV2Templates) {
            this.names = Objects.requireNonNull(names);
            this.cause = Objects.requireNonNull(cause);
            this.preferV2Templates = preferV2Templates;
            assert names.size() != 0;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readSet(StreamInput::readString);
            this.cause = in.readString();
            this.preferV2Templates = in.readOptionalBoolean();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(names);
            out.writeString(cause);
            out.writeOptionalBoolean(preferV2Templates);
        }

        @Override
        public String[] indices() {
            return names.toArray(new String[0]);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        public Set<String> getNames() {
            return names;
        }

        public String getCause() {
            return cause;
        }

        public Boolean getPreferV2Templates() {
            return preferV2Templates;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return names.equals(request.names) &&
                Objects.equals(preferV2Templates, request.preferV2Templates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(names, preferV2Templates);
        }
    }

    public static class Response extends ActionResponse {

        private final Map<String, Exception> failureByNames;

        public Response(Map<String, Exception> failureByNames) {
            this.failureByNames = failureByNames;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            failureByNames = in.readMap(StreamInput::readString, StreamInput::readException);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(failureByNames, StreamOutput::writeString, StreamOutput::writeException);
        }

        public Map<String, Exception> getFailureByNames() {
            return failureByNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            /*
             * Exception does not implement equals(...) so we will compute the hash code based on the key set and the
             * messages.
             */
            return Objects.equals(getKeys(failureByNames), getKeys(response.failureByNames)) &&
                Objects.equals(getExceptionMessages(failureByNames), getExceptionMessages(response.failureByNames));
        }

        @Override
        public int hashCode() {
            /*
             * Exception does not implement hash code so we will compute the hash code based on the key set and the
             * messages.
             */
            return Objects.hash(getKeys(failureByNames), getExceptionMessages(failureByNames));
        }

        private static List<String> getExceptionMessages(final Map<String, Exception> map) {
            return map.values().stream().map(Throwable::getMessage).sorted(String::compareTo).collect(Collectors.toList());
        }

        private static List<String> getKeys(final Map<String, Exception> map) {
            return map.keySet().stream().sorted(String::compareTo).collect(Collectors.toList());
        }
    }

    public static final class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final Client client;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.client = client;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(Task task,
                                       Request request,
                                       ClusterState state,
                                       ActionListener<Response> listener) throws Exception {
            // For now always redirect to the auto create index action, because only indices get auto created.
            client.execute(AutoCreateIndexAction.INSTANCE, request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.names.toArray(new String[0]));
        }
    }

}
