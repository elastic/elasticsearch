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
package org.elasticsearch.action.admin.indices.datastream;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetDataStreamAction extends ActionType<GetDataStreamAction.Response> {

    public static final GetDataStreamAction INSTANCE = new GetDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/get";

    private GetDataStreamAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final String name;

        public Request(String name) {
            this.name = name;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<DataStream> dataStreams;

        public Response(List<DataStream> dataStreams) {
            this.dataStreams = dataStreams;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readList(DataStream::new));
        }

        public List<DataStream> getDataStreams() {
            return dataStreams;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(dataStreams);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (DataStream dataStream : dataStreams) {
                dataStream.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return dataStreams.equals(response.dataStreams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreams);
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
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
        protected void masterOperation(Task task, Request request, ClusterState state,
                                       ActionListener<Response> listener) throws Exception {
            listener.onResponse(new Response(getDataStreams(state, request)));
        }

        static List<DataStream> getDataStreams(ClusterState clusterState, Request request) {
            Map<String, DataStream> dataStreams = clusterState.metadata().dataStreams();

            // return all data streams if no name was specified
            final String requestedName = request.name == null ? "*" : request.name;

            final List<DataStream> results = new ArrayList<>();
                if (Regex.isSimpleMatchPattern(requestedName)) {
                    for (Map.Entry<String, DataStream> entry : dataStreams.entrySet()) {
                        if (Regex.simpleMatch(requestedName, entry.getKey())) {
                            results.add(entry.getValue());
                        }
                    }
                } else if (dataStreams.containsKey(request.name)) {
                    results.add(dataStreams.get(request.name));
                } else {
                    throw new ResourceNotFoundException("data_stream matching [" + request.name + "] not found");
                }
            results.sort(Comparator.comparing(DataStream::getName));
            return results;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
