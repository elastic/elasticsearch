/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.analysis.catalog.IndexResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SqlGetIndicesAction
        extends Action<SqlGetIndicesAction.Request, SqlGetIndicesAction.Response, SqlGetIndicesAction.RequestBuilder> {
    public static final SqlGetIndicesAction INSTANCE = new SqlGetIndicesAction();
    public static final String NAME = "indices:data/read/sql/indices";

    private SqlGetIndicesAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {
        private IndicesOptions indicesOptions;
        private String[] indices;

        /**
         * Deserialization and builder ctor.
         */
        Request() {}

        /**
         * Sensible ctor.
         */
        public Request(IndicesOptions indicesOptions, String... indices) {
            this.indicesOptions = indicesOptions;
            this.indices = indices;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            indicesOptions = IndicesOptions.readIndicesOptions(in);
            indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indicesOptions.writeIndicesOptions(out);
            out.writeStringArray(indices);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public Request indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.equals(indices, other.indices)
                    && indicesOptions.equals(other.indicesOptions)
                    && local == other.local
                    && masterNodeTimeout.equals(other.masterNodeTimeout)
                    && getParentTask().equals(other.getParentTask());
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), indicesOptions, local, masterNodeTimeout, getParentTask());
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {
        public RequestBuilder(ElasticsearchClient client, SqlGetIndicesAction action) {
            super(client, action, new Request());
        }

        RequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
            request.indicesOptions(indicesOptions);
            return this;
        }

        RequestBuilder setIndices(String... indices) {
            request.indices(indices);
            return this;
        }
    }

    public static class Response extends ActionResponse {
        private List<EsIndex> indices;

        /**
         * Deserialization ctor.
         */
        Response() {}

        public Response(List<EsIndex> indices) {
            this.indices = indices;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            throw new UnsupportedOperationException("Must be requested locally for now");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            throw new UnsupportedOperationException("Must be requested locally for now");
        }

        public List<EsIndex> indices() {
            return indices;
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {
        private final IndexResolver indexResolver;
        private final SqlLicenseChecker licenseChecker;

        @Inject
        public TransportAction(Settings settings, TransportService transportService,
                               ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver, SqlLicenseChecker licenseChecker,
                               IndexResolver indexResolver) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters,
                    Request::new, indexNameExpressionResolver);
            this.licenseChecker = licenseChecker;
            this.indexResolver = indexResolver;
        }

        @Override
        protected String executor() {
            // read operation, lightweight...
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) {
            licenseChecker.checkIfSqlAllowed();
            operation(indexResolver, request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                    indexNameExpressionResolver.concreteIndexNames(state, request));
        }
    }

    static void operation(IndexResolver indexResolver, Request request, ActionListener<Response> listener) {
        indexResolver.asList(ActionListener.wrap(results -> listener.onResponse(new Response(results)), listener::onFailure),
                request.indices);
    }
}