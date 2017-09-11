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
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog.GetIndexResult;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Comparator.comparing;

public class SqlGetIndicesAction
        extends Action<SqlGetIndicesAction.Request, SqlGetIndicesAction.Response, SqlGetIndicesAction.RequestBuilder> {
    public static final SqlGetIndicesAction INSTANCE = new SqlGetIndicesAction();
    public static final String NAME = "indices:data/read/sql/tables";

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

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
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
        private final Function<ClusterState, Catalog> catalogSupplier;
        private final SqlLicenseChecker licenseChecker;

        @Inject
        public TransportAction(Settings settings, TransportService transportService,
                ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver, CatalogHolder catalog, SqlLicenseChecker licenseChecker) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.catalogSupplier = catalog.catalogSupplier;
            this.licenseChecker = licenseChecker;
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
            operation(indexNameExpressionResolver, catalogSupplier, request, state, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                    indexNameExpressionResolver.concreteIndexNames(state, request));
        }

        /**
         * Class that holds that {@link Catalog} to aid in guice binding.
         */
        public static class CatalogHolder {
            final Function<ClusterState, Catalog> catalogSupplier;

            public CatalogHolder(Function<ClusterState, Catalog> catalogSupplier) {
                this.catalogSupplier = catalogSupplier;
            }
        }
    }

    /**
     * Actually looks up the indices in the cluster state and converts
     * them into {@link EsIndex} instances. The rest of the contents of
     * this class integrates this behavior cleanly into Elasticsearch,
     * makes sure that we only try and read the cluster state when it is
     * ready, integrate with security to filter the requested indices to
     * what the user has permission to access, and leaves an appropriate
     * audit trail.
     */
    public static void operation(IndexNameExpressionResolver indexNameExpressionResolver, Function<ClusterState, Catalog> catalogSupplier,
            Request request, ClusterState state, ActionListener<Response> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        List<EsIndex> results = new ArrayList<>(concreteIndices.length);
        Catalog catalog = catalogSupplier.apply(state);
        for (String index : concreteIndices) {
            GetIndexResult result = catalog.getIndex(index);
            if (result.isValid()) {
                results.add(result.get());
            }
        }

        // Consistent sorting is better for testing and for humans
        Collections.sort(results, comparing(EsIndex::name));

        listener.onResponse(new Response(results));
    }
}
