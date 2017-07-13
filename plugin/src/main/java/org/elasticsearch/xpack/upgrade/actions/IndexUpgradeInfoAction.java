/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.upgrade.IndexUpgradeService;
import org.elasticsearch.xpack.upgrade.UpgradeActionRequired;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IndexUpgradeInfoAction extends Action<IndexUpgradeInfoAction.Request, IndexUpgradeInfoAction.Response,
        IndexUpgradeInfoAction.RequestBuilder> {

    public static final IndexUpgradeInfoAction INSTANCE = new IndexUpgradeInfoAction();
    public static final String NAME = "cluster:admin/xpack/upgrade/info";

    private IndexUpgradeInfoAction() {
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

    public static class Response extends ActionResponse implements ToXContentObject {
        private Map<String, UpgradeActionRequired> actions;

        protected Response() {

        }

        public Response(Map<String, UpgradeActionRequired> actions) {
            this.actions = actions;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            actions = in.readMap(StreamInput::readString, UpgradeActionRequired::readFromStream);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(actions, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        }

        public Map<String, UpgradeActionRequired> getActions() {
            return actions;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startObject("indices");
                for (Map.Entry<String, UpgradeActionRequired> entry : actions.entrySet()) {
                    builder.startObject(entry.getKey());
                    {
                        builder.field("action_required", entry.getValue().toString());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(actions, response.actions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actions);
        }
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private String[] indices = null;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true);

        // for serialization
        public Request() {

        }

        public Request(String... indices) {
            this.indices = indices;
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

        public void indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (indices == null) {
                validationException = addValidationError("index/indices is missing", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            indices = in.readStringArray();
            indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(indices, request.indices) &&
                    Objects.equals(indicesOptions.toString(), request.indicesOptions.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), indicesOptions.toString());
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, IndexUpgradeInfoAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder setIndices(String... indices) {
            request.indices(indices);
            return this;
        }

        public RequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
            request.indicesOptions(indicesOptions);
            return this;
        }
    }

    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final IndexUpgradeService indexUpgradeService;
        private final XPackLicenseState licenseState;


        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexUpgradeService indexUpgradeService,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               XPackLicenseState licenseState) {
            super(settings, IndexUpgradeInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.indexUpgradeService = indexUpgradeService;
            this.licenseState = licenseState;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected final void masterOperation(final Request request, ClusterState state, final ActionListener<Response> listener) {
            if (licenseState.isUpgradeAllowed()) {
                Map<String, UpgradeActionRequired> results =
                        indexUpgradeService.upgradeInfo(request.indices(), request.indicesOptions(), state);
                listener.onResponse(new Response(results));
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.UPGRADE));
            }
        }
    }
}