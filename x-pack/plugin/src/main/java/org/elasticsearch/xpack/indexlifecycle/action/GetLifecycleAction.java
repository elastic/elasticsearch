/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy;

import java.io.IOException;
import java.util.Objects;

public class GetLifecycleAction
        extends Action<GetLifecycleAction.Request, GetLifecycleAction.Response, GetLifecycleAction.RequestBuilder> {
    public static final GetLifecycleAction INSTANCE = new GetLifecycleAction();
    public static final String NAME = "cluster:admin/xpack/indexlifecycle/get";

    protected GetLifecycleAction() {
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

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private LifecyclePolicy policy;

        Response() {
        }

        public Response(LifecyclePolicy policy) {
            this.policy = policy;
        }

        public LifecyclePolicy getPolicy() {
            return policy;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            policy.toXContent(builder, params);
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            policy = in.readNamedWriteable(LifecyclePolicy.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(policy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(policy, other.policy);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField POLICY_FIELD = new ParseField("policy");

        private String policyName;

        public Request(String policyName) {
            this.policyName = policyName;
        }

        Request() {
        }

        public String getPolicyName() {
            return policyName;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            policyName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(policyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policyName);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(policyName, other.policyName);
        }

    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, GetLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            IndexLifecycleMetadata metadata = clusterService.state().metaData().custom(IndexLifecycleMetadata.TYPE);
            if (metadata == null) {
                listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName()));
            } else {
                LifecyclePolicy policy = metadata.getPolicies().get(request.getPolicyName());
                if (policy == null) {
                    listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName()));
                } else {
                    listener.onResponse(new Response(policy));
                }
            }
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
