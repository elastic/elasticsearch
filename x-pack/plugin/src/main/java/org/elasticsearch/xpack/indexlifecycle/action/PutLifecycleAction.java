/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class PutLifecycleAction extends Action<PutLifecycleAction.Request, PutLifecycleAction.Response, PutLifecycleAction.RequestBuilder> {
    public static final PutLifecycleAction INSTANCE = new PutLifecycleAction();
    public static final String NAME = "cluster:admin/xpack/indexlifecycle/put";

    protected PutLifecycleAction() {
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

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        public Response() {
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            addAcknowledgedField(builder);
            builder.endObject();
            return builder;
        }

    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ParseField POLICY_FIELD = new ParseField("policy");
        private static final ConstructingObjectParser<Request, Tuple<String, NamedXContentRegistry>> PARSER = new ConstructingObjectParser<>(
                "put_lifecycle_request", a -> new Request((LifecyclePolicy) a[0]));
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> LifecyclePolicy.parse(p, c), POLICY_FIELD);
        }
        
        private LifecyclePolicy policy;
        
        public Request(LifecyclePolicy policy) {
            this.policy = policy;
        }
        
        Request() {
        }

        public LifecyclePolicy getPolicy() {
            return policy;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public static Request parseRequest(String name, XContentParser parser, NamedXContentRegistry namedXContentRegistry) {
            return PARSER.apply(parser, new Tuple<String, NamedXContentRegistry>(name, namedXContentRegistry));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(POLICY_FIELD.getPreferredName(), policy);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            policy = new LifecyclePolicy(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            policy.writeTo(out);
        }

    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, PutLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver,
                    Request::new);
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
            clusterService.submitStateUpdateTask("put-lifecycle-" + request.getPolicy().getName(),
                    new AckedClusterStateUpdateTask<Response>(request, listener) {
                        @Override
                        protected Response newResponse(boolean acknowledged) {
                            return new Response(acknowledged);
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            ClusterState.Builder newState = ClusterState.builder(currentState);
                            IndexLifecycleMetadata currentMetadata = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);
                            if (currentMetadata.getPolicies().containsKey(request.getPolicy().getName())) {
                                throw new ResourceAlreadyExistsException("Lifecycle policy already exists: {}",
                                        request.getPolicy().getName());
                            }
                            SortedMap<String, LifecyclePolicy> newPolicies = new TreeMap<>(currentMetadata.getPolicies());
                            newPolicies.put(request.getPolicy().getName(), request.getPolicy());
                            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentMetadata.getPollInterval());
                            newState.metaData(MetaData.builder(currentState.getMetaData())
                                    .putCustom(IndexLifecycleMetadata.TYPE, newMetadata).build());
                            return newState.build();
                        }
                    });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
