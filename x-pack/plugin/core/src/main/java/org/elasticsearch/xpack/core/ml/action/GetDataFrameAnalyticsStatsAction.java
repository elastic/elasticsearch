/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetDataFrameAnalyticsStatsAction extends Action<GetDataFrameAnalyticsStatsAction.Response> {

    public static final GetDataFrameAnalyticsStatsAction INSTANCE = new GetDataFrameAnalyticsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/data_frame/analytics/stats/get";

    private GetDataFrameAnalyticsStatsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String id;
        private PageParams pageParams = PageParams.defaultParams();

        public Request(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID.getPreferredName());
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            pageParams = in.readOptionalWriteable(PageParams::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeOptionalWriteable(pageParams);
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, pageParams);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(id, other.id) && Objects.equals(pageParams, other.pageParams);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        public RequestBuilder(ElasticsearchClient client, GetDataFrameAnalyticsStatsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static class Stats implements ToXContentObject, Writeable {

            private final String id;
            private final DataFrameAnalyticsState state;
            @Nullable
            private final DiscoveryNode node;
            @Nullable
            private final String assignmentExplanation;

            public Stats(String id, DataFrameAnalyticsState state, @Nullable DiscoveryNode node,
                         @Nullable String assignmentExplanation) {
                this.id = Objects.requireNonNull(id);
                this.state = Objects.requireNonNull(state);
                this.node = node;
                this.assignmentExplanation = assignmentExplanation;
            }

            public Stats(StreamInput in) throws IOException {
                id = in.readString();
                state = DataFrameAnalyticsState.fromStream(in);
                node = in.readOptionalWriteable(DiscoveryNode::new);
                assignmentExplanation = in.readOptionalString();
            }

            public String getId() {
                return id;
            }

            public DataFrameAnalyticsState getState() {
                return state;
            }

            public DiscoveryNode getNode() {
                return node;
            }

            public String getAssignmentExplanation() {
                return assignmentExplanation;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                // TODO: Have callers wrap the content with an object as they choose rather than forcing it upon them
                builder.startObject();
                {
                    toUnwrappedXContent(builder);
                }
                return builder.endObject();
            }

            public XContentBuilder toUnwrappedXContent(XContentBuilder builder) throws IOException {
                builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
                builder.field("state", state.toString());
                if (node != null) {
                    builder.startObject("node");
                    builder.field("id", node.getId());
                    builder.field("name", node.getName());
                    builder.field("ephemeral_id", node.getEphemeralId());
                    builder.field("transport_address", node.getAddress().toString());

                    builder.startObject("attributes");
                    for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                        builder.field(entry.getKey(), entry.getValue());
                    }
                    builder.endObject();
                    builder.endObject();
                }
                if (assignmentExplanation != null) {
                    builder.field("assignment_explanation", assignmentExplanation);
                }
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(id);
                state.writeTo(out);
                out.writeOptionalWriteable(node);
                out.writeOptionalString(assignmentExplanation);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, state, node, assignmentExplanation);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                Stats other = (Stats) obj;
                return Objects.equals(id, other.id)
                        && Objects.equals(this.state, other.state)
                        && Objects.equals(this.node, other.node)
                        && Objects.equals(this.assignmentExplanation, other.assignmentExplanation);
            }
        }

        private QueryPage<Stats> stats;

        public Response() {}

        public Response(QueryPage<Stats> stats) {
            this.stats = stats;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            stats = new QueryPage<>(in, Stats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            stats.writeTo(out);
        }

        public QueryPage<Stats> getResponse() {
            return stats;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            stats.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(stats);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(stats, other.stats);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

}
