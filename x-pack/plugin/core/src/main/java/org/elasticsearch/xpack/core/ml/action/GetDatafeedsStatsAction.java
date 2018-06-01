/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetDatafeedsStatsAction extends Action<GetDatafeedsStatsAction.Request, GetDatafeedsStatsAction.Response> {

    public static final GetDatafeedsStatsAction INSTANCE = new GetDatafeedsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/datafeeds/stats/get";

    public static final String ALL = "_all";
    private static final String STATE = "state";

    private GetDatafeedsStatsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        public static final ParseField ALLOW_NO_DATAFEEDS = new ParseField("allow_no_datafeeds");

        private String datafeedId;
        private boolean allowNoDatafeeds = true;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeedId = in.readString();
            if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
                allowNoDatafeeds = in.readBoolean();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
                out.writeBoolean(allowNoDatafeeds);
            }
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public boolean allowNoDatafeeds() {
            return allowNoDatafeeds;
        }

        public void setAllowNoDatafeeds(boolean allowNoDatafeeds) {
            this.allowNoDatafeeds = allowNoDatafeeds;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, allowNoDatafeeds);
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
            return Objects.equals(datafeedId, other.datafeedId) && Objects.equals(allowNoDatafeeds, other.allowNoDatafeeds);
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetDatafeedsStatsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static class DatafeedStats implements ToXContentObject, Writeable {

            private final String datafeedId;
            private final DatafeedState datafeedState;
            @Nullable
            private DiscoveryNode node;
            @Nullable
            private String assignmentExplanation;

            public DatafeedStats(String datafeedId, DatafeedState datafeedState, @Nullable DiscoveryNode node,
                          @Nullable String assignmentExplanation) {
                this.datafeedId = Objects.requireNonNull(datafeedId);
                this.datafeedState = Objects.requireNonNull(datafeedState);
                this.node = node;
                this.assignmentExplanation = assignmentExplanation;
            }

            DatafeedStats(StreamInput in) throws IOException {
                datafeedId = in.readString();
                datafeedState = DatafeedState.fromStream(in);
                node = in.readOptionalWriteable(DiscoveryNode::new);
                assignmentExplanation = in.readOptionalString();
            }

            public String getDatafeedId() {
                return datafeedId;
            }

            public DatafeedState getDatafeedState() {
                return datafeedState;
            }

            public DiscoveryNode getNode() {
                return node;
            }

            public String getAssignmentExplanation() {
                return assignmentExplanation;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
                builder.field(STATE, datafeedState.toString());
                if (node != null) {
                    builder.startObject("node");
                    builder.field("id", node.getId());
                    builder.field("name", node.getName());
                    builder.field("ephemeral_id", node.getEphemeralId());
                    builder.field("transport_address", node.getAddress().toString());

                    builder.startObject("attributes");
                    for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                        if (entry.getKey().startsWith("ml.")) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                    }
                    builder.endObject();
                    builder.endObject();
                }
                if (assignmentExplanation != null) {
                    builder.field("assignment_explanation", assignmentExplanation);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(datafeedId);
                datafeedState.writeTo(out);
                out.writeOptionalWriteable(node);
                out.writeOptionalString(assignmentExplanation);
            }

            @Override
            public int hashCode() {
                return Objects.hash(datafeedId, datafeedState, node, assignmentExplanation);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                DatafeedStats other = (DatafeedStats) obj;
                return Objects.equals(datafeedId, other.datafeedId) &&
                        Objects.equals(this.datafeedState, other.datafeedState) &&
                        Objects.equals(this.node, other.node) &&
                        Objects.equals(this.assignmentExplanation, other.assignmentExplanation);
            }
        }

        private QueryPage<DatafeedStats> datafeedsStats;

        public Response(QueryPage<DatafeedStats> datafeedsStats) {
            this.datafeedsStats = datafeedsStats;
        }

        public Response() {}

        public QueryPage<DatafeedStats> getResponse() {
            return datafeedsStats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedsStats = new QueryPage<>(in, DatafeedStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            datafeedsStats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            datafeedsStats.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedsStats);
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
            return Objects.equals(datafeedsStats, other.datafeedsStats);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

}
