/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class StartDataFrameAnalyticsAction extends Action<AcknowledgedResponse> {

    public static final StartDataFrameAnalyticsAction INSTANCE = new StartDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/start";

    private StartDataFrameAnalyticsAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        private String id;

        public Request(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID);
        }

        public Request(StreamInput in) throws IOException {
            readFrom(in);
        }

        public Request() {
        }

        public String getId() {
            return id;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (id != null) {
                builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
            }
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            StartDataFrameAnalyticsAction.Request other = (StartDataFrameAnalyticsAction.Request) obj;
            return Objects.equals(id, other.id);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        RequestBuilder(ElasticsearchClient client, StartDataFrameAnalyticsAction action) {
            super(client, action, new Request());
        }
    }

    public static class TaskParams implements XPackPlugin.XPackPersistentTaskParams {

        public static ConstructingObjectParser<TaskParams, Void> PARSER = new ConstructingObjectParser<>(
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, true, a -> new TaskParams((String) a[0]));

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private String id;

        public TaskParams(String id) {
            this.id = Objects.requireNonNull(id);
        }

        public TaskParams(StreamInput in) throws IOException {
            this.id = in.readString();
        }

        public String getId() {
            return id;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            // TODO Update to first released version
            return Version.CURRENT;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
            builder.endObject();
            return builder;
        }
    }
}
