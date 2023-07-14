/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StartDataFrameAnalyticsAction extends ActionType<NodeAcknowledgedResponse> {

    public static final StartDataFrameAnalyticsAction INSTANCE = new StartDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/start";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(20, TimeUnit.SECONDS);

    private StartDataFrameAnalyticsAction() {
        super(NAME, NodeAcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        public static final ParseField TIMEOUT = new ParseField("timeout");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, id) -> request.id = id, DataFrameAnalyticsConfig.ID);
            PARSER.declareString((request, val) -> request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request parseRequest(String id, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getId() == null) {
                request.setId(id);
            } else if (Strings.isNullOrEmpty(id) == false && id.equals(request.getId()) == false) {
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, DataFrameAnalyticsConfig.ID, request.getId(), id)
                );
            }
            return request;
        }

        private String id;
        private TimeValue timeout = DEFAULT_TIMEOUT;

        public Request(String id) {
            setId(id);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            timeout = in.readTimeValue();
        }

        public Request() {}

        public final void setId(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID);
        }

        public String getId() {
            return id;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeTimeValue(timeout);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (id != null) {
                builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
            }
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, timeout);
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
            return Objects.equals(id, other.id) && Objects.equals(timeout, other.timeout);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class TaskParams implements PersistentTaskParams, MlTaskParams {

        public static final MlConfigVersion VERSION_INTRODUCED = MlConfigVersion.V_7_3_0;
        public static final TransportVersion TRANSPORT_VERSION_INTRODUCED = TransportVersion.V_7_3_0;
        public static final MlConfigVersion VERSION_DESTINATION_INDEX_MAPPINGS_CHANGED = MlConfigVersion.V_7_10_0;

        public static final ConstructingObjectParser<TaskParams, Void> PARSER = new ConstructingObjectParser<>(
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            true,
            a -> new TaskParams((String) a[0], (String) a[1], (Boolean) a[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameAnalyticsConfig.ID);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameAnalyticsConfig.VERSION);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DataFrameAnalyticsConfig.ALLOW_LAZY_START);
        }

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String id;
        private final MlConfigVersion version;
        private final boolean allowLazyStart;

        public TaskParams(String id, MlConfigVersion version, boolean allowLazyStart) {
            this.id = Objects.requireNonNull(id);
            this.version = Objects.requireNonNull(version);
            this.allowLazyStart = allowLazyStart;
        }

        private TaskParams(String id, String version, Boolean allowLazyStart) {
            this(id, MlConfigVersion.fromString(version), allowLazyStart != null && allowLazyStart);
        }

        public TaskParams(StreamInput in) throws IOException {
            this.id = in.readString();
            this.version = MlConfigVersion.readVersion(in);
            this.allowLazyStart = in.readBoolean();
        }

        public String getId() {
            return id;
        }

        public MlConfigVersion getVersion() {
            return version;
        }

        public boolean isAllowLazyStart() {
            return allowLazyStart;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TRANSPORT_VERSION_INTRODUCED;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            MlConfigVersion.writeVersion(version, out);
            out.writeBoolean(allowLazyStart);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
            builder.field(DataFrameAnalyticsConfig.VERSION.getPreferredName(), version);
            builder.field(DataFrameAnalyticsConfig.ALLOW_LAZY_START.getPreferredName(), allowLazyStart);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, version, allowLazyStart);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskParams other = (TaskParams) o;
            return Objects.equals(id, other.id)
                && Objects.equals(version, other.version)
                && Objects.equals(allowLazyStart, other.allowLazyStart);
        }

        @Override
        public String getMlId() {
            return id;
        }
    }

    public interface TaskMatcher {

        static boolean match(Task task, String expectedId) {
            if (task instanceof TaskMatcher) {
                if (Strings.isAllOrWildcard(expectedId)) {
                    return true;
                }
                String expectedDescription = MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + expectedId;
                return expectedDescription.equals(task.getDescription());
            }
            return false;
        }
    }
}
