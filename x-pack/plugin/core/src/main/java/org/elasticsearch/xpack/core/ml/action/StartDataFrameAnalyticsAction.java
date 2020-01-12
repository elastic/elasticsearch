/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StartDataFrameAnalyticsAction extends ActionType<AcknowledgedResponse> {

    public static final StartDataFrameAnalyticsAction INSTANCE = new StartDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/start";

    private StartDataFrameAnalyticsAction() {
        super(NAME, AcknowledgedResponse::new);
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
            } else if (!Strings.isNullOrEmpty(id) && !id.equals(request.getId())) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, DataFrameAnalyticsConfig.ID,
                    request.getId(), id));
            }
            return request;
        }

        private String id;
        private TimeValue timeout = TimeValue.timeValueSeconds(20);

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

    static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        RequestBuilder(ElasticsearchClient client, StartDataFrameAnalyticsAction action) {
            super(client, action, new Request());
        }
    }

    public static class TaskParams implements PersistentTaskParams {

        public static final Version VERSION_INTRODUCED = Version.V_7_3_0;

        private static final ParseField PROGRESS_ON_START = new ParseField("progress_on_start");

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<TaskParams, Void> PARSER = new ConstructingObjectParser<>(
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, true,
            a -> new TaskParams((String) a[0], (String) a[1], (List<PhaseProgress>) a[2], (Boolean) a[3]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameAnalyticsConfig.ID);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameAnalyticsConfig.VERSION);
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), PhaseProgress.PARSER, PROGRESS_ON_START);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DataFrameAnalyticsConfig.ALLOW_LAZY_START);
        }

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String id;
        private final Version version;
        private final List<PhaseProgress> progressOnStart;
        private final boolean allowLazyStart;

        public TaskParams(String id, Version version, List<PhaseProgress> progressOnStart, boolean allowLazyStart) {
            this.id = Objects.requireNonNull(id);
            this.version = Objects.requireNonNull(version);
            this.progressOnStart = Collections.unmodifiableList(progressOnStart);
            this.allowLazyStart = allowLazyStart;
        }

        private TaskParams(String id, String version, @Nullable List<PhaseProgress> progressOnStart, Boolean allowLazyStart) {
            this(id, Version.fromString(version), progressOnStart == null ? Collections.emptyList() : progressOnStart,
                allowLazyStart != null && allowLazyStart);
        }

        public TaskParams(StreamInput in) throws IOException {
            this.id = in.readString();
            this.version = Version.readVersion(in);
            if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
                progressOnStart = in.readList(PhaseProgress::new);
            } else {
                progressOnStart = Collections.emptyList();
            }
            if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
                allowLazyStart = in.readBoolean();
            } else {
                allowLazyStart = false;
            }
        }

        public String getId() {
            return id;
        }

        public List<PhaseProgress> getProgressOnStart() {
            return progressOnStart;
        }

        public boolean isAllowLazyStart() {
            return allowLazyStart;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return VERSION_INTRODUCED;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            Version.writeVersion(version, out);
            if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
                out.writeList(progressOnStart);
            }
            if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
                out.writeBoolean(allowLazyStart);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
            builder.field(DataFrameAnalyticsConfig.VERSION.getPreferredName(), version);
            builder.field(PROGRESS_ON_START.getPreferredName(), progressOnStart);
            builder.field(DataFrameAnalyticsConfig.ALLOW_LAZY_START.getPreferredName(), allowLazyStart);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, version, progressOnStart, allowLazyStart);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskParams other = (TaskParams) o;
            return Objects.equals(id, other.id)
                && Objects.equals(version, other.version)
                && Objects.equals(progressOnStart, other.progressOnStart)
                && Objects.equals(allowLazyStart, other.allowLazyStart);
        }
    }

    public interface TaskMatcher {

        static boolean match(Task task, String expectedId) {
            if (task instanceof TaskMatcher) {
                if (MetaData.ALL.equals(expectedId)) {
                    return true;
                }
                String expectedDescription = MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + expectedId;
                return expectedDescription.equals(task.getDescription());
            }
            return false;
        }
    }
}
