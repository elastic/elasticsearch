/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StopDataFrameAnalyticsAction extends ActionType<StopDataFrameAnalyticsAction.Response> {

    public static final StopDataFrameAnalyticsAction INSTANCE = new StopDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/stop";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private StopDataFrameAnalyticsAction() {
        super(NAME, StopDataFrameAnalyticsAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {

        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
        public static final ParseField FORCE = new ParseField("force");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, id) -> request.id = id, DataFrameAnalyticsConfig.ID);
            PARSER.declareString((request, val) -> request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareBoolean(Request::setAllowNoMatch, ALLOW_NO_MATCH);
            PARSER.declareBoolean(Request::setForce, FORCE);
        }

        public static Request parseRequest(String id, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getId() == null) {
                request.setId(id);
            } else if (Strings.isNullOrEmpty(id) == false && id.equals(request.getId()) == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, DataFrameAnalyticsConfig.ID,
                    request.getId(), id));
            }
            return request;
        }

        private String id;
        private boolean allowNoMatch = true;
        private boolean force;
        private Set<String> expandedIds = Collections.emptySet();

        public Request(String id) {
            this();
            setId(id);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            allowNoMatch = in.readBoolean();
            force = in.readBoolean();
            expandedIds = new HashSet<>(Arrays.asList(in.readStringArray()));
        }

        public Request() {
            setTimeout(DEFAULT_TIMEOUT);
        }

        public final Request setId(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID);
            return this;
        }

        public String getId() {
            return id;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public Request setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
            return this;
        }

        public boolean isForce() {
            return force;
        }

        public Request setForce(boolean force) {
            this.force = force;
            return this;
        }

        @Nullable
        public Set<String> getExpandedIds() {
            return expandedIds;
        }

        public void setExpandedIds(Set<String> expandedIds) {
            this.expandedIds = Objects.requireNonNull(expandedIds);
        }

        @Override
        public boolean match(Task task) {
            return expandedIds.stream().anyMatch(expandedId -> StartDataFrameAnalyticsAction.TaskMatcher.match(task, expandedId));
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(allowNoMatch);
            out.writeBoolean(force);
            out.writeStringArray(expandedIds.toArray(new String[0]));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder
                .startObject()
                .field(DataFrameAnalyticsConfig.ID.getPreferredName(), id)
                .field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch)
                .field(FORCE.getPreferredName(), force)
                .endObject();
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, getTimeout(), allowNoMatch, force, expandedIds);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            StopDataFrameAnalyticsAction.Request other = (StopDataFrameAnalyticsAction.Request) obj;
            return Objects.equals(id, other.id)
                && Objects.equals(getTimeout(), other.getTimeout())
                && allowNoMatch == other.allowNoMatch
                && force == other.force
                && Objects.equals(expandedIds, other.expandedIds);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean stopped;

        public Response(boolean stopped) {
            super(null, null);
            this.stopped = stopped;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            stopped = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(stopped);
        }

        public boolean isStopped() {
            return stopped;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("stopped", stopped);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Response response = (Response) o;
            return stopped == response.stopped;
        }

        @Override
        public int hashCode() {
            return Objects.hash(stopped);
        }
    }
}
