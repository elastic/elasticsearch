/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;

public class GetTransformStatsAction extends ActionType<GetTransformStatsAction.Response> {

    public static final GetTransformStatsAction INSTANCE = new GetTransformStatsAction();
    public static final String NAME = "cluster:monitor/transform/stats/get";

    public GetTransformStatsAction() {
        super(NAME);
    }

    public static final class Request extends BaseTasksRequest<Request> {
        private final String id;
        private PageParams pageParams = PageParams.defaultParams();
        private boolean allowNoMatch = true;
        private final boolean basic;

        public static final int MAX_SIZE_RETURN = 1000;
        // used internally to expand the queried id expression
        private List<String> expandedIds;

        public Request(String id, @Nullable TimeValue timeout, boolean basic) {
            setTimeout(timeout);
            if (Strings.isNullOrEmpty(id) || id.equals("*")) {
                this.id = Metadata.ALL;
            } else {
                this.id = id;
            }
            this.expandedIds = Collections.singletonList(this.id);
            this.basic = basic;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            expandedIds = in.readCollectionAsImmutableList(StreamInput::readString);
            pageParams = new PageParams(in);
            allowNoMatch = in.readBoolean();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                basic = in.readBoolean();
            } else {
                basic = false;
            }
        }

        @Override
        public boolean match(Task task) {
            // Only get tasks that we have expanded to
            return expandedIds.stream()
                .anyMatch(transformId -> task.getDescription().equals(TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX + transformId));
        }

        public String getId() {
            return id;
        }

        public List<String> getExpandedIds() {
            return expandedIds;
        }

        public void setExpandedIds(List<String> expandedIds) {
            this.expandedIds = List.copyOf(expandedIds);
        }

        public void setPageParams(PageParams pageParams) {
            this.pageParams = Objects.requireNonNull(pageParams);
        }

        public PageParams getPageParams() {
            return pageParams;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        public boolean isBasic() {
            return basic;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeStringCollection(expandedIds);
            pageParams.writeTo(out);
            out.writeBoolean(allowNoMatch);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeBoolean(basic);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException exception = null;
            if (getPageParams() != null && getPageParams().getSize() > MAX_SIZE_RETURN) {
                exception = addValidationError(
                    "Param [" + PageParams.SIZE.getPreferredName() + "] has a max acceptable value of [" + MAX_SIZE_RETURN + "]",
                    exception
                );
            }
            return exception;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, pageParams, allowNoMatch, basic);
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
            return Objects.equals(id, other.id)
                && Objects.equals(pageParams, other.pageParams)
                && allowNoMatch == other.allowNoMatch
                && basic == other.basic;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("get_transform_stats[%s]", this.id), parentTaskId, headers);
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {
        private final QueryPage<TransformStats> transformsStats;

        public Response(List<TransformStats> transformStateAndStats) {
            this(new QueryPage<>(transformStateAndStats, transformStateAndStats.size(), TransformField.TRANSFORMS));
        }

        public Response(
            List<TransformStats> transformStateAndStats,
            long count,
            List<TaskOperationFailure> taskFailures,
            List<? extends ElasticsearchException> nodeFailures
        ) {
            this(new QueryPage<>(transformStateAndStats, count, TransformField.TRANSFORMS), taskFailures, nodeFailures);
        }

        public Response(QueryPage<TransformStats> transformsStats) {
            this(transformsStats, Collections.emptyList(), Collections.emptyList());
        }

        private Response(
            QueryPage<TransformStats> transformsStats,
            List<TaskOperationFailure> taskFailures,
            List<? extends ElasticsearchException> nodeFailures
        ) {
            super(taskFailures, nodeFailures);
            this.transformsStats = ExceptionsHelper.requireNonNull(transformsStats, "transformsStats");
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            transformsStats = new QueryPage<>(in, TransformStats::new);
        }

        public List<TransformStats> getTransformsStats() {
            return transformsStats.results();
        }

        public long getCount() {
            return transformsStats.count();
        }

        public ParseField getResultsField() {
            return transformsStats.getResultsField();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            transformsStats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            transformsStats.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformsStats);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final Response that = (Response) other;
            return Objects.equals(this.transformsStats, that.transformsStats);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
