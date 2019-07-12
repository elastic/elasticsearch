/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStatsInfo;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetDataFrameTransformsStatsAction extends ActionType<GetDataFrameTransformsStatsAction.Response> {

    public static final GetDataFrameTransformsStatsAction INSTANCE = new GetDataFrameTransformsStatsAction();
    public static final String NAME = "cluster:monitor/data_frame/stats/get";
    public GetDataFrameTransformsStatsAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class Request extends BaseTasksRequest<Request> {
        private final String id;
        private PageParams pageParams = PageParams.defaultParams();
        private boolean allowNoMatch = true;

        public static final int MAX_SIZE_RETURN = 1000;
        // used internally to expand the queried id expression
        private List<String> expandedIds;

        public Request(String id) {
            if (Strings.isNullOrEmpty(id) || id.equals("*")) {
                this.id = MetaData.ALL;
            } else {
                this.id = id;
            }
            this.expandedIds = Collections.singletonList(id);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            expandedIds = Collections.unmodifiableList(in.readStringList());
            pageParams = new PageParams(in);
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                allowNoMatch = in.readBoolean();
            }
        }

        @Override
        public boolean match(Task task) {
            // Only get tasks that we have expanded to
            return expandedIds.stream()
                .anyMatch(transformId -> task.getDescription().equals(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + transformId));
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

        public final void setPageParams(PageParams pageParams) {
            this.pageParams = Objects.requireNonNull(pageParams);
        }

        public final PageParams getPageParams() {
            return pageParams;
        }

        public boolean isAllowNoMatch() {
            return allowNoMatch;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeStringCollection(expandedIds);
            pageParams.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
                out.writeBoolean(allowNoMatch);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException exception = null;
            if (getPageParams() != null && getPageParams().getSize() > MAX_SIZE_RETURN) {
                exception = addValidationError("Param [" + PageParams.SIZE.getPreferredName() +
                    "] has a max acceptable value of [" + MAX_SIZE_RETURN + "]", exception);
            }
            return exception;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, pageParams, allowNoMatch);
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
                && allowNoMatch == other.allowNoMatch;
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {
        private final QueryPage<DataFrameTransformStateAndStatsInfo> transformsStateAndStatsInfo;

        public Response(List<DataFrameTransformStateAndStatsInfo> transformStateAndStats, long count) {
            this(new QueryPage<>(transformStateAndStats, count, DataFrameField.TRANSFORMS));
        }

        public Response(List<DataFrameTransformStateAndStatsInfo> transformStateAndStats,
                        long count,
                        List<TaskOperationFailure> taskFailures,
                        List<? extends ElasticsearchException> nodeFailures) {
            this(new QueryPage<>(transformStateAndStats, count, DataFrameField.TRANSFORMS), taskFailures, nodeFailures);
        }

        private Response(QueryPage<DataFrameTransformStateAndStatsInfo> transformsStateAndStatsInfo) {
            this(transformsStateAndStatsInfo, Collections.emptyList(), Collections.emptyList());
        }

        private Response(QueryPage<DataFrameTransformStateAndStatsInfo> transformsStateAndStatsInfo,
                         List<TaskOperationFailure> taskFailures,
                         List<? extends ElasticsearchException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.transformsStateAndStatsInfo = ExceptionsHelper.requireNonNull(transformsStateAndStatsInfo, "transformsStateAndStatsInfo");
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                transformsStateAndStatsInfo = new QueryPage<>(in, DataFrameTransformStateAndStatsInfo::new);
            } else {
                List<DataFrameTransformStateAndStatsInfo> stats = in.readList(DataFrameTransformStateAndStatsInfo::new);
                transformsStateAndStatsInfo = new QueryPage<>(stats, stats.size(), DataFrameField.TRANSFORMS);
            }
        }

        public List<DataFrameTransformStateAndStatsInfo> getTransformsStateAndStatsInfo() {
            return transformsStateAndStatsInfo.results();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
                transformsStateAndStatsInfo.writeTo(out);
            } else {
                out.writeList(transformsStateAndStatsInfo.results());
            }
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            transformsStateAndStatsInfo.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformsStateAndStatsInfo);
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
            return Objects.equals(this.transformsStateAndStatsInfo, that.transformsStateAndStatsInfo);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
