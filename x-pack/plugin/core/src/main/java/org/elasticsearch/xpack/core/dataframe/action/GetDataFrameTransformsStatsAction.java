/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
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
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetDataFrameTransformsStatsAction extends Action<GetDataFrameTransformsStatsAction.Response> {

    public static final GetDataFrameTransformsStatsAction INSTANCE = new GetDataFrameTransformsStatsAction();
    public static final String NAME = "cluster:monitor/data_frame/stats/get";
    public GetDataFrameTransformsStatsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> {
        private String id;
        private PageParams pageParams = PageParams.defaultParams();

        public static final int MAX_SIZE_RETURN = 1000;
        // used internally to expand the queried id expression
        private List<String> expandedIds = Collections.emptyList();

        public Request(String id) {
            if (Strings.isNullOrEmpty(id) || id.equals("*")) {
                this.id = MetaData.ALL;
            } else {
                this.id = id;
            }
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            expandedIds = in.readList(StreamInput::readString);
            pageParams = in.readOptionalWriteable(PageParams::new);
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
            this.expandedIds = Collections.unmodifiableList(new ArrayList<>(expandedIds));
        }

        public final void setPageParams(PageParams pageParams) {
            this.pageParams = pageParams;
        }

        public final PageParams getPageParams() {
            return pageParams;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeStringCollection(expandedIds);
            out.writeOptionalWriteable(pageParams);
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

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {
        private List<DataFrameTransformStateAndStats> transformsStateAndStats;

        public Response(List<DataFrameTransformStateAndStats> transformsStateAndStats) {
            super(Collections.emptyList(), Collections.emptyList());
            this.transformsStateAndStats = transformsStateAndStats;
        }

        public Response(List<DataFrameTransformStateAndStats> transformsStateAndStats, List<TaskOperationFailure> taskFailures,
                List<? extends ElasticsearchException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.transformsStateAndStats = transformsStateAndStats;
        }

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
            this.transformsStateAndStats = Collections.emptyList();
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            readFrom(in);
        }

        public List<DataFrameTransformStateAndStats> getTransformsStateAndStats() {
            return transformsStateAndStats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            transformsStateAndStats = in.readList(DataFrameTransformStateAndStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(transformsStateAndStats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field(DataFrameField.COUNT.getPreferredName(), transformsStateAndStats.size());
            builder.field(DataFrameField.TRANSFORMS.getPreferredName(), transformsStateAndStats);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformsStateAndStats);
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
            return Objects.equals(this.transformsStateAndStats, that.transformsStateAndStats);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
