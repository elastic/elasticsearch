/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetDataFrameTransformsAction extends Action<GetDataFrameTransformsAction.Response>{

    public static final GetDataFrameTransformsAction INSTANCE = new GetDataFrameTransformsAction();
    public static final String NAME = "cluster:monitor/data_frame/get";

    private static final ParseField INVALID_TRANSFORMS = new ParseField("invalid_transforms_count");
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(GetDataFrameTransformsAction.class));

    private GetDataFrameTransformsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContent {
        private String id;

        public Request(String id) {
            if (Strings.isNullOrEmpty(id) || id.equals("*")) {
                this.id = MetaData.ALL;
            } else {
                this.id = id;
            }
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
        }

        @Override
        public boolean match(Task task) {
            // If we are retrieving all the transforms, the task description does not contain the id
            if (id.equals(MetaData.ALL)) {
                return task.getDescription().startsWith(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX);
            }
            // Otherwise find the task by ID
            return task.getDescription().equals(DataFrameField.PERSISTENT_TASK_DESCRIPTION_PREFIX + id);
        }

        public String getId() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(DataFrameField.ID.getPreferredName(), id);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
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
            return Objects.equals(id, other.id);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, GetDataFrameTransformsAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private List<DataFrameTransformConfig> transformConfigurations;

        public Response(List<DataFrameTransformConfig> transformConfigs) {
            super(Collections.emptyList(), Collections.emptyList());
            this.transformConfigurations = transformConfigs;
        }

        public Response(List<DataFrameTransformConfig> transformConfigs, List<TaskOperationFailure> taskFailures,
                List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.transformConfigurations = transformConfigs;
        }

        public Response() {
            super(Collections.emptyList(), Collections.emptyList());
        }

        public Response(StreamInput in) throws IOException {
            super(Collections.emptyList(), Collections.emptyList());
            readFrom(in);
        }

        public List<DataFrameTransformConfig> getTransformConfigurations() {
            return transformConfigurations;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            transformConfigurations = in.readList(DataFrameTransformConfig::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(transformConfigurations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            int invalidTransforms = 0;

            builder.startObject();
            builder.field(DataFrameField.COUNT.getPreferredName(), transformConfigurations.size());
            // XContentBuilder does not support passing the params object for Iterables
            builder.field(DataFrameField.TRANSFORMS.getPreferredName());
            builder.startArray();
            for (DataFrameTransformConfig configResponse : transformConfigurations) {
                configResponse.toXContent(builder, params);
                if (configResponse.isValid() == false) {
                    ++invalidTransforms;
                }
            }
            builder.endArray();
            if (invalidTransforms != 0) {
                builder.field(INVALID_TRANSFORMS.getPreferredName(), invalidTransforms);
                deprecationLogger.deprecated("Found [{}] invalid transforms", invalidTransforms);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformConfigurations);
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
            return Objects.equals(this.transformConfigurations, that.transformConfigurations);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }
}
