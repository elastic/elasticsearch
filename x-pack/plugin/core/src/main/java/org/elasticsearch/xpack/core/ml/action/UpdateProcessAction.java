/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class UpdateProcessAction extends ActionType<UpdateProcessAction.Response> {

    public static final UpdateProcessAction INSTANCE = new UpdateProcessAction();
    public static final String NAME = "cluster:internal/xpack/ml/job/update/process";

    private UpdateProcessAction() {
        super(NAME, UpdateProcessAction.Response::new);
    }

    public static class Response extends BaseTasksResponse implements StatusToXContentObject, Writeable {

        private final boolean isUpdated;

        public Response() {
            super(null, null);
            this.isUpdated = true;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            isUpdated = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(isUpdated);
        }

        public boolean isUpdated() {
            return isUpdated;
        }

        @Override
        public RestStatus status() {
            return RestStatus.ACCEPTED;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("updated", isUpdated);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(isUpdated);
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

            return this.isUpdated == other.isUpdated;
        }
    }

    public static class Request extends JobTaskRequest<Request> {

        private ModelPlotConfig modelPlotConfig;
        private PerPartitionCategorizationConfig perPartitionCategorizationConfig;
        private List<JobUpdate.DetectorUpdate> detectorUpdates;
        private MlFilter filter;
        private boolean updateScheduledEvents = false;

        public Request(StreamInput in) throws IOException {
            super(in);
            modelPlotConfig = in.readOptionalWriteable(ModelPlotConfig::new);
            perPartitionCategorizationConfig = in.readOptionalWriteable(PerPartitionCategorizationConfig::new);
            if (in.readBoolean()) {
                detectorUpdates = in.readList(JobUpdate.DetectorUpdate::new);
            }
            filter = in.readOptionalWriteable(MlFilter::new);
            updateScheduledEvents = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(modelPlotConfig);
            out.writeOptionalWriteable(perPartitionCategorizationConfig);
            boolean hasDetectorUpdates = detectorUpdates != null;
            out.writeBoolean(hasDetectorUpdates);
            if (hasDetectorUpdates) {
                out.writeList(detectorUpdates);
            }
            out.writeOptionalWriteable(filter);
            out.writeBoolean(updateScheduledEvents);
        }

        public Request(String jobId, ModelPlotConfig modelPlotConfig, PerPartitionCategorizationConfig perPartitionCategorizationConfig,
                       List<JobUpdate.DetectorUpdate> detectorUpdates, MlFilter filter, boolean updateScheduledEvents) {
            super(jobId);
            this.modelPlotConfig = modelPlotConfig;
            this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
            this.detectorUpdates = detectorUpdates;
            this.filter = filter;
            this.updateScheduledEvents = updateScheduledEvents;
        }

        public ModelPlotConfig getModelPlotConfig() {
            return modelPlotConfig;
        }

        public PerPartitionCategorizationConfig getPerPartitionCategorizationConfig() {
            return perPartitionCategorizationConfig;
        }

        public List<JobUpdate.DetectorUpdate> getDetectorUpdates() {
            return detectorUpdates;
        }

        public MlFilter getFilter() {
            return filter;
        }

        public boolean isUpdateScheduledEvents() {
            return updateScheduledEvents;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getJobId(), modelPlotConfig, perPartitionCategorizationConfig, detectorUpdates, filter,
                updateScheduledEvents);
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

            return Objects.equals(getJobId(), other.getJobId()) &&
                    Objects.equals(modelPlotConfig, other.modelPlotConfig) &&
                    Objects.equals(perPartitionCategorizationConfig, other.perPartitionCategorizationConfig) &&
                    Objects.equals(detectorUpdates, other.detectorUpdates) &&
                    Objects.equals(filter, other.filter) &&
                    Objects.equals(updateScheduledEvents, other.updateScheduledEvents);
        }
    }
}
