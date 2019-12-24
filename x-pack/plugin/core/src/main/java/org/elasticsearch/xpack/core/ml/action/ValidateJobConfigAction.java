/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class ValidateJobConfigAction extends ActionType<AcknowledgedResponse> {

    public static final ValidateJobConfigAction INSTANCE = new ValidateJobConfigAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/validate";

    protected ValidateJobConfigAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse> {

        protected RequestBuilder(ElasticsearchClient client, ValidateJobConfigAction action) {
            super(client, action, new Request());
        }

    }

    public static class Request extends ActionRequest {

        private Job job;

        public static Request parseRequest(XContentParser parser) {
            Job.Builder jobBuilder = Job.STRICT_PARSER.apply(parser, null);
            // When jobs are PUT their ID must be supplied in the URL - assume this will
            // be valid unless an invalid job ID is specified in the JSON to be validated
            jobBuilder.setId(jobBuilder.getId() != null ? jobBuilder.getId() : "ok");

            // Validate that detector configs are unique.
            // This validation logically belongs to validateInputFields call but we perform it only for PUT action to avoid BWC issues which
            // would occur when parsing an old job config that already had duplicate detectors.
            jobBuilder.validateDetectorsAreUnique();

            // Some fields cannot be set at create time
            List<String> invalidJobCreationSettings = jobBuilder.invalidCreateTimeSettings();
            if (invalidJobCreationSettings.isEmpty() == false) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_INVALID_CREATE_SETTINGS,
                        String.join(",", invalidJobCreationSettings)));
            }

            return new Request(jobBuilder.build(new Date()));
        }

        public Request() {
            this.job = null;
        }

        public Request(Job job) {
            this.job = job;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            job = new Job(in);
        }

        public Job getJob() {
            return job;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            job.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(job);
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
            return Objects.equals(job, other.job);
        }

    }
}
