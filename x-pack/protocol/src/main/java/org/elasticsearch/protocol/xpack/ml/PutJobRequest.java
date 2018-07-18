/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;
import org.elasticsearch.protocol.xpack.ml.messages.Messages;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class PutJobRequest extends AcknowledgedRequest<PutJobRequest> implements ToXContentObject {

    public static PutJobRequest parseRequest(String jobId, XContentParser parser) {
        Job.Builder jobBuilder = Job.CONFIG_PARSER.apply(parser, null);
        if (jobBuilder.getId() == null) {
            jobBuilder.setId(jobId);
        } else if (!Strings.isNullOrEmpty(jobId) && !jobId.equals(jobBuilder.getId())) {
            // If we have both URI and body jobBuilder ID, they must be identical
            throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, Job.ID.getPreferredName(),
                    jobBuilder.getId(), jobId));
        }

        return new PutJobRequest(jobBuilder);
    }

    private Job.Builder jobBuilder;

    public PutJobRequest(Job.Builder jobBuilder) {
        // Validate the jobBuilder immediately so that errors can be detected prior to transportation.
        jobBuilder.validateInputFields();

        // Some fields cannot be set at create time
        List<String> invalidJobCreationSettings = jobBuilder.invalidCreateTimeSettings();
        if (invalidJobCreationSettings.isEmpty() == false) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_INVALID_CREATE_SETTINGS,
                    String.join(",", invalidJobCreationSettings)));
        }

        this.jobBuilder = jobBuilder;
    }

    public PutJobRequest() {
    }

    public Job.Builder getJobBuilder() {
        return jobBuilder;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobBuilder = new Job.Builder(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        jobBuilder.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        jobBuilder.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutJobRequest request = (PutJobRequest) o;
        return Objects.equals(jobBuilder, request.jobBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobBuilder);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
