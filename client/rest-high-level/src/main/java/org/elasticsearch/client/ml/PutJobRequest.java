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
package org.elasticsearch.client.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to create a new Machine Learning Job given a {@link Job} configuration
 */
public class PutJobRequest extends ActionRequest implements ToXContentObject {

    private final Job job;

    /**
     * Construct a new PutJobRequest
     *
     * @param job a {@link Job} configuration to create
     */
    public PutJobRequest(Job job) {
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return job.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PutJobRequest request = (PutJobRequest) object;
        return Objects.equals(job, request.job);
    }

    @Override
    public int hashCode() {
        return Objects.hash(job);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
