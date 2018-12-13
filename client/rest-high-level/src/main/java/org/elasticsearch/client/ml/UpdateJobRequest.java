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
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Updates a {@link org.elasticsearch.client.ml.job.config.Job} with the passed {@link JobUpdate}
 * settings
 */
public class UpdateJobRequest extends ActionRequest implements ToXContentObject {

    private final JobUpdate update;

    public UpdateJobRequest(JobUpdate update) {
        this.update = update;
    }

    public JobUpdate getJobUpdate() {
        return update;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return update.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UpdateJobRequest that = (UpdateJobRequest) o;
        return Objects.equals(update, that.update);
    }

    @Override
    public int hashCode() {
        return Objects.hash(update);
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
