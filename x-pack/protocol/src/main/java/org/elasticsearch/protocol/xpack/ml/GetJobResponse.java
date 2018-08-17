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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Contains a {@link List} of the found {@link Job} objects and the total count found
 */
public class GetJobResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField RESULTS_FIELD = new ParseField("jobs");
    public static final ObjectParser<GetJobResponse, Void> PARSER = new ObjectParser<>("jobs_response", true, GetJobResponse::new);

    static {
        PARSER.declareObjectArray(GetJobResponse::setJobs, Job.PARSER, RESULTS_FIELD);
        PARSER.declareLong(GetJobResponse::setCount, QueryPage.COUNT);
    }

    private QueryPage<Job> jobs;

    GetJobResponse() {
        jobs = new QueryPage<>(RESULTS_FIELD);
    }

    public List<Job> getJobs() {
        return jobs.results();
    }

    public static GetJobResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    void setJobs(List<Job.Builder> jobs) {
        this.jobs.setResults(jobs.stream().map(Job.Builder::build).collect(Collectors.toList()));
    }

    void setCount(long count) {
        this.jobs.setCount(count);
    }

    public long getCount() {
        return this.jobs.count();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return jobs.toXContent(builder, params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetJobResponse other = (GetJobResponse) obj;
        return Objects.equals(jobs, other.jobs);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }
}
