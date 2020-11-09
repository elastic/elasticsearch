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

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A request to retrieve overall buckets of set of jobs
 */
public class GetOverallBucketsRequest implements Validatable, ToXContentObject {

    public static final ParseField TOP_N = new ParseField("top_n");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField OVERALL_SCORE = new ParseField("overall_score");
    public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");

    private static final String ALL_JOBS = "_all";

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetOverallBucketsRequest, Void> PARSER = new ConstructingObjectParser<>(
            "get_overall_buckets_request", a -> new GetOverallBucketsRequest((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareInt(GetOverallBucketsRequest::setTopN, TOP_N);
        PARSER.declareString(GetOverallBucketsRequest::setBucketSpan, BUCKET_SPAN);
        PARSER.declareBoolean(GetOverallBucketsRequest::setExcludeInterim, EXCLUDE_INTERIM);
        PARSER.declareDouble(GetOverallBucketsRequest::setOverallScore, OVERALL_SCORE);
        PARSER.declareStringOrNull(GetOverallBucketsRequest::setStart, START);
        PARSER.declareStringOrNull(GetOverallBucketsRequest::setEnd, END);
        PARSER.declareBoolean(GetOverallBucketsRequest::setAllowNoMatch, ALLOW_NO_MATCH);
    }

    private final List<String> jobIds;
    private Integer topN;
    private TimeValue bucketSpan;
    private Boolean excludeInterim;
    private Double overallScore;
    private String start;
    private String end;
    private Boolean allowNoMatch;

    private GetOverallBucketsRequest(String jobId) {
        this(Strings.tokenizeToStringArray(jobId, ","));
    }

    /**
     * Constructs a request to retrieve overall buckets for a set of jobs
     * @param jobIds The job identifiers. Each can be a job identifier, a group name, or a wildcard expression.
     */
    public GetOverallBucketsRequest(String... jobIds) {
        this(Arrays.asList(jobIds));
    }

    /**
     * Constructs a request to retrieve overall buckets for a set of jobs
     * @param jobIds The job identifiers. Each can be a job identifier, a group name, or a wildcard expression.
     */
    public GetOverallBucketsRequest(List<String> jobIds) {
        if (jobIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("jobIds must not contain null values");
        }
        if (jobIds.isEmpty()) {
            this.jobIds = Collections.singletonList(ALL_JOBS);
        } else {
            this.jobIds = Collections.unmodifiableList(jobIds);
        }
    }

    public List<String> getJobIds() {
        return jobIds;
    }

    public Integer getTopN() {
        return topN;
    }

    /**
     * Sets the value of "top_n".
     * @param topN The number of top job bucket scores to be used in the overall_score calculation. Defaults to 1.
     */
    public void setTopN(Integer topN) {
        this.topN = topN;
    }

    public TimeValue getBucketSpan() {
        return bucketSpan;
    }

    /**
     * Sets the value of "bucket_span".
     * @param bucketSpan The span of the overall buckets. Must be greater or equal to the largest job’s bucket_span.
     *                   Defaults to the largest job’s bucket_span.
     */
    public void setBucketSpan(TimeValue bucketSpan) {
        this.bucketSpan = bucketSpan;
    }

    private void setBucketSpan(String bucketSpan) {
        this.bucketSpan = TimeValue.parseTimeValue(bucketSpan, BUCKET_SPAN.getPreferredName());
    }

    public boolean isExcludeInterim() {
        return excludeInterim;
    }

    /**
     * Sets the value of "exclude_interim".
     * When {@code true}, interim overall buckets will be filtered out.
     * Overall buckets are interim if any of the job buckets within the overall bucket interval are interim.
     * @param excludeInterim value of "exclude_interim" to be set
     */
    public void setExcludeInterim(Boolean excludeInterim) {
        this.excludeInterim = excludeInterim;
    }

    public String getStart() {
        return start;
    }

    /**
     * Sets the value of "start" which is a timestamp.
     * Only overall buckets whose timestamp is on or after the "start" value will be returned.
     * @param start String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    /**
     * Sets the value of "end" which is a timestamp.
     * Only overall buckets whose timestamp is before the "end" value will be returned.
     * @param end String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setEnd(String end) {
        this.end = end;
    }

    public Double getOverallScore() {
        return overallScore;
    }

    /**
     * Sets the value of "overall_score".
     * Only buckets with "overall_score" equal or greater will be returned.
     * @param overallScore value of "anomaly_score".
     */
    public void setOverallScore(double overallScore) {
        this.overallScore = overallScore;
    }

    /**
     * See {@link GetJobRequest#getAllowNoMatch()}
     * @param allowNoMatch value of "allow_no_match".
     */
    public void setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
    }

    /**
     * Whether to ignore if a wildcard expression matches no jobs.
     *
     * If this is {@code false}, then an error is returned when a wildcard (or {@code _all}) does not match any jobs
     */
    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (jobIds.isEmpty() == false) {
            builder.field(Job.ID.getPreferredName(), Strings.collectionToCommaDelimitedString(jobIds));
        }
        if (topN != null) {
            builder.field(TOP_N.getPreferredName(), topN);
        }
        if (bucketSpan != null) {
            builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan.getStringRep());
        }
        if (excludeInterim != null) {
            builder.field(EXCLUDE_INTERIM.getPreferredName(), excludeInterim);
        }
        if (start != null) {
            builder.field(START.getPreferredName(), start);
        }
        if (end != null) {
            builder.field(END.getPreferredName(), end);
        }
        if (overallScore != null) {
            builder.field(OVERALL_SCORE.getPreferredName(), overallScore);
        }
        if (allowNoMatch != null) {
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobIds, topN, bucketSpan, excludeInterim, overallScore, start, end, allowNoMatch);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetOverallBucketsRequest other = (GetOverallBucketsRequest) obj;
        return Objects.equals(jobIds, other.jobIds) &&
                Objects.equals(topN, other.topN) &&
                Objects.equals(bucketSpan, other.bucketSpan) &&
                Objects.equals(excludeInterim, other.excludeInterim) &&
                Objects.equals(overallScore, other.overallScore) &&
                Objects.equals(start, other.start) &&
                Objects.equals(end, other.end) &&
                Objects.equals(allowNoMatch, other.allowNoMatch);
    }
}
