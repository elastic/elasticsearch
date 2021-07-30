/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.results.Result;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to retrieve buckets of a given job
 */
public class GetBucketsRequest implements Validatable, ToXContentObject {

    public static final ParseField EXPAND = new ParseField("expand");
    public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField ANOMALY_SCORE = new ParseField("anomaly_score");
    public static final ParseField SORT = new ParseField("sort");
    public static final ParseField DESCENDING = new ParseField("desc");

    public static final ObjectParser<GetBucketsRequest, Void> PARSER = new ObjectParser<>("get_buckets_request", GetBucketsRequest::new);

    static {
        PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
        PARSER.declareString(GetBucketsRequest::setTimestamp, Result.TIMESTAMP);
        PARSER.declareBoolean(GetBucketsRequest::setExpand, EXPAND);
        PARSER.declareBoolean(GetBucketsRequest::setExcludeInterim, EXCLUDE_INTERIM);
        PARSER.declareStringOrNull(GetBucketsRequest::setStart, START);
        PARSER.declareStringOrNull(GetBucketsRequest::setEnd, END);
        PARSER.declareObject(GetBucketsRequest::setPageParams, PageParams.PARSER, PageParams.PAGE);
        PARSER.declareDouble(GetBucketsRequest::setAnomalyScore, ANOMALY_SCORE);
        PARSER.declareString(GetBucketsRequest::setSort, SORT);
        PARSER.declareBoolean(GetBucketsRequest::setDescending, DESCENDING);
    }

    private String jobId;
    private String timestamp;
    private Boolean expand;
    private Boolean excludeInterim;
    private String start;
    private String end;
    private PageParams pageParams;
    private Double anomalyScore;
    private String sort;
    private Boolean descending;

    private GetBucketsRequest() {}

    /**
     * Constructs a request to retrieve buckets of a given job
     * @param jobId id of the job to retrieve buckets of
     */
    public GetBucketsRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId);
    }

    public String getJobId() {
        return jobId;
    }

    /**
     * Sets the timestamp of a specific bucket to be retrieved.
     * @param timestamp String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public boolean isExpand() {
        return expand;
    }

    /**
     * Sets the value of "expand".
     * When {@code true}, buckets will be expanded to include their records.
     * @param expand value of "expand" to be set
     */
    public void setExpand(Boolean expand) {
        this.expand = expand;
    }

    public Boolean getExcludeInterim() {
        return excludeInterim;
    }

    /**
     * Sets the value of "exclude_interim".
     * When {@code true}, interim buckets will be filtered out.
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
     * Only buckets whose timestamp is on or after the "start" value will be returned.
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
     * Only buckets whose timestamp is before the "end" value will be returned.
     * @param end String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setEnd(String end) {
        this.end = end;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    /**
     * Sets the paging parameters
     * @param pageParams the paging parameters
     */
    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public Double getAnomalyScore() {
        return anomalyScore;
    }

    /**
     * Sets the value of "anomaly_score".
     * Only buckets with "anomaly_score" equal or greater will be returned.
     * @param anomalyScore value of "anomaly_score".
     */
    public void setAnomalyScore(Double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }

    public String getSort() {
        return sort;
    }

    /**
     * Sets the value of "sort".
     * Specifies the bucket field to sort on.
     * @param sort value of "sort".
     */
    public void setSort(String sort) {
        this.sort = sort;
    }

    public Boolean getDescending() {
        return descending;
    }

    /**
     * Sets the value of "desc".
     * Specifies the sorting order.
     * @param descending value of "desc"
     */
    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (timestamp != null) {
            builder.field(Result.TIMESTAMP.getPreferredName(), timestamp);
        }
        if (expand != null) {
            builder.field(EXPAND.getPreferredName(), expand);
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
        if (pageParams != null) {
            builder.field(PageParams.PAGE.getPreferredName(), pageParams);
        }
        if (anomalyScore != null) {
            builder.field(ANOMALY_SCORE.getPreferredName(), anomalyScore);
        }
        if (sort != null) {
            builder.field(SORT.getPreferredName(), sort);
        }
        if (descending != null) {
            builder.field(DESCENDING.getPreferredName(), descending);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, expand, excludeInterim, anomalyScore, pageParams, start, end, sort, descending);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetBucketsRequest other = (GetBucketsRequest) obj;
        return Objects.equals(jobId, other.jobId) &&
                Objects.equals(timestamp, other.timestamp) &&
                Objects.equals(expand, other.expand) &&
                Objects.equals(excludeInterim, other.excludeInterim) &&
                Objects.equals(anomalyScore, other.anomalyScore) &&
                Objects.equals(pageParams, other.pageParams) &&
                Objects.equals(start, other.start) &&
                Objects.equals(end, other.end) &&
                Objects.equals(sort, other.sort) &&
                Objects.equals(descending, other.descending);
    }
}
