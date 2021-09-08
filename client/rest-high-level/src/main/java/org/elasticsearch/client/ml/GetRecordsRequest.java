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
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to retrieve records of a given job
 */
public class GetRecordsRequest implements ToXContentObject, Validatable {

    public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField RECORD_SCORE = new ParseField("record_score");
    public static final ParseField SORT = new ParseField("sort");
    public static final ParseField DESCENDING = new ParseField("desc");

    public static final ObjectParser<GetRecordsRequest, Void> PARSER = new ObjectParser<>("get_records_request", GetRecordsRequest::new);

    static {
        PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
        PARSER.declareBoolean(GetRecordsRequest::setExcludeInterim, EXCLUDE_INTERIM);
        PARSER.declareStringOrNull(GetRecordsRequest::setStart, START);
        PARSER.declareStringOrNull(GetRecordsRequest::setEnd, END);
        PARSER.declareObject(GetRecordsRequest::setPageParams, PageParams.PARSER, PageParams.PAGE);
        PARSER.declareDouble(GetRecordsRequest::setRecordScore, RECORD_SCORE);
        PARSER.declareString(GetRecordsRequest::setSort, SORT);
        PARSER.declareBoolean(GetRecordsRequest::setDescending, DESCENDING);
    }

    private String jobId;
    private Boolean excludeInterim;
    private String start;
    private String end;
    private PageParams pageParams;
    private Double recordScore;
    private String sort;
    private Boolean descending;

    private GetRecordsRequest() {}

    /**
     * Constructs a request to retrieve records of a given job
     * @param jobId id of the job to retrieve records of
     */
    public GetRecordsRequest(String jobId) {
        this.jobId = Objects.requireNonNull(jobId);
    }

    public String getJobId() {
        return jobId;
    }

    public Boolean getExcludeInterim() {
        return excludeInterim;
    }

    /**
     * Sets the value of "exclude_interim".
     * When {@code true}, interim records will be filtered out.
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
     * Only records whose timestamp is on or after the "start" value will be returned.
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
     * Only records whose timestamp is before the "end" value will be returned.
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
     * @param pageParams The paging parameters
     */
    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public Double getRecordScore() {
        return recordScore;
    }

    /**
     * Sets the value of "record_score".
     * Only records with "record_score" equal or greater will be returned.
     * @param recordScore value of "record_score".
     */
    public void setRecordScore(Double recordScore) {
        this.recordScore = recordScore;
    }

    public String getSort() {
        return sort;
    }

    /**
     * Sets the value of "sort".
     * Specifies the record field to sort on.
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
    public void setDescending(Boolean descending) {
        this.descending = descending;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
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
        if (recordScore != null) {
            builder.field(RECORD_SCORE.getPreferredName(), recordScore);
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
        return Objects.hash(jobId, excludeInterim, recordScore, pageParams, start, end, sort, descending);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetRecordsRequest other = (GetRecordsRequest) obj;
        return Objects.equals(jobId, other.jobId) &&
                Objects.equals(excludeInterim, other.excludeInterim) &&
                Objects.equals(recordScore, other.recordScore) &&
                Objects.equals(pageParams, other.pageParams) &&
                Objects.equals(start, other.start) &&
                Objects.equals(end, other.end) &&
                Objects.equals(sort, other.sort) &&
                Objects.equals(descending, other.descending);
    }
}
