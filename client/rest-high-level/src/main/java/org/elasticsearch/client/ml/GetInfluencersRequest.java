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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A request to retrieve influencers of a given job
 */
public class GetInfluencersRequest implements Validatable, ToXContentObject {

    public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField INFLUENCER_SCORE = new ParseField("influencer_score");
    public static final ParseField SORT = new ParseField("sort");
    public static final ParseField DESCENDING = new ParseField("desc");

    public static final ConstructingObjectParser<GetInfluencersRequest, Void> PARSER = new ConstructingObjectParser<>(
            "get_influencers_request", a -> new GetInfluencersRequest((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareBoolean(GetInfluencersRequest::setExcludeInterim, EXCLUDE_INTERIM);
        PARSER.declareStringOrNull(GetInfluencersRequest::setStart, START);
        PARSER.declareStringOrNull(GetInfluencersRequest::setEnd, END);
        PARSER.declareObject(GetInfluencersRequest::setPageParams, PageParams.PARSER, PageParams.PAGE);
        PARSER.declareDouble(GetInfluencersRequest::setInfluencerScore, INFLUENCER_SCORE);
        PARSER.declareString(GetInfluencersRequest::setSort, SORT);
        PARSER.declareBoolean(GetInfluencersRequest::setDescending, DESCENDING);
    }

    private final String jobId;
    private Boolean excludeInterim;
    private String start;
    private String end;
    private Double influencerScore;
    private PageParams pageParams;
    private String sort;
    private Boolean descending;

    /**
     * Constructs a request to retrieve influencers of a given job
     * @param jobId id of the job to retrieve influencers of
     */
    public GetInfluencersRequest(String jobId) {
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
     * When {@code true}, interim influencers will be filtered out.
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
     * Only influencers whose timestamp is on or after the "start" value will be returned.
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
     * Only influencers whose timestamp is before the "end" value will be returned.
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

    public Double getInfluencerScore() {
        return influencerScore;
    }

    /**
     * Sets the value of "influencer_score".
     * Only influencers with "influencer_score" equal or greater will be returned.
     * @param influencerScore value of "influencer_score".
     */
    public void setInfluencerScore(Double influencerScore) {
        this.influencerScore = influencerScore;
    }

    public String getSort() {
        return sort;
    }

    /**
     * Sets the value of "sort".
     * Specifies the influencer field to sort on.
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
        if (influencerScore != null) {
            builder.field(INFLUENCER_SCORE.getPreferredName(), influencerScore);
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
        return Objects.hash(jobId, excludeInterim, influencerScore, pageParams, start, end, sort, descending);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetInfluencersRequest other = (GetInfluencersRequest) obj;
        return Objects.equals(jobId, other.jobId) &&
                Objects.equals(excludeInterim, other.excludeInterim) &&
                Objects.equals(influencerScore, other.influencerScore) &&
                Objects.equals(pageParams, other.pageParams) &&
                Objects.equals(start, other.start) &&
                Objects.equals(end, other.end) &&
                Objects.equals(sort, other.sort) &&
                Objects.equals(descending, other.descending);
    }
}
