/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request object to flush a given Machine Learning job.
 */
public class FlushJobRequest implements Validatable, ToXContentObject {

    public static final ParseField CALC_INTERIM = new ParseField("calc_interim");
    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField ADVANCE_TIME = new ParseField("advance_time");
    public static final ParseField SKIP_TIME = new ParseField("skip_time");

    public static final ConstructingObjectParser<FlushJobRequest, Void> PARSER =
        new ConstructingObjectParser<>("flush_job_request", (a) -> new FlushJobRequest((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareBoolean(FlushJobRequest::setCalcInterim, CALC_INTERIM);
        PARSER.declareString(FlushJobRequest::setStart, START);
        PARSER.declareString(FlushJobRequest::setEnd, END);
        PARSER.declareString(FlushJobRequest::setAdvanceTime, ADVANCE_TIME);
        PARSER.declareString(FlushJobRequest::setSkipTime, SKIP_TIME);
    }

    private final String jobId;
    private Boolean calcInterim;
    private String start;
    private String end;
    private String advanceTime;
    private String skipTime;

    /**
     * Create new Flush job request
     *
     * @param jobId The job ID of the job to flush
     */
    public FlushJobRequest(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public boolean getCalcInterim() {
        return calcInterim;
    }

    /**
     * When {@code true} calculates the interim results for the most recent bucket or all buckets within the latency period.
     *
     * @param calcInterim defaults to {@code false}.
     */
    public void setCalcInterim(boolean calcInterim) {
        this.calcInterim = calcInterim;
    }

    public String getStart() {
        return start;
    }

    /**
     * When used in conjunction with {@link FlushJobRequest#calcInterim},
     * specifies the start of the range of buckets on which to calculate interim results.
     *
     * @param start the beginning of the range of buckets; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    /**
     * When used in conjunction with {@link FlushJobRequest#calcInterim}, specifies the end of the range
     * of buckets on which to calculate interim results
     *
     * @param end the end of the range of buckets; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setEnd(String end) {
        this.end = end;
    }

    public String getAdvanceTime() {
        return advanceTime;
    }

    /**
     * Specifies to advance to a particular time value.
     * Results are generated and the model is updated for data from the specified time interval.
     *
     * @param advanceTime String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setAdvanceTime(String advanceTime) {
        this.advanceTime = advanceTime;
    }

    public String getSkipTime() {
        return skipTime;
    }

    /**
     * Specifies to skip to a particular time value.
     * Results are not generated and the model is not updated for data from the specified time interval.
     *
     * @param skipTime String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO string
     */
    public void setSkipTime(String skipTime) {
        this.skipTime = skipTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, calcInterim, start, end, advanceTime, skipTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FlushJobRequest other = (FlushJobRequest) obj;
        return Objects.equals(jobId, other.jobId) &&
            calcInterim == other.calcInterim &&
            Objects.equals(start, other.start) &&
            Objects.equals(end, other.end) &&
            Objects.equals(advanceTime, other.advanceTime) &&
            Objects.equals(skipTime, other.skipTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (calcInterim != null) {
            builder.field(CALC_INTERIM.getPreferredName(), calcInterim);
        }
        if (start != null) {
            builder.field(START.getPreferredName(), start);
        }
        if (end != null) {
            builder.field(END.getPreferredName(), end);
        }
        if (advanceTime != null) {
            builder.field(ADVANCE_TIME.getPreferredName(), advanceTime);
        }
        if (skipTime != null) {
            builder.field(SKIP_TIME.getPreferredName(), skipTime);
        }
        builder.endObject();
        return builder;
    }

}
