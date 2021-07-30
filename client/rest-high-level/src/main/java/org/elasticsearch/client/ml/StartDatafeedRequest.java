/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to start a Datafeed
 */
public class StartDatafeedRequest implements Validatable, ToXContentObject {

    public static final ParseField START = new ParseField("start");
    public static final ParseField END = new ParseField("end");
    public static final ParseField TIMEOUT = new ParseField("timeout");

    public static final ConstructingObjectParser<StartDatafeedRequest, Void> PARSER =
        new ConstructingObjectParser<>("start_datafeed_request", a -> new StartDatafeedRequest((String)a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DatafeedConfig.ID);
        PARSER.declareString(StartDatafeedRequest::setStart, START);
        PARSER.declareString(StartDatafeedRequest::setEnd, END);
        PARSER.declareString((params, val) ->
            params.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
    }

    private final String datafeedId;
    private String start;
    private String end;
    private TimeValue timeout;

    /**
     * Create a new StartDatafeedRequest for the given DatafeedId
     *
     * @param datafeedId non-null existing Datafeed ID
     */
    public StartDatafeedRequest(String datafeedId) {
        this.datafeedId = Objects.requireNonNull(datafeedId, "[datafeed_id] must not be null");
    }

    public String getDatafeedId() {
        return datafeedId;
    }

    public String getStart() {
        return start;
    }

    /**
     * The time that the datafeed should begin. This value is inclusive.
     *
     * If you specify a start value that is earlier than the timestamp of the latest processed record,
     * the datafeed continues from 1 millisecond after the timestamp of the latest processed record.
     *
     * If you do not specify a start time and the datafeed is associated with a new job,
     * the analysis starts from the earliest time for which data is available.
     *
     * @param start String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO 8601 string
     */
    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    /**
     * The time that the datafeed should end. This value is exclusive.
     * If you do not specify an end time, the datafeed runs continuously.
     *
     * @param end String representation of a timestamp; may be an epoch seconds, epoch millis or an ISO 8601 string
     */
    public void setEnd(String end) {
        this.end = end;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Indicates how long to wait for the cluster to respond to the request.
     *
     * @param timeout TimeValue for how long to wait for a response from the cluster
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedId, start, end, timeout);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        StartDatafeedRequest other = (StartDatafeedRequest) obj;
        return Objects.equals(datafeedId, other.datafeedId) &&
            Objects.equals(start, other.start) &&
            Objects.equals(end, other.end) &&
            Objects.equals(timeout, other.timeout);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
        if (start != null) {
            builder.field(START.getPreferredName(), start);
        }
        if (end != null) {
            builder.field(END.getPreferredName(), end);
        }
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        builder.endObject();
        return builder;
    }
}
