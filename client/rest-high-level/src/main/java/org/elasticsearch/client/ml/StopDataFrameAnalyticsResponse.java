/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Response indicating if the Machine Learning Data Frame Analytics is now stopped or not
 */
public class StopDataFrameAnalyticsResponse implements ToXContentObject {

    private static final ParseField STOPPED = new ParseField("stopped");

    public static final ConstructingObjectParser<StopDataFrameAnalyticsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "stop_data_frame_analytics_response",
            true,
            args -> new StopDataFrameAnalyticsResponse((Boolean) args[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), STOPPED);
    }

    public static StopDataFrameAnalyticsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final boolean stopped;

    public StopDataFrameAnalyticsResponse(boolean stopped) {
        this.stopped = stopped;
    }

    /**
     * Has the Data Frame Analytics stopped or not
     *
     * @return boolean value indicating the Data Frame Analytics stopped status
     */
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StopDataFrameAnalyticsResponse other = (StopDataFrameAnalyticsResponse) o;
        return stopped == other.stopped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stopped);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(STOPPED.getPreferredName(), stopped)
            .endObject();
    }
}
