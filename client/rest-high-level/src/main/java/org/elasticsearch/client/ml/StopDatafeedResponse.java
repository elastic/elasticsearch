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
 * Response indicating if the Machine Learning Datafeed is now stopped or not
 */
public class StopDatafeedResponse implements ToXContentObject {

    private static final ParseField STOPPED = new ParseField("stopped");

    public static final ConstructingObjectParser<StopDatafeedResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "stop_datafeed_response",
            true,
            (a) -> new StopDatafeedResponse((Boolean)a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), STOPPED);
    }

    private final boolean stopped;

    public StopDatafeedResponse(boolean stopped) {
        this.stopped = stopped;
    }

    public static StopDatafeedResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Has the Datafeed stopped or not
     *
     * @return boolean value indicating the Datafeed stopped status
     */
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        StopDatafeedResponse that = (StopDatafeedResponse) other;
        return isStopped() == that.isStopped();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isStopped());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STOPPED.getPreferredName(), stopped);
        builder.endObject();
        return builder;
    }
}
