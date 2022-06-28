/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class RunningState implements ToXContentObject {

    private static final ParseField REAL_TIME_CONFIGURED = new ParseField("real_time_configured");
    private static final ParseField REAL_TIME_RUNNING = new ParseField("real_time_running");

    public static final ConstructingObjectParser<RunningState, Void> PARSER = new ConstructingObjectParser<>(
        "datafeed_running_state",
        true,
        a -> new RunningState((Boolean) a[0], (Boolean) a[1])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), REAL_TIME_CONFIGURED);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), REAL_TIME_RUNNING);
    }

    // Is the datafeed a "realtime" datafeed, meaning it was started without an end_time
    private final boolean realTimeConfigured;
    // Has the reading historical data has finished and are we now running on "real-time" data
    private final boolean realTimeRunning;

    public RunningState(boolean realTimeConfigured, boolean realTimeRunning) {
        this.realTimeConfigured = realTimeConfigured;
        this.realTimeRunning = realTimeRunning;
    }

    /**
     * Indicates if the datafeed is configured to run in real time
     *
     * @return true if the datafeed is configured to run in real time.
     */
    public boolean isRealTimeConfigured() {
        return realTimeConfigured;
    }

    /**
     * Indicates if the datafeed has processed all historical data available at the start time and is now processing "real-time" data.
     * @return true if the datafeed is now running in real-time
     */
    public boolean isRealTimeRunning() {
        return realTimeRunning;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RunningState that = (RunningState) o;
        return realTimeConfigured == that.realTimeConfigured && realTimeRunning == that.realTimeRunning;
    }

    @Override
    public int hashCode() {
        return Objects.hash(realTimeConfigured, realTimeRunning);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REAL_TIME_CONFIGURED.getPreferredName(), realTimeConfigured);
        builder.field(REAL_TIME_RUNNING.getPreferredName(), realTimeRunning);
        builder.endObject();
        return builder;
    }

}
