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

import org.elasticsearch.common.ParseField;
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
