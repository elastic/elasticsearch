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
