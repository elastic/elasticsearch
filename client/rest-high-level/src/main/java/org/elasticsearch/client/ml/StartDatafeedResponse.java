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
 * Response indicating if the Machine Learning Datafeed is now started or not
 */
public class StartDatafeedResponse implements ToXContentObject {

    private static final ParseField STARTED = new ParseField("started");

    public static final ConstructingObjectParser<StartDatafeedResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "start_datafeed_response",
            true,
            (a) -> new StartDatafeedResponse((Boolean)a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), STARTED);
    }

    private final boolean started;

    public StartDatafeedResponse(boolean started) {
        this.started = started;
    }

    public static StartDatafeedResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Has the Datafeed started or not
     *
     * @return boolean value indicating the Datafeed started status
     */
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        StartDatafeedResponse that = (StartDatafeedResponse) other;
        return isStarted() == that.isStarted();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isStarted());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STARTED.getPreferredName(), started);
        builder.endObject();
        return builder;
    }
}
