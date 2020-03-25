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
 * Forecast response object
 */
public class ForecastJobResponse implements ToXContentObject {

    public static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
    public static final ParseField FORECAST_ID = new ParseField("forecast_id");

    public static final ConstructingObjectParser<ForecastJobResponse, Void> PARSER =
        new ConstructingObjectParser<>("forecast_job_response",
            true,
            (a) -> new ForecastJobResponse((Boolean)a[0], (String)a[1]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ACKNOWLEDGED);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FORECAST_ID);
    }

    public static ForecastJobResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final boolean acknowledged;
    private final String forecastId;

    public ForecastJobResponse(boolean acknowledged, String forecastId) {
        this.acknowledged = acknowledged;
        this.forecastId = forecastId;
    }

    /**
     * Forecast creating acknowledgement
     * @return {@code true} indicates success, {@code false} otherwise
     */
    public boolean isAcknowledged() {
        return acknowledged;
    }

    /**
     * The created forecast ID
     */
    public String getForecastId() {
        return forecastId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged, forecastId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ForecastJobResponse other = (ForecastJobResponse) obj;
        return Objects.equals(acknowledged, other.acknowledged)
            && Objects.equals(forecastId, other.forecastId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), acknowledged);
        builder.field(FORECAST_ID.getPreferredName(), forecastId);
        builder.endObject();
        return builder;
    }
}
