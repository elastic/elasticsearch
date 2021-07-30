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
