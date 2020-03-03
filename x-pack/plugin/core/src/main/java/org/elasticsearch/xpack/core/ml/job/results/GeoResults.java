/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GeoResults implements ToXContentObject, Writeable {

    public static final ParseField GEO_RESULTS = new ParseField("geo_results");

    public static final ParseField TYPICAL_POINT = new ParseField("typical_point");
    public static final ParseField ACTUAL_POINT = new ParseField("actual_point");

    public static final ObjectParser<GeoResults, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<GeoResults, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<GeoResults, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<GeoResults, Void> parser = new ObjectParser<>(GEO_RESULTS.getPreferredName(), ignoreUnknownFields,
            GeoResults::new);
        parser.declareString(GeoResults::setActualPoint, ACTUAL_POINT);
        parser.declareString(GeoResults::setTypicalPoint, TYPICAL_POINT);
        return parser;
    }

    private String actualPoint;
    private String typicalPoint;

    public GeoResults() {}

    public GeoResults(StreamInput in) throws IOException {
        this.actualPoint = in.readOptionalString();
        this.typicalPoint = in.readOptionalString();
    }

    public String getActualPoint() {
        return actualPoint;
    }

    public void setActualPoint(String actualPoint) {
        this.actualPoint = actualPoint;
    }

    public String getTypicalPoint() {
        return typicalPoint;
    }

    public void setTypicalPoint(String typicalPoint) {
        this.typicalPoint = typicalPoint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(actualPoint);
        out.writeOptionalString(typicalPoint);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (typicalPoint != null) {
            builder.field(TYPICAL_POINT.getPreferredName(), typicalPoint);
        }
        if (actualPoint != null) {
            builder.field(ACTUAL_POINT.getPreferredName(), actualPoint);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typicalPoint, actualPoint);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        GeoResults that = (GeoResults) other;
        return Objects.equals(this.typicalPoint, that.typicalPoint) && Objects.equals(this.actualPoint, that.actualPoint);
    }

}
