/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query to boost scores based on their proximity to the given origin
 * for date, date_nanos and geo_point field types
 */
public class DistanceFeatureQueryBuilder extends AbstractQueryBuilder<DistanceFeatureQueryBuilder> {
    public static final String NAME = "distance_feature";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField ORIGIN_FIELD = new ParseField("origin");
    private static final ParseField PIVOT_FIELD = new ParseField("pivot");

    private final String field;
    private final Origin origin;
    private final String pivot;

    private static final ConstructingObjectParser<DistanceFeatureQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "distance_feature", false,
        args -> new DistanceFeatureQueryBuilder((String) args[0], (Origin) args[1], (String) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        // origin: number or string for date and date_nanos fields; string, array, object for geo fields
        PARSER.declareField(constructorArg(), DistanceFeatureQueryBuilder.Origin::originFromXContent,
            ORIGIN_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER);
        PARSER.declareString(constructorArg(), PIVOT_FIELD);
        declareStandardFields(PARSER);
    }

    public DistanceFeatureQueryBuilder(String field, Origin origin, String pivot) {
        this.field = Objects.requireNonNull(field);
        this.origin = Objects.requireNonNull(origin);
        this.pivot = Objects.requireNonNull(pivot);
    }

    public static DistanceFeatureQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(ORIGIN_FIELD.getPreferredName(), origin.origin);
        builder.field(PIVOT_FIELD.getPreferredName(), pivot);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public DistanceFeatureQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        origin = new Origin(in);
        pivot = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        origin.writeTo(out);
        out.writeString(pivot);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("Can't run [" + NAME + "] query on unmapped fields!");
        }
        return fieldType.distanceFeatureQuery(origin.origin(), pivot, context);
    }

    String fieldName() {
        return field;
    }

    Origin origin() {
        return origin;
    }

    String pivot() {
        return pivot;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, origin, pivot);
    }

    @Override
    protected boolean doEquals(DistanceFeatureQueryBuilder other) {
        return this.field.equals(other.field) && Objects.equals(this.origin, other.origin) && this.pivot.equals(other.pivot);
    }

    public static class Origin {
        private final Object origin;

        public Origin(Long origin) {
            this.origin = Objects.requireNonNull(origin);
        }

        public Origin(String origin) {
            this.origin = Objects.requireNonNull(origin);
        }

        public Origin(GeoPoint origin) {
            this.origin = Objects.requireNonNull(origin);
        }

        private static Origin originFromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return new Origin(parser.longValue());
            } else if(parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new Origin(parser.text());
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                return new Origin(GeoUtils.parseGeoPoint(parser));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                return new Origin(GeoUtils.parseGeoPoint(parser));
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "Illegal type while parsing [origin]! Must be [number] or [string] for date and date_nanos fields;" +
                    " or [string], [array], [object] for geo_point fields!");
            }
        }

        private Origin(StreamInput in) throws IOException {
            origin = in.readGenericValue();
        }

        private void writeTo(final StreamOutput out) throws IOException {
            out.writeGenericValue(origin);
        }

        Object origin() {
            return origin;
        }

        @Override
        public final boolean equals(Object other) {
            if ((other instanceof Origin) == false) {
                return false;
            }
            Object otherOrigin = ((Origin) other).origin();
            return this.origin().equals(otherOrigin);
        }

        @Override
        public int hashCode() {
            return Objects.hash(origin);
        }

        @Override
        public String toString() {
            return origin.toString();
        }
    }
}
