/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion.context;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.geo.GeoUtils.parsePrecision;
import static org.elasticsearch.search.suggest.completion.context.GeoContextMapping.CONTEXT_BOOST;
import static org.elasticsearch.search.suggest.completion.context.GeoContextMapping.CONTEXT_NEIGHBOURS;
import static org.elasticsearch.search.suggest.completion.context.GeoContextMapping.CONTEXT_PRECISION;
import static org.elasticsearch.search.suggest.completion.context.GeoContextMapping.CONTEXT_VALUE;

/**
 * Defines the query context for {@link GeoContextMapping}
 */
public final class GeoQueryContext implements ToXContentObject {
    public static final String NAME = "geo";

    private final GeoPoint geoPoint;
    private final int boost;
    private final int precision;
    private final List<Integer> neighbours;

    private GeoQueryContext(GeoPoint geoPoint, int boost, int precision, List<Integer> neighbours) {
        this.geoPoint = geoPoint;
        this.boost = boost;
        this.precision = precision;
        this.neighbours = neighbours;
    }

    /**
     * Returns the geo point of the context
     */
    public GeoPoint getGeoPoint() {
        return geoPoint;
    }

    /**
     * Returns the query-time boost of the context
     */
    public int getBoost() {
        return boost;
    }

    /**
     * Returns the precision (length) for the geohash
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns the precision levels at which geohash cells neighbours are considered
     */
    public List<Integer> getNeighbours() {
        return neighbours;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoQueryContext that = (GeoQueryContext) o;

        return Objects.equals(boost, that.boost)
            && Objects.equals(precision, that.precision)
            && Objects.equals(geoPoint, that.geoPoint)
            && Objects.equals(neighbours, that.neighbours);
    }

    @Override
    public int hashCode() {
        int result = geoPoint != null ? geoPoint.hashCode() : 0;
        result = 31 * result + boost;
        result = 31 * result + precision;
        result = 31 * result + (neighbours != null ? neighbours.hashCode() : 0);
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static final ObjectParser<GeoQueryContext.Builder, Void> GEO_CONTEXT_PARSER = new ObjectParser<>(NAME);
    static {
        GEO_CONTEXT_PARSER.declareField(
            (parser, geoQueryContext, geoContextMapping) -> geoQueryContext.setGeoPoint(GeoUtils.parseGeoPoint(parser)),
            new ParseField(CONTEXT_VALUE),
            ObjectParser.ValueType.OBJECT
        );
        GEO_CONTEXT_PARSER.declareInt(GeoQueryContext.Builder::setBoost, new ParseField(CONTEXT_BOOST));
        GEO_CONTEXT_PARSER.declareField(
            (parser, builder, context) -> builder.setPrecision(parsePrecision(parser)),
            new ParseField(CONTEXT_PRECISION),
            ObjectParser.ValueType.INT
        );
        GEO_CONTEXT_PARSER.declareFieldArray(
            GeoQueryContext.Builder::setNeighbours,
            (parser, builder) -> parsePrecision(parser),
            new ParseField(CONTEXT_NEIGHBOURS),
            ObjectParser.ValueType.INT_ARRAY
        );
        GEO_CONTEXT_PARSER.declareDouble(GeoQueryContext.Builder::setLat, new ParseField("lat"));
        GEO_CONTEXT_PARSER.declareDouble(GeoQueryContext.Builder::setLon, new ParseField("lon"));
    }

    public static GeoQueryContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        GeoQueryContext.Builder builder = new Builder();
        if (token == XContentParser.Token.START_OBJECT) {
            GEO_CONTEXT_PARSER.parse(parser, builder, null);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            builder.setGeoPoint(GeoPoint.fromGeohash(parser.text()));
        } else {
            throw new ElasticsearchParseException("geo context must be an object or string");
        }
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(CONTEXT_VALUE);
        builder.field("lat", geoPoint.getLat());
        builder.field("lon", geoPoint.getLon());
        builder.endObject();
        builder.field(CONTEXT_BOOST, boost);
        builder.field(CONTEXT_NEIGHBOURS, neighbours);
        builder.field(CONTEXT_PRECISION, precision);
        builder.endObject();
        return builder;
    }

    public static class Builder {
        private GeoPoint geoPoint;
        private int boost = 1;
        private int precision = 12;
        private List<Integer> neighbours = Collections.emptyList();

        public Builder() {}

        /**
         * Sets the query-time boost for the context
         * Defaults to 1
         */
        public Builder setBoost(int boost) {
            if (boost <= 0) {
                throw new IllegalArgumentException("boost must be greater than 0");
            }
            this.boost = boost;
            return this;
        }

        /**
         * Sets the precision level for computing the geohash from the context geo point.
         * Defaults to using index-time precision level
         */
        public Builder setPrecision(int precision) {
            if (precision < 1 || precision > 12) {
                throw new IllegalArgumentException("precision must be between 1 and 12");
            }
            this.precision = precision;
            return this;
        }

        /**
         * Sets the precision levels at which geohash cells neighbours are considered.
         * Defaults to only considering neighbours at the index-time precision level
         */
        public Builder setNeighbours(List<Integer> neighbours) {
            for (int neighbour : neighbours) {
                if (neighbour < 1 || neighbour > 12) {
                    throw new IllegalArgumentException("neighbour value must be between 1 and 12");
                }
            }
            this.neighbours = neighbours;
            return this;
        }

        /**
         * Sets the geo point of the context.
         * This is a required field
         */
        public Builder setGeoPoint(GeoPoint geoPoint) {
            Objects.requireNonNull(geoPoint, "geoPoint must not be null");
            this.geoPoint = geoPoint;
            return this;
        }

        private double lat = Double.NaN;

        void setLat(double lat) {
            this.lat = lat;
        }

        private double lon = Double.NaN;

        void setLon(double lon) {
            this.lon = lon;
        }

        public GeoQueryContext build() {
            if (geoPoint == null) {
                if (Double.isNaN(lat) == false && Double.isNaN(lon) == false) {
                    geoPoint = new GeoPoint(lat, lon);
                }
            }
            Objects.requireNonNull(geoPoint, "geoPoint must not be null");
            return new GeoQueryContext(geoPoint, boost, precision, neighbours);
        }
    }
}
