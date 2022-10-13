/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.geo.GeoBoundingBox.BOTTOM_RIGHT_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.BOUNDS_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.LAT_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.LON_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.TOP_LEFT_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ParsedGeoBounds extends ParsedAggregation implements GeoBounds {

    // A top of Double.NEGATIVE_INFINITY yields an empty xContent, so the bounding box is null
    @Nullable
    private GeoBoundingBox geoBoundingBox;

    @Override
    public String getType() {
        return GeoBoundsAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (geoBoundingBox != null) {
            builder.startObject(GeoBoundingBox.BOUNDS_FIELD.getPreferredName());
            geoBoundingBox.toXContentFragment(builder);
            builder.endObject();
        }
        return builder;
    }

    @Override
    @Nullable
    public GeoPoint topLeft() {
        return geoBoundingBox != null ? geoBoundingBox.topLeft() : null;
    }

    @Override
    @Nullable
    public GeoPoint bottomRight() {
        return geoBoundingBox != null ? geoBoundingBox.bottomRight() : null;
    }

    private static final ObjectParser<ParsedGeoBounds, Void> PARSER = new ObjectParser<>(
        ParsedGeoBounds.class.getSimpleName(),
        true,
        ParsedGeoBounds::new
    );

    private static final ConstructingObjectParser<Tuple<GeoPoint, GeoPoint>, Void> BOUNDS_PARSER = new ConstructingObjectParser<>(
        ParsedGeoBounds.class.getSimpleName() + "_BOUNDS",
        true,
        args -> new Tuple<>((GeoPoint) args[0], (GeoPoint) args[1])
    );

    private static final ObjectParser<GeoPoint, Void> GEO_POINT_PARSER = new ObjectParser<>(
        ParsedGeoBounds.class.getSimpleName() + "_POINT",
        true,
        GeoPoint::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject(
            (agg, bbox) -> { agg.geoBoundingBox = new GeoBoundingBox(bbox.v1(), bbox.v2()); },
            BOUNDS_PARSER,
            BOUNDS_FIELD
        );

        BOUNDS_PARSER.declareObject(constructorArg(), GEO_POINT_PARSER, TOP_LEFT_FIELD);
        BOUNDS_PARSER.declareObject(constructorArg(), GEO_POINT_PARSER, BOTTOM_RIGHT_FIELD);

        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLat, LAT_FIELD);
        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLon, LON_FIELD);
    }

    public static ParsedGeoBounds fromXContent(XContentParser parser, final String name) {
        ParsedGeoBounds geoBounds = PARSER.apply(parser, null);
        geoBounds.setName(name);
        return geoBounds;
    }

}
