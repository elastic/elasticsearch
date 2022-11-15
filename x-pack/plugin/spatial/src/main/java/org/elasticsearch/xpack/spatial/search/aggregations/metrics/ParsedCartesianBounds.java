/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;

import static org.elasticsearch.common.geo.GeoBoundingBox.BOTTOM_RIGHT_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.BOUNDS_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.TOP_LEFT_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.spatial.common.CartesianBoundingBox.X_FIELD;
import static org.elasticsearch.xpack.spatial.common.CartesianBoundingBox.Y_FIELD;

public class ParsedCartesianBounds extends ParsedAggregation implements CartesianBounds {

    // A top of Double.NEGATIVE_INFINITY yields an empty xContent, so the bounding box is null
    @Nullable
    private CartesianBoundingBox boundingBox;

    @Override
    public String getType() {
        return CartesianBoundsAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (boundingBox != null) {
            builder.startObject(CartesianBoundingBox.BOUNDS_FIELD.getPreferredName());
            boundingBox.toXContentFragment(builder);
            builder.endObject();
        }
        return builder;
    }

    @Override
    @Nullable
    public CartesianPoint topLeft() {
        return boundingBox != null ? boundingBox.topLeft() : null;
    }

    @Override
    @Nullable
    public CartesianPoint bottomRight() {
        return boundingBox != null ? boundingBox.bottomRight() : null;
    }

    private static final ObjectParser<ParsedCartesianBounds, Void> PARSER = new ObjectParser<>(
        ParsedCartesianBounds.class.getSimpleName(),
        true,
        ParsedCartesianBounds::new
    );

    private static final ConstructingObjectParser<Tuple<CartesianPoint, CartesianPoint>, Void> BOUNDS_PARSER =
        new ConstructingObjectParser<>(
            ParsedCartesianBounds.class.getSimpleName() + "_BOUNDS",
            true,
            args -> new Tuple<>((CartesianPoint) args[0], (CartesianPoint) args[1])
        );

    private static final ObjectParser<CartesianPoint, Void> POINT_PARSER = new ObjectParser<>(
        ParsedCartesianBounds.class.getSimpleName() + "_POINT",
        true,
        CartesianPoint::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject((agg, bbox) -> agg.boundingBox = new CartesianBoundingBox(bbox.v1(), bbox.v2()), BOUNDS_PARSER, BOUNDS_FIELD);

        BOUNDS_PARSER.declareObject(constructorArg(), POINT_PARSER, TOP_LEFT_FIELD);
        BOUNDS_PARSER.declareObject(constructorArg(), POINT_PARSER, BOTTOM_RIGHT_FIELD);

        POINT_PARSER.declareDouble(CartesianPoint::resetY, Y_FIELD);
        POINT_PARSER.declareDouble(CartesianPoint::resetX, X_FIELD);
    }

    public static ParsedCartesianBounds fromXContent(XContentParser parser, final String name) {
        ParsedCartesianBounds geoBounds = PARSER.apply(parser, null);
        geoBounds.setName(name);
        return geoBounds;
    }

}
