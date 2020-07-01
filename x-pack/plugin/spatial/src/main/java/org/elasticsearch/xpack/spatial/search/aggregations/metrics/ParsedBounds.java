/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xpack.spatial.common.CartesianBoundingBox;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;

import static org.elasticsearch.common.geo.GeoBoundingBox.BOTTOM_RIGHT_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.BOUNDS_FIELD;
import static org.elasticsearch.common.geo.GeoBoundingBox.TOP_LEFT_FIELD;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ParsedBounds extends ParsedAggregation implements Bounds {

    // A top of Double.NEGATIVE_INFINITY yields an empty xContent, so the bounding box is null
    @Nullable
    private CartesianBoundingBox box;

    @Override
    public String getType() {
        return BoundsAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (box != null) {
            box.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    @Nullable
    public CartesianPoint topLeft() {
        return box != null ? box.topLeft() : null;
    }

    @Override
    @Nullable
    public CartesianPoint bottomRight() {
        return box != null ? box.bottomRight() : null;
    }

    private static final ObjectParser<ParsedBounds, Void> PARSER = new ObjectParser<>(ParsedBounds.class.getSimpleName(), true,
            ParsedBounds::new);

    private static final ConstructingObjectParser<Tuple<CartesianPoint, CartesianPoint>, Void> BOUNDS_PARSER =
            new ConstructingObjectParser<>(ParsedBounds.class.getSimpleName() + "_BOUNDS", true,
                    args -> new Tuple<>((CartesianPoint) args[0], (CartesianPoint) args[1]));

    private static final ObjectParser<CartesianPoint, Void> POINT_PARSER = new ObjectParser<>(
            ParsedBounds.class.getSimpleName() + "_POINT", true, CartesianPoint::new);

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject((agg, bbox) -> {
            agg.box = new CartesianBoundingBox(bbox.v1(), bbox.v2());
        }, BOUNDS_PARSER, BOUNDS_FIELD);

        BOUNDS_PARSER.declareObject(constructorArg(), POINT_PARSER, TOP_LEFT_FIELD);
        BOUNDS_PARSER.declareObject(constructorArg(), POINT_PARSER, BOTTOM_RIGHT_FIELD);

        POINT_PARSER.declareFloat(CartesianPoint::resetX,  new ParseField("x"));
        POINT_PARSER.declareFloat(CartesianPoint::resetY,  new ParseField("y"));
    }

    public static ParsedAggregation parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static ParsedBounds fromXContent(XContentParser parser, final String name) {
        ParsedBounds geoBounds = PARSER.apply(parser, null);
        geoBounds.setName(name);
        return geoBounds;
    }

}
