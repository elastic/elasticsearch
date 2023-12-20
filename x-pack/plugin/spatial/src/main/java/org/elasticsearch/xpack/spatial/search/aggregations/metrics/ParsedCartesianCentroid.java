/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalCentroid;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.search.aggregations.metrics.InternalCartesianCentroid.Fields;

import java.io.IOException;

/**
 * Serialization and merge logic for {@link CartesianCentroidAggregator}.
 */
public class ParsedCartesianCentroid extends ParsedAggregation implements CartesianCentroid {
    private CartesianPoint centroid;
    private long count;

    @Override
    public CartesianPoint centroid() {
        return centroid;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public String getType() {
        return CartesianCentroidAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(InternalCentroid.Fields.CENTROID.getPreferredName());
            {
                builder.field(Fields.CENTROID_X.getPreferredName(), centroid.getX());
                builder.field(Fields.CENTROID_Y.getPreferredName(), centroid.getY());
            }
            builder.endObject();
        }
        builder.field(InternalCentroid.Fields.COUNT.getPreferredName(), count);
        return builder;
    }

    private static final ObjectParser<ParsedCartesianCentroid, Void> PARSER = new ObjectParser<>(
        ParsedCartesianCentroid.class.getSimpleName(),
        true,
        ParsedCartesianCentroid::new
    );

    private static final ObjectParser<CartesianPoint, Void> CARTESIAN_POINT_PARSER = new ObjectParser<>(
        ParsedCartesianCentroid.class.getSimpleName() + "_POINT",
        true,
        CartesianPoint::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject((agg, centroid) -> agg.centroid = centroid, CARTESIAN_POINT_PARSER, InternalCentroid.Fields.CENTROID);
        PARSER.declareLong((agg, count) -> agg.count = count, InternalCentroid.Fields.COUNT);

        CARTESIAN_POINT_PARSER.declareDouble(CartesianPoint::resetX, Fields.CENTROID_X);
        CARTESIAN_POINT_PARSER.declareDouble(CartesianPoint::resetY, Fields.CENTROID_Y);
    }

    public static ParsedCartesianCentroid fromXContent(XContentParser parser, final String name) {
        ParsedCartesianCentroid centroid = PARSER.apply(parser, null);
        centroid.setName(name);
        return centroid;
    }
}
