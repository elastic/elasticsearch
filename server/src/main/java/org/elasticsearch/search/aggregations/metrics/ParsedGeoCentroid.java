/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid.Fields;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 */
public class ParsedGeoCentroid extends ParsedAggregation implements GeoCentroid {
    private GeoPoint centroid;
    private long count;

    @Override
    public GeoPoint centroid() {
        return centroid;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public String getType() {
        return GeoCentroidAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(Fields.CENTROID.getPreferredName());
            {
                builder.field(Fields.CENTROID_LAT.getPreferredName(), centroid.lat());
                builder.field(Fields.CENTROID_LON.getPreferredName(), centroid.lon());
            }
            builder.endObject();
        }
        builder.field(Fields.COUNT.getPreferredName(), count);
        return builder;
    }

    private static final ObjectParser<ParsedGeoCentroid, Void> PARSER = new ObjectParser<>(
        ParsedGeoCentroid.class.getSimpleName(),
        true,
        ParsedGeoCentroid::new
    );

    private static final ObjectParser<GeoPoint, Void> GEO_POINT_PARSER = new ObjectParser<>(
        ParsedGeoCentroid.class.getSimpleName() + "_POINT",
        true,
        GeoPoint::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject((agg, centroid) -> agg.centroid = centroid, GEO_POINT_PARSER, Fields.CENTROID);
        PARSER.declareLong((agg, count) -> agg.count = count, Fields.COUNT);

        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLat, Fields.CENTROID_LAT);
        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLon, Fields.CENTROID_LON);
    }

    public static ParsedGeoCentroid fromXContent(XContentParser parser, final String name) {
        ParsedGeoCentroid geoCentroid = PARSER.apply(parser, null);
        geoCentroid.setName(name);
        return geoCentroid;
    }
}
