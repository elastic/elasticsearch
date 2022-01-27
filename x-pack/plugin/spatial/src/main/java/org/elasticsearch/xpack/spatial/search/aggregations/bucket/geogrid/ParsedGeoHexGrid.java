/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoGrid;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ParsedGeoHexGrid extends ParsedGeoGrid {

    private static final ObjectParser<ParsedGeoGrid, Void> PARSER = createParser(
        ParsedGeoHexGrid::new,
        ParsedGeoHexGridBucket::fromXContent,
        ParsedGeoHexGridBucket::fromXContent
    );

    public static ParsedGeoGrid fromXContent(XContentParser parser, String name) throws IOException {
        ParsedGeoGrid aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    @Override
    public String getType() {
        return GeoHexGridAggregationBuilder.NAME;
    }
}
