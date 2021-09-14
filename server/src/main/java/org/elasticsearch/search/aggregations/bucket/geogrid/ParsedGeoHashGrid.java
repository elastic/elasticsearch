/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedGeoHashGrid extends ParsedGeoGrid {

    private static final ObjectParser<ParsedGeoGrid, Void> PARSER = createParser(
        ParsedGeoHashGrid::new,
        ParsedGeoHashGridBucket::fromXContent,
        ParsedGeoHashGridBucket::fromXContent
    );

    public static ParsedGeoGrid fromXContent(XContentParser parser, String name) throws IOException {
        ParsedGeoGrid aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    @Override
    public String getType() {
        return GeoHashGridAggregationBuilder.NAME;
    }
}
