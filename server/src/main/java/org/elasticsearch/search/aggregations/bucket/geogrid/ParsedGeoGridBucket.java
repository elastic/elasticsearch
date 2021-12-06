/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class ParsedGeoGridBucket extends ParsedMultiBucketAggregation.ParsedBucket implements GeoGrid.Bucket {

    protected String hashAsString;

    @Override
    protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
        return builder.field(Aggregation.CommonFields.KEY.getPreferredName(), hashAsString);
    }
}
