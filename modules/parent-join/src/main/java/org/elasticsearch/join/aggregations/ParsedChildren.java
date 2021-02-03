/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.join.aggregations;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.io.IOException;

public class ParsedChildren extends ParsedSingleBucketAggregation implements Children {

    @Override
    public String getType() {
        return ChildrenAggregationBuilder.NAME;
    }

    public static ParsedChildren fromXContent(XContentParser parser, final String name) throws IOException {
        return parseXContent(parser, new ParsedChildren(), name);
    }
}
