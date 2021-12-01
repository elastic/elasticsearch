/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class ExtendedStatsBucketParser extends BucketMetricsParser {
    static final ParseField SIGMA = new ParseField("sigma");

    @Override
    protected ExtendedStatsBucketPipelineAggregationBuilder buildFactory(
        String pipelineAggregatorName,
        String bucketsPath,
        Map<String, Object> params
    ) {
        ExtendedStatsBucketPipelineAggregationBuilder factory = new ExtendedStatsBucketPipelineAggregationBuilder(
            pipelineAggregatorName,
            bucketsPath
        );
        Double sigma = (Double) params.get(SIGMA.getPreferredName());
        if (sigma != null) {
            factory.sigma(sigma);
        }

        return factory;
    }

    @Override
    protected boolean token(XContentParser parser, String field, XContentParser.Token token, Map<String, Object> params)
        throws IOException {
        if (SIGMA.match(field, parser.getDeprecationHandler()) && token == XContentParser.Token.VALUE_NUMBER) {
            params.put(SIGMA.getPreferredName(), parser.doubleValue());
            return true;
        }
        return false;
    }
}
