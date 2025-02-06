/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A parser for parsing requests for a {@link BucketMetricsPipelineAggregator}
 */
public abstract class BucketMetricsParser implements PipelineAggregator.Parser {

    public static final ParseField FORMAT = new ParseField("format");

    public BucketMetricsParser() {
        super();
    }

    @Override
    public final BucketMetricsPipelineAggregationBuilder<?> parse(String pipelineAggregatorName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        GapPolicy gapPolicy = null;
        Map<String, Object> params = Maps.newMapWithExpectedSize(5);

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName, parser.getDeprecationHandler())) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName, parser.getDeprecationHandler())) {
                    gapPolicy = GapPolicy.parse(parser.text(), parser.getTokenLocation());
                } else {
                    parseToken(pipelineAggregatorName, parser, currentFieldName, token, params);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<String> paths = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    parseToken(pipelineAggregatorName, parser, currentFieldName, token, params);
                }
            } else {
                parseToken(pipelineAggregatorName, parser, currentFieldName, token, params);
            }
        }

        if (bucketsPaths == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Missing required field [" + BUCKETS_PATH.getPreferredName() + "] for aggregation [" + pipelineAggregatorName + "]"
            );
        }

        BucketMetricsPipelineAggregationBuilder<?> factory = buildFactory(pipelineAggregatorName, bucketsPaths[0], params);
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }

        assert (factory != null);

        return factory;
    }

    protected abstract BucketMetricsPipelineAggregationBuilder<?> buildFactory(
        String pipelineAggregatorName,
        String bucketsPaths,
        Map<String, Object> params
    );

    protected boolean token(XContentParser parser, String field, XContentParser.Token token, Map<String, Object> params)
        throws IOException {
        return false;
    }

    private void parseToken(
        String aggregationName,
        XContentParser parser,
        String currentFieldName,
        XContentParser.Token currentToken,
        Map<String, Object> params
    ) throws IOException {
        if (token(parser, currentFieldName, currentToken, params) == false) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Unexpected token " + currentToken + " [" + currentFieldName + "] in [" + aggregationName + "]"
            );
        }
    }
}
