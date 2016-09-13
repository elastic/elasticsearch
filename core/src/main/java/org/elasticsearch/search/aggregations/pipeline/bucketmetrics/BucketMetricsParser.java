/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    public final BucketMetricsPipelineAggregationBuilder<?> parse(String pipelineAggregatorName, QueryParseContext context)
            throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        GapPolicy gapPolicy = null;
        Map<String, Object> params = new HashMap<>(5);

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.getParseFieldMatcher().match(currentFieldName, FORMAT)) {
                    format = parser.text();
                } else if (context.getParseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (context.getParseFieldMatcher().match(currentFieldName, GAP_POLICY)) {
                    gapPolicy = GapPolicy.parse(context, parser.text(), parser.getTokenLocation());
                } else {
                    parseToken(pipelineAggregatorName, parser, context, currentFieldName, token, params);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.getParseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    parseToken(pipelineAggregatorName, parser, context, currentFieldName, token, params);
                }
            } else {
                parseToken(pipelineAggregatorName, parser, context, currentFieldName, token, params);
            }
        }

        if (bucketsPaths == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Missing required field [" + BUCKETS_PATH.getPreferredName() + "] for aggregation [" + pipelineAggregatorName + "]");
        }

        BucketMetricsPipelineAggregationBuilder<?> factory  = buildFactory(pipelineAggregatorName, bucketsPaths[0], params);
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }

        assert(factory != null);

        return factory;
    }

    protected abstract BucketMetricsPipelineAggregationBuilder<?> buildFactory(String pipelineAggregatorName, String bucketsPaths,
                                                                              Map<String, Object> params);

    protected boolean token(XContentParser parser, QueryParseContext context, String field,
                            XContentParser.Token token, Map<String, Object> params) throws IOException {
        return false;
    }

    private void parseToken(String aggregationName, XContentParser parser, QueryParseContext context, String currentFieldName,
                       XContentParser.Token currentToken, Map<String, Object> params) throws IOException {
        if (token(parser, context, currentFieldName, currentToken, params) == false) {
            throw new ParsingException(parser.getTokenLocation(),
                "Unexpected token " + currentToken + " [" + currentFieldName + "] in [" + aggregationName + "]");
        }
    }
}
