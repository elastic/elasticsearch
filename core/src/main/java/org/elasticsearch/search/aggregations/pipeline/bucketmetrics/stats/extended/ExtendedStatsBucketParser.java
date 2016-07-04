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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;

import java.io.IOException;
import java.util.Map;

public class ExtendedStatsBucketParser extends BucketMetricsParser {
    static final ParseField SIGMA = new ParseField("sigma");

    @Override
    protected ExtendedStatsBucketPipelineAggregationBuilder buildFactory(String pipelineAggregatorName,
            String bucketsPath, Map<String, Object> params) {
        ExtendedStatsBucketPipelineAggregationBuilder factory =
            new ExtendedStatsBucketPipelineAggregationBuilder(pipelineAggregatorName, bucketsPath);
        Double sigma = (Double) params.get(SIGMA.getPreferredName());
        if (sigma != null) {
            factory.sigma(sigma);
        }

        return factory;
    }

    @Override
    protected boolean token(XContentParser parser, QueryParseContext context, String field,
                            XContentParser.Token token, Map<String, Object> params) throws IOException {
        if (context.getParseFieldMatcher().match(field, SIGMA) && token == XContentParser.Token.VALUE_NUMBER) {
            params.put(SIGMA.getPreferredName(), parser.doubleValue());
            return true;
        }
        return false;
    }
}
