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

package org.elasticsearch.client.analytics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Results of the {@code top_metrics} aggregation.
 */
public class ParsedTopMetrics extends ParsedAggregation {
    private static final ParseField TOP_FIELD = new ParseField("top");

    private final List<TopMetrics> topMetrics;

    private ParsedTopMetrics(String name, List<TopMetrics> topMetrics) {
        setName(name);
        this.topMetrics = topMetrics;
    }

    /**
     * The list of top metrics, in sorted order.
     */
    public List<TopMetrics> getTopMetrics() {
        return topMetrics;
    }

    @Override
    public String getType() {
        return TopMetricsAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TOP_FIELD.getPreferredName());
        for (TopMetrics top : topMetrics) {
            top.toXContent(builder, params);
        }
        return builder.endArray();
    }

    public static final ConstructingObjectParser<ParsedTopMetrics, String> PARSER = new ConstructingObjectParser<>(
            TopMetricsAggregationBuilder.NAME, true, (args, name) -> {
                @SuppressWarnings("unchecked")
                List<TopMetrics> topMetrics = (List<TopMetrics>) args[0];
                return new ParsedTopMetrics(name, topMetrics);
            });
    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> TopMetrics.PARSER.parse(p, null), TOP_FIELD);
        ParsedAggregation.declareAggregationFields(PARSER);
    }

    /**
     * The metrics belonging to the document with the "top" sort key.
     */
    public static class TopMetrics implements ToXContent {
        private static final ParseField SORT_FIELD = new ParseField("sort");
        private static final ParseField METRICS_FIELD = new ParseField("metrics");

        private final List<Object> sort;
        private final Map<String, Object> metrics;

        private TopMetrics(List<Object> sort, Map<String, Object> metrics) {
            this.sort = sort;
            this.metrics = metrics;
        }

        /**
         * The sort key for these top metrics.
         */
        public List<Object> getSort() {
            return sort;
        }

        /**
         * The top metric values returned by the aggregation.
         */
        public Map<String, Object> getMetrics() {
            return metrics;
        }

        private static final ConstructingObjectParser<TopMetrics, Void> PARSER = new ConstructingObjectParser<>("top", true,
                (args, name) -> {
                    @SuppressWarnings("unchecked")
                    List<Object> sort = (List<Object>) args[0];
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metrics = (Map<String, Object>) args[1];
                    return new TopMetrics(sort, metrics);
                });
        static {
            PARSER.declareFieldArray(constructorArg(), (p, c) -> XContentParserUtils.parseFieldsValue(p),
                    SORT_FIELD, ObjectParser.ValueType.VALUE_ARRAY);
            PARSER.declareObject(constructorArg(), (p, c) -> p.map(), METRICS_FIELD);
        }

        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(SORT_FIELD.getPreferredName(), sort);
            builder.field(METRICS_FIELD.getPreferredName(), metrics);
            builder.endObject();
            return builder;
        };
    }
}
