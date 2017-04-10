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

package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class ParsedPercentiles extends ParsedAggregation implements Percentiles {

    private final Map<Double, Double> percentiles = new HashMap<>();
    private final Map<Double, String> percentilesAsString = new HashMap<>();

    protected boolean keyed;

    void addPercentile(double key, double value) {
        percentiles.put(key, value);
    }

    void addPercentileAsString(double key, String valueAsString) {
        percentilesAsString.put(key, valueAsString);
    }

    @Override
    public double percentile(double percent) {
        Double percentile = percentiles.get(percent);
        if (percentile == null) {
            return Double.NaN;
        }
        return percentile;
    }

    @Override
    public String percentileAsString(double percent) {
        return percentilesAsString.get(percent);
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iterator<Percentile>() {
            final Iterator<Map.Entry<Double, Double>> iterator = percentiles.entrySet().iterator();
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Percentile next() {
                Map.Entry<Double, Double> next = iterator.next();
                return new InternalPercentile(next.getKey(), next.getValue());
            }
        };
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(InternalAggregation.CommonFields.VALUES.getPreferredName());
            for (Map.Entry<Double, Double> percentile : percentiles.entrySet()) {
                Double key = percentile.getKey();
                builder.field(String.valueOf(key), percentile.getValue());

                String valueAsString = percentilesAsString.get(key);
                if (valueAsString != null && valueAsString.isEmpty() == false) {
                    builder.field(key + "_as_string", valueAsString);
                }
            }
            builder.endObject();
        } else {
            builder.startArray(InternalAggregation.CommonFields.VALUES.getPreferredName());
            for (Map.Entry<Double, Double> percentile : percentiles.entrySet()) {
                Double key = percentile.getKey();
                builder.startObject();
                {
                    builder.field(InternalAggregation.CommonFields.KEY.getPreferredName(), key);
                    builder.field(InternalAggregation.CommonFields.VALUE.getPreferredName(), percentile.getValue());
                    String valueAsString = percentilesAsString.get(key);
                    if (valueAsString != null && valueAsString.isEmpty() == false) {
                        builder.field(InternalAggregation.CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
                    }
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    protected static void declarePercentilesFields(ObjectParser<? extends ParsedPercentiles, Void> objectParser) {
        ParsedAggregation.declareCommonFields(objectParser);

        objectParser.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.keyed = true;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token.isValue()) {
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            aggregation.addPercentile(Double.valueOf(parser.currentName()), parser.doubleValue());

                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            int i = parser.currentName().indexOf("_as_string");
                            if (i > 0) {
                                double key = Double.valueOf(parser.currentName().substring(0, i));
                                aggregation.addPercentileAsString(key, parser.text());
                            } else {
                                aggregation.addPercentile(Double.valueOf(parser.currentName()), Double.valueOf(parser.text()));
                            }
                        }
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.keyed = false;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Double key = null;
                    double value = 0.0d;
                    String valueAsString = null;

                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (InternalAggregation.CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                                key = parser.doubleValue();
                            } else if (InternalAggregation.CommonFields.VALUE.getPreferredName().equals(currentFieldName)) {
                                value = parser.doubleValue();
                            } else if (InternalAggregation.CommonFields.VALUE_AS_STRING.getPreferredName().equals(currentFieldName)) {
                                valueAsString = parser.text();
                            }
                        }
                    }
                    if (key != null) {
                        aggregation.addPercentile(key, value);
                        if (valueAsString != null) {
                            aggregation.addPercentileAsString(key, valueAsString);
                        }
                    }
                }
            }
        }, InternalAggregation.CommonFields.VALUES, ObjectParser.ValueType.OBJECT_ARRAY);
    }
}
