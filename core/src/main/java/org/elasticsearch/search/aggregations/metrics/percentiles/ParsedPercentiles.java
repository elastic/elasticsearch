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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ParsedPercentiles extends ParsedAggregation implements Percentiles {

    private final String type;

    // Map of <Key, Tuple<Value, Value As String>>
    protected final Map<Double, Tuple<Double, String>> percentiles = new HashMap<>();
    protected boolean keyed;

    protected ParsedPercentiles(String type) {
        this.type = type;
    }

    private Tuple<Double, String> getPercentile(double percent) {
        return percentiles.get(percent);
    }

    @Override
    public double percentile(double percent) {
        Tuple<Double, String> percentile = getPercentile(percent);
        return percentile != null ? percentile.v1() : 0;
    }

    @Override
    public String percentileAsString(double percent) {
        Tuple<Double, String> percentile = getPercentile(percent);
        return percentile != null ? percentile.v2() : null;
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iterator<Percentile>() {
            final Iterator<Map.Entry<Double, Tuple<Double, String>>> iterator = percentiles.entrySet().iterator();
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Percentile next() {
                Map.Entry<Double, Tuple<Double, String>> next = iterator.next();
                return new InternalPercentile(next.getKey(), next.getValue().v1());
            }
        };
    }

    @Override
    protected String getType() {
        return type;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(InternalAggregation.CommonFields.VALUES.getPreferredName());
            for (Map.Entry<Double, Tuple<Double, String>> percentile : percentiles.entrySet()) {
                builder.field(String.valueOf(percentile.getKey()), percentile.getValue().v1());

                String valueAsString = percentile.getValue().v2();
                if (valueAsString != null && valueAsString.isEmpty() == false) {
                    builder.field(percentile.getKey() + "_as_string", percentile.getValue().v2());
                }
            }
            builder.endObject();
        } else {
            builder.startArray(InternalAggregation.CommonFields.VALUES.getPreferredName());
            for (Map.Entry<Double, Tuple<Double, String>> percentile : percentiles.entrySet()) {
                builder.startObject();
                {
                    builder.field(InternalAggregation.CommonFields.KEY.getPreferredName(), percentile.getKey());
                    builder.field(InternalAggregation.CommonFields.VALUE.getPreferredName(), percentile.getValue().v1());
                    String valueAsString = percentile.getValue().v2();
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
        objectParser.declareField((parser, percentiles, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                percentiles.keyed = true;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token.isValue()) {
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            double key = Double.valueOf(parser.currentName());
                            double value = parser.doubleValue();
                            percentiles.percentiles.compute(key, (k, v) -> {
                                if (v == null) {
                                    return Tuple.tuple(value, null);
                                } else {
                                    return Tuple.tuple(value, v.v2());
                                }
                            });
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            int i = parser.currentName().indexOf("_as_string");
                            if (i > 0) {
                                double key = Double.valueOf(parser.currentName().substring(0, i));
                                String valueAsString = parser.text();
                                percentiles.percentiles.compute(key, (k, v) -> {
                                    if (v == null) {
                                        return Tuple.tuple(null, valueAsString);
                                    } else {
                                        return Tuple.tuple(v.v1(), valueAsString);
                                    }
                                });
                            } else {
                                double key = Double.valueOf(parser.currentName());
                                double value = Double.valueOf(parser.text());
                                percentiles.percentiles.compute(key, (k, v) -> {
                                    if (v == null) {
                                        return Tuple.tuple(value, null);
                                    } else {
                                        return Tuple.tuple(value, v.v2());
                                    }
                                });
                            }
                        }
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                percentiles.keyed = false;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Double key = null;
                    String valueAsString = null;
                    Double value = null;

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
                    percentiles.percentiles.put(key, Tuple.tuple(value, valueAsString));
                }
            }
        }, InternalAggregation.CommonFields.VALUES, ObjectParser.ValueType.OBJECT_ARRAY);
    }
}
