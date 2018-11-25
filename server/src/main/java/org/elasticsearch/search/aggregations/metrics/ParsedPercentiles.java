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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class ParsedPercentiles extends ParsedAggregation implements Iterable<Percentile>  {

    protected final Map<Double, Double> percentiles = new LinkedHashMap<>();
    protected final Map<Double, String> percentilesAsString = new HashMap<>();

    private boolean keyed;

    void addPercentile(Double key, Double value) {
        percentiles.put(key, value);
    }

    void addPercentileAsString(Double key, String valueAsString) {
        percentilesAsString.put(key, valueAsString);
    }

    protected Double getPercentile(double percent) {
        if (percentiles.isEmpty()) {
            return Double.NaN;
        }
        return percentiles.get(percent);
    }

    protected String getPercentileAsString(double percent) {
        String valueAsString = percentilesAsString.get(percent);
        if (valueAsString != null) {
            return valueAsString;
        }
        Double value = getPercentile(percent);
        if (value != null) {
            return Double.toString(value);
        }
        return null;
    }

    void setKeyed(boolean keyed) {
        this.keyed = keyed;
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
                return new Percentile(next.getKey(), next.getValue());
            }
        };
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean valuesAsString = (percentilesAsString.isEmpty() == false);
        if (keyed) {
            builder.startObject(CommonFields.VALUES.getPreferredName());
            for (Map.Entry<Double, Double> percentile : percentiles.entrySet()) {
                Double key = percentile.getKey();
                Double value = percentile.getValue();
                builder.field(String.valueOf(key), value.isNaN() ? null : value);
                if (valuesAsString && value.isNaN() == false) {
                    builder.field(key + "_as_string", getPercentileAsString(key));
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES.getPreferredName());
            for (Map.Entry<Double, Double> percentile : percentiles.entrySet()) {
                Double key = percentile.getKey();
                builder.startObject();
                {
                    builder.field(CommonFields.KEY.getPreferredName(), key);
                    Double value = percentile.getValue();
                    builder.field(CommonFields.VALUE.getPreferredName(), value.isNaN() ? null : value);
                    if (valuesAsString && value.isNaN() == false) {
                        builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), getPercentileAsString(key));
                    }
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    protected static void declarePercentilesFields(ObjectParser<? extends ParsedPercentiles, Void> objectParser) {
        ParsedAggregation.declareAggregationFields(objectParser);

        objectParser.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.setKeyed(true);
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
                    } else if (token == XContentParser.Token.VALUE_NULL) {
                        aggregation.addPercentile(Double.valueOf(parser.currentName()), Double.NaN);
                    } else {
                        parser.skipChildren(); // skip potential inner objects and arrays for forward compatibility
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.setKeyed(false);

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Double key = null;
                    Double value = null;
                    String valueAsString = null;

                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                                key = parser.doubleValue();
                            } else if (CommonFields.VALUE.getPreferredName().equals(currentFieldName)) {
                                value = parser.doubleValue();
                            } else if (CommonFields.VALUE_AS_STRING.getPreferredName().equals(currentFieldName)) {
                                valueAsString = parser.text();
                            }
                        } else if (token == XContentParser.Token.VALUE_NULL) {
                            value = Double.NaN;
                        } else {
                            parser.skipChildren(); // skip potential inner objects and arrays for forward compatibility
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
        }, CommonFields.VALUES, ObjectParser.ValueType.OBJECT_ARRAY);
    }
}
