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

import com.carrotsearch.hppc.DoubleArrayList;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractPercentilesParser extends NumericValuesSourceParser {

    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField METHOD_FIELD = new ParseField("method");
    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");
    public static final ParseField NUMBER_SIGNIFICANT_DIGITS_FIELD = new ParseField("number_of_significant_value_digits");

    public AbstractPercentilesParser(boolean formattable) {
        super(true, formattable, false);
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            if (parseFieldMatcher.match(currentFieldName, keysField())) {
                DoubleArrayList values = new DoubleArrayList(10);
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    double value = parser.doubleValue();
                    values.add(value);
                }
                double[] keys = values.toArray();
                otherOptions.put(keysField(), keys);
                return true;
            } else {
                return false;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parseFieldMatcher.match(currentFieldName, KEYED_FIELD)) {
                boolean keyed = parser.booleanValue();
                otherOptions.put(KEYED_FIELD, keyed);
                return true;
            } else {
                return false;
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            PercentilesMethod method = PercentilesMethod.resolveFromName(currentFieldName);
            if (method == null) {
                return false;
            } else {
                otherOptions.put(METHOD_FIELD, method);
                switch (method) {
                case TDIGEST:
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if (parseFieldMatcher.match(currentFieldName, COMPRESSION_FIELD)) {
                                double compression = parser.doubleValue();
                                otherOptions.put(COMPRESSION_FIELD, compression);
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    break;
                case HDR:
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if (parseFieldMatcher.match(currentFieldName, NUMBER_SIGNIFICANT_DIGITS_FIELD)) {
                                int numberOfSignificantValueDigits = parser.intValue();
                                otherOptions.put(NUMBER_SIGNIFICANT_DIGITS_FIELD, numberOfSignificantValueDigits);
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    break;
                }
                return true;
            }
        }
        return false;
    }

    @Override
    protected ValuesSourceAggregationBuilder<Numeric, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                                       ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        PercentilesMethod method = (PercentilesMethod) otherOptions.getOrDefault(METHOD_FIELD, PercentilesMethod.TDIGEST);

        double[] cdfValues = (double[]) otherOptions.get(keysField());
        Double compression = (Double) otherOptions.get(COMPRESSION_FIELD);
        Integer numberOfSignificantValueDigits = (Integer) otherOptions.get(NUMBER_SIGNIFICANT_DIGITS_FIELD);
        Boolean keyed = (Boolean) otherOptions.get(KEYED_FIELD);
        return buildFactory(aggregationName, cdfValues, method, compression, numberOfSignificantValueDigits, keyed);
    }

    protected abstract ValuesSourceAggregationBuilder<Numeric, ?> buildFactory(String aggregationName, double[] cdfValues,
                                                                               PercentilesMethod method,
                                                                               Double compression,
                                                                               Integer numberOfSignificantValueDigits, Boolean keyed);

    protected abstract ParseField keysField();

}
