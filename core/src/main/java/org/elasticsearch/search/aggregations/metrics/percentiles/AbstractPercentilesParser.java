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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractPercentilesParser implements Aggregator.Parser {

    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField METHOD_FIELD = new ParseField("method");
    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");
    public static final ParseField NUMBER_SIGNIFICANT_DIGITS_FIELD = new ParseField("number_of_significant_value_digits");

    private boolean formattable;

    public AbstractPercentilesParser(boolean formattable) {
        this.formattable = formattable;
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalTDigestPercentiles.TYPE, context)
                .formattable(formattable).build();

        double[] keys = null;
        boolean keyed = true;
        Double compression = null;
        Integer numberOfSignificantValueDigits = null;
        PercentilesMethod method = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.parseFieldMatcher().match(currentFieldName, keysField())) {
                    DoubleArrayList values = new DoubleArrayList(10);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double value = parser.doubleValue();
                        values.add(value);
                    }
                    keys = values.toArray();
                    Arrays.sort(keys);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (context.parseFieldMatcher().match(currentFieldName, KEYED_FIELD)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (method != null) {
                    throw new SearchParseException(context, "Found multiple methods in [" + aggregationName + "]: [" + currentFieldName
                            + "]. only one of [" + PercentilesMethod.TDIGEST.getName() + "] and [" + PercentilesMethod.HDR.getName()
                            + "] may be used.", parser.getTokenLocation());
                }
                method = PercentilesMethod.resolveFromName(currentFieldName);
                if (method == null) {
                    throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].",
                            parser.getTokenLocation());
                } else {
                    switch (method) {
                    case TDIGEST:
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if (context.parseFieldMatcher().match(currentFieldName, COMPRESSION_FIELD)) {
                                    compression = parser.doubleValue();
                                } else {
                                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName
                                            + "]: [" + currentFieldName + "].", parser.getTokenLocation());
                                }
                            } else {
                                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                                        + currentFieldName + "].", parser.getTokenLocation());
                            }
                        }
                        break;
                    case HDR:
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if (context.parseFieldMatcher().match(currentFieldName, NUMBER_SIGNIFICANT_DIGITS_FIELD)) {
                                    numberOfSignificantValueDigits = parser.intValue();
                                } else {
                                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName
                                            + "]: [" + currentFieldName + "].", parser.getTokenLocation());
                                }
                            } else {
                                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                                        + currentFieldName + "].", parser.getTokenLocation());
                            }
                        }
                        break;
                    }
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
        }

        if (method == null) {
            method = PercentilesMethod.TDIGEST;
        }

        switch (method) {
        case TDIGEST:
            if (numberOfSignificantValueDigits != null) {
                throw new SearchParseException(context, "[number_of_significant_value_digits] cannot be used with method [tdigest] in ["
                        + aggregationName + "].", parser.getTokenLocation());
            }
            if (compression == null) {
                compression = 100.0;
            }
            break;
        case HDR:
            if (compression != null) {
                throw new SearchParseException(context, "[compression] cannot be used with method [hdr] in [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
            if (numberOfSignificantValueDigits == null) {
                numberOfSignificantValueDigits = 3;
            }
            break;

        default:
            // Shouldn't get here but if we do, throw a parse exception for
            // invalid method
            throw new SearchParseException(context, "Unknown value for [" + currentFieldName + "] in [" + aggregationName + "]: [" + method
                    + "].", parser.getTokenLocation());
        }

        return buildFactory(context, aggregationName, vsParser.config(), keys, method, compression,
                numberOfSignificantValueDigits, keyed);
    }

    protected abstract AggregatorFactory buildFactory(SearchContext context, String aggregationName, ValuesSourceConfig<Numeric> config,
            double[] cdfValues, PercentilesMethod method, Double compression, Integer numberOfSignificantValueDigits, boolean keyed);

    protected abstract ParseField keysField();

}
