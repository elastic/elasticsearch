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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;

import java.io.IOException;

public class PercentileRanksParser extends NumericValuesSourceParser {

    public static final ParseField VALUES_FIELD = new ParseField("values");

    private static class TDigestOptions {
        Double compression;
    }

    private static final ObjectParser<TDigestOptions, QueryParseContext> TDIGEST_OPTIONS_PARSER =
            new ObjectParser<>(PercentilesMethod.TDIGEST.getParseField().getPreferredName(), TDigestOptions::new);
    static {
        TDIGEST_OPTIONS_PARSER.declareDouble((opts, compression) -> opts.compression = compression, new ParseField("compression"));
    }

    private static class HDROptions {
        Integer numberOfSigDigits;
    }

    private static final ObjectParser<HDROptions, QueryParseContext> HDR_OPTIONS_PARSER =
            new ObjectParser<>(PercentilesMethod.HDR.getParseField().getPreferredName(), HDROptions::new);
    static {
        HDR_OPTIONS_PARSER.declareInt((opts, numberOfSigDigits) -> opts.numberOfSigDigits = numberOfSigDigits,
                new ParseField("number_of_significant_value_digits"));
    }

    private final ObjectParser<PercentileRanksAggregationBuilder, QueryParseContext> parser;

    public PercentileRanksParser() {
        parser = new ObjectParser<>(PercentileRanksAggregationBuilder.NAME);
        addFields(parser, true, false, false);

        parser.declareDoubleArray(
                (b, v) -> b.values(v.stream().mapToDouble(Double::doubleValue).toArray()),
                VALUES_FIELD);

        parser.declareBoolean(PercentileRanksAggregationBuilder::keyed, PercentilesParser.KEYED_FIELD);

        parser.declareField((b, v) -> {
            b.method(PercentilesMethod.TDIGEST);
            if (v.compression != null) {
                b.compression(v.compression);
            }
        }, TDIGEST_OPTIONS_PARSER::parse, PercentilesMethod.TDIGEST.getParseField(), ObjectParser.ValueType.OBJECT);

        parser.declareField((b, v) -> {
            b.method(PercentilesMethod.HDR);
            if (v.numberOfSigDigits != null) {
                b.numberOfSignificantValueDigits(v.numberOfSigDigits);
            }
        }, HDR_OPTIONS_PARSER::parse, PercentilesMethod.HDR.getParseField(), ObjectParser.ValueType.OBJECT);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new PercentileRanksAggregationBuilder(aggregationName), context);
    }
}
