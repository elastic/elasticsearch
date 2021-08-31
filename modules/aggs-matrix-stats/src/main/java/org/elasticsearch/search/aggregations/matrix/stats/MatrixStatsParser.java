/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceParser.NumericValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD;

public class MatrixStatsParser extends NumericValuesSourceParser {

    public MatrixStatsParser() {
        super(true);
    }

    @Override
    protected boolean token(
        String aggregationName,
        String currentFieldName,
        XContentParser.Token token,
        XContentParser parser,
        Map<ParseField, Object> otherOptions
    ) throws IOException {
        if (MULTIVALUE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(MULTIVALUE_MODE_FIELD, parser.text());
                return true;
            }
        }
        return false;
    }

    @Override
    protected MatrixStatsAggregationBuilder createFactory(
        String aggregationName,
        ValuesSourceType valuesSourceType,
        ValueType targetValueType,
        Map<ParseField, Object> otherOptions
    ) {
        MatrixStatsAggregationBuilder builder = new MatrixStatsAggregationBuilder(aggregationName);
        String mode = (String) otherOptions.get(MULTIVALUE_MODE_FIELD);
        if (mode != null) {
            builder.multiValueMode(MultiValueMode.fromString(mode));
        }
        return builder;
    }
}
