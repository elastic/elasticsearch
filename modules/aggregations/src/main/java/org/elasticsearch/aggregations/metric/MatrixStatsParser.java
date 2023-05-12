/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.aggregations.metric;

import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class MatrixStatsParser extends ArrayValuesSourceParser.NumericValuesSourceParser {

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
        if (ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD, parser.text());
                return true;
            }
        }
        return false;
    }

    @Override
    protected MatrixStatsAggregationBuilder createFactory(
        String aggregationName,
        ValuesSourceType valuesSourceType,
        Map<ParseField, Object> otherOptions
    ) {
        MatrixStatsAggregationBuilder builder = new MatrixStatsAggregationBuilder(aggregationName);
        String mode = (String) otherOptions.get(ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD);
        if (mode != null) {
            builder.multiValueMode(MultiValueMode.fromString(mode));
        }
        return builder;
    }
}
