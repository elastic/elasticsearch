/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD;
import static org.elasticsearch.xpack.ml.aggregations.pca.PCAAggregationBuilder.USE_COVARIANCE_FIELD;

public class PCAAggregationParser extends ArrayValuesSourceParser.NumericValuesSourceParser {
    public PCAAggregationParser() {
        super(true);
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser,
                            Map<ParseField, Object> otherOptions) throws IOException {
        if (MULTIVALUE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(MULTIVALUE_MODE_FIELD, parser.text());
                return true;
            }
        } else if (USE_COVARIANCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_BOOLEAN) {
                otherOptions.put(USE_COVARIANCE_FIELD, Boolean.valueOf(parser.booleanValue()));
                return true;
            }
        }
        return false;
    }

    @Override
    protected PCAAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                  ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        PCAAggregationBuilder builder = new PCAAggregationBuilder(aggregationName);
        String mode = (String)otherOptions.get(MULTIVALUE_MODE_FIELD);
        if (mode != null) {
            builder.multiValueMode(MultiValueMode.fromString(mode));
        }
        Boolean useCovariance = (Boolean)otherOptions.get(USE_COVARIANCE_FIELD);
        if (useCovariance != null) {
            builder.setUseCovariance(useCovariance);
        }
        return builder;
    }
}
