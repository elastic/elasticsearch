/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public final class MultiValuesSourceParseHelper {

    public static <T> void declareCommon(
            AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<?>, T> objectParser, boolean formattable,
            ValueType expectedValueType) {

        objectParser.declareField(MultiValuesSourceAggregationBuilder::userValueTypeHint, p -> {
            ValueType valueType = ValueType.lenientParse(p.text());
            if (expectedValueType != null && valueType.isNotA(expectedValueType)) {
                throw new ParsingException(p.getTokenLocation(),
                    "Aggregation [" + objectParser.getName() + "] was configured with an incompatible value type ["
                        + valueType + "].  It can only work on value off type ["
                        + expectedValueType + "]");
            }
            return valueType;
        }, ValueType.VALUE_TYPE, ObjectParser.ValueType.STRING);

        if (formattable) {
            objectParser.declareField(MultiValuesSourceAggregationBuilder::format, XContentParser::text,
                ParseField.CommonFields.FORMAT, ObjectParser.ValueType.STRING);
        }
    }

    /**
     * Declares a field that contains information about a values source
     *
     * @param scriptable - allows specifying script in addition to a field as a values source
     * @param timezoneAware - allows specifying timezone
     * @param filterable - allows specifying filters on the values
     * @param heterogeneous - allows specifying value-source specific format and user value type hint
     * @param <VS> - values source type
     * @param <T> - parser context
     */
    public static <VS extends ValuesSource, T> void declareField(
        String fieldName,
        AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<?>, T> objectParser,
        boolean scriptable, boolean timezoneAware, boolean filterable, boolean heterogeneous) {

        objectParser.declareField((o, fieldConfig) -> o.field(fieldName, fieldConfig.build()),
            (p, c) -> MultiValuesSourceFieldConfig.parserBuilder(scriptable, timezoneAware, filterable, heterogeneous).parse(p, null),
            new ParseField(fieldName), ObjectParser.ValueType.OBJECT);
    }
}
