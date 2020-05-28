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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
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

    public static <VS extends ValuesSource, T> void declareField(String fieldName,
        AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<?>, T> objectParser,
        boolean scriptable, boolean timezoneAware, boolean filterable) {

        objectParser.declareField((o, fieldConfig) -> o.field(fieldName, fieldConfig.build()),
            (p, c) -> MultiValuesSourceFieldConfig.parserBuilder(scriptable, timezoneAware, filterable).parse(p, null),
            new ParseField(fieldName), ObjectParser.ValueType.OBJECT);
    }
}
