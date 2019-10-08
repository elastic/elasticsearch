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
import org.elasticsearch.script.Script;

import java.time.ZoneId;
import java.time.ZoneOffset;

public final class ValuesSourceParserHelper {

    private ValuesSourceParserHelper() {} // utility class, no instantiation

    public static <T> void declareAnyFields(
        AbstractObjectParser<? extends ValuesSourceAggregationBuilder<ValuesSource, ?>, T> objectParser,
        boolean scriptable, boolean formattable) {
        declareAnyFields(objectParser, scriptable, formattable, false);
    }

    public static <T> void declareAnyFields(
        AbstractObjectParser<? extends ValuesSourceAggregationBuilder<ValuesSource, ?>, T> objectParser,
        boolean scriptable, boolean formattable, boolean timezoneAware) {
        declareFields(objectParser, scriptable, formattable, timezoneAware, null);
    }

    public static <T> void declareNumericFields(
            AbstractObjectParser<? extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, ?>, T> objectParser,
            boolean scriptable, boolean formattable, boolean timezoneAware) {
        declareFields(objectParser, scriptable, formattable, timezoneAware, ValueType.NUMERIC);
    }

    public static <T> void declareBytesFields(
            AbstractObjectParser<? extends ValuesSourceAggregationBuilder<ValuesSource.Bytes, ?>, T> objectParser,
            boolean scriptable, boolean formattable) {
        declareFields(objectParser, scriptable, formattable, false, ValueType.STRING);
    }

    public static <T> void declareGeoFields(
            AbstractObjectParser<? extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, ?>, T> objectParser,
            boolean scriptable, boolean formattable) {
        declareFields(objectParser, scriptable, formattable, false, ValueType.GEOPOINT);
    }

    private static <VS extends ValuesSource, T> void declareFields(
            AbstractObjectParser<? extends ValuesSourceAggregationBuilder<VS, ?>, T> objectParser,
            boolean scriptable, boolean formattable, boolean timezoneAware, ValueType targetValueType) {


        objectParser.declareField(ValuesSourceAggregationBuilder::field, XContentParser::text,
            ParseField.CommonFields.FIELD, ObjectParser.ValueType.STRING);

        objectParser.declareField(ValuesSourceAggregationBuilder::missing, XContentParser::objectText,
            ParseField.CommonFields.MISSING, ObjectParser.ValueType.VALUE);

        objectParser.declareField(ValuesSourceAggregationBuilder::valueType, p -> {
            ValueType valueType = ValueType.resolveForScript(p.text());
            if (targetValueType != null && valueType.isNotA(targetValueType)) {
                throw new ParsingException(p.getTokenLocation(),
                        "Aggregation [" + objectParser.getName() + "] was configured with an incompatible value type ["
                                + valueType + "]. It can only work on value of type ["
                                + targetValueType + "]");
            }
            return valueType;
        }, ValueType.VALUE_TYPE, ObjectParser.ValueType.STRING);

        if (formattable) {
            objectParser.declareField(ValuesSourceAggregationBuilder::format, XContentParser::text,
                ParseField.CommonFields.FORMAT, ObjectParser.ValueType.STRING);
        }

        if (scriptable) {
            objectParser.declareField(ValuesSourceAggregationBuilder::script,
                    (parser, context) -> Script.parse(parser),
                    Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
        }

        if (timezoneAware) {
            objectParser.declareField(ValuesSourceAggregationBuilder::timeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ZoneId.of(p.text());
                } else {
                    return ZoneOffset.ofHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }
    }



}
