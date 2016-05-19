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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.search.aggregations.Aggregator;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class AbstractValuesSourceParser<VS extends ValuesSource>
        implements Aggregator.Parser {
    static final ParseField TIME_ZONE = new ParseField("time_zone");

    public abstract static class AnyValuesSourceParser extends AbstractValuesSourceParser<ValuesSource> {

        protected AnyValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.ANY, null);
        }
    }

    public abstract static class NumericValuesSourceParser extends AbstractValuesSourceParser<ValuesSource.Numeric> {

        protected NumericValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware) {
            super(scriptable, formattable, timezoneAware, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
        }
    }

    public abstract static class BytesValuesSourceParser extends AbstractValuesSourceParser<ValuesSource.Bytes> {

        protected BytesValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.BYTES, ValueType.STRING);
        }
    }

    public abstract static class GeoPointValuesSourceParser extends AbstractValuesSourceParser<ValuesSource.GeoPoint> {

        protected GeoPointValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        }
    }

    private boolean scriptable = true;
    private boolean formattable = false;
    private boolean timezoneAware = false;
    private ValuesSourceType valuesSourceType = null;
    private ValueType targetValueType = null;

    private AbstractValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware, ValuesSourceType valuesSourceType,
            ValueType targetValueType) {
        this.timezoneAware = timezoneAware;
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
        this.scriptable = scriptable;
        this.formattable = formattable;
    }

    @Override
    public final ValuesSourceAggregationBuilder<VS, ?> parse(String aggregationName, QueryParseContext context)
            throws IOException {

        XContentParser parser = context.parser();
        String field = null;
        Script script = null;
        ValueType valueType = null;
        String format = null;
        Object missing = null;
        DateTimeZone timezone = null;
        Map<ParseField, Object> otherOptions = new HashMap<>();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("missing".equals(currentFieldName) && token.isValue()) {
                missing = parser.objectText();
            } else if (timezoneAware && context.getParseFieldMatcher().match(currentFieldName, TIME_ZONE)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    timezone = DateTimeZone.forID(parser.text());
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    timezone = DateTimeZone.forOffsetHours(parser.intValue());
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if (formattable && "format".equals(currentFieldName)) {
                    format = parser.text();
                } else if (scriptable) {
                    if ("value_type".equals(currentFieldName) || "valueType".equals(currentFieldName)) {
                        valueType = ValueType.resolveForScript(parser.text());
                        if (targetValueType != null && valueType.isNotA(targetValueType)) {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "Aggregation [" + aggregationName + "] was configured with an incompatible value type ["
                                            + valueType + "]. It can only work on value of type ["
                                            + targetValueType + "]");
                        }
                    } else if (!token(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
                    }
                } else if (!token(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)) {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
                }
            } else if (scriptable && token == XContentParser.Token.START_OBJECT) {
                if (context.getParseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, context.getParseFieldMatcher());
                } else if (!token(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)) {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
                }
            } else if (!token(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)) {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
            }
        }

        ValuesSourceAggregationBuilder<VS, ?> factory = createFactory(aggregationName, this.valuesSourceType, this.targetValueType,
                otherOptions);
        if (field != null) {
            factory.field(field);
        }
        if (script != null) {
            factory.script(script);
        }
        if (valueType != null) {
            factory.valueType(valueType);
        }
        if (format != null) {
            factory.format(format);
        }
        if (missing != null) {
            factory.missing(missing);
        }
        if (timezone != null) {
            factory.timeZone(timezone);
        }
        return factory;
    }

    /**
     * Creates a {@link ValuesSourceAggregationBuilder} from the information
     * gathered by the subclass. Options parsed in
     * {@link AbstractValuesSourceParser} itself will be added to the factory
     * after it has been returned by this method.
     *
     * @param aggregationName
     *            the name of the aggregation
     * @param valuesSourceType
     *            the type of the {@link ValuesSource}
     * @param targetValueType
     *            the target type of the final value output by the aggregation
     * @param otherOptions
     *            a {@link Map} containing the extra options parsed by the
     *            {@link #token(String, String, org.elasticsearch.common.xcontent.XContentParser.Token,
     *             XContentParser, ParseFieldMatcher, Map)}
     *            method
     * @return the created factory
     */
    protected abstract ValuesSourceAggregationBuilder<VS, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                                           ValueType targetValueType, Map<ParseField, Object> otherOptions);

    /**
     * Allows subclasses of {@link AbstractValuesSourceParser} to parse extra
     * parameters and store them in a {@link Map} which will later be passed to
     * {@link #createFactory(String, ValuesSourceType, ValueType, Map)}.
     *
     * @param aggregationName
     *            the name of the aggregation
     * @param currentFieldName
     *            the name of the current field being parsed
     * @param token
     *            the current token for the parser
     * @param parser
     *            the parser
     * @param parseFieldMatcher
     *            the {@link ParseFieldMatcher} to use to match field names
     * @param otherOptions
     *            a {@link Map} of options to be populated by successive calls
     *            to this method which will then be passed to the
     *            {@link #createFactory(String, ValuesSourceType, ValueType, Map)}
     *            method
     * @return <code>true</code> if the current token was correctly parsed,
     *         <code>false</code> otherwise
     * @throws IOException
     *             if an error occurs whilst parsing
     */
    protected abstract boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException;
}
