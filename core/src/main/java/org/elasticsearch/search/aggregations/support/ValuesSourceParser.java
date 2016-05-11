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
import org.elasticsearch.search.aggregations.Aggregator;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class ValuesSourceParser<VS extends ValuesSource> implements Aggregator.Parser {
    static final ParseField TIME_ZONE = new ParseField("time_zone");

    protected boolean scriptable = true;
    protected boolean formattable = false;
    protected boolean timezoneAware = false;
    protected ValuesSourceType valuesSourceType = null;
    protected ValueType targetValueType = null;

    protected ValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware, ValuesSourceType valuesSourceType,
            ValueType targetValueType) {
        this.timezoneAware = timezoneAware;
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
        this.scriptable = scriptable;
        this.formattable = formattable;
    }

    @Override
    public final ValuesSourceAggregatorBuilder<VS, ?> parse(String aggregationName,  QueryParseContext context)
        throws IOException {
        XContentParser parser = context.parser();
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
                    parseField(parser);
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
                if (context.getParseFieldMatcher().match(currentFieldName, Script.ScriptField.SCRIPT)) {
                    parseScript(aggregationName, currentFieldName, parser, context.getParseFieldMatcher());
                } else if (!token(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)) {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
                }
            } else if (!internalToken(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)
                && !token(aggregationName, currentFieldName, token, parser, context.getParseFieldMatcher(), otherOptions)) {
                throw new ParsingException(parser.getTokenLocation(),
                    "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
            }
        }

        return createBuilder(aggregationName, valueType, format, missing, timezone, otherOptions);
    }

    protected abstract void parseField(XContentParser parser) throws IOException;

    protected abstract void parseScript(final String aggregationName, final String currentFieldName,
            XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException;

    /**
     * Creates a {@link ValuesSourceAggregatorBuilder} from the information
     * gathered by the subclass. Options parsed in
     * {@link ValuesSourceParser} itself will be added to the factory
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
    protected abstract ValuesSourceAggregatorBuilder<VS, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions);

    protected abstract ValuesSourceAggregatorBuilder<VS, ?> createBuilder(String aggregationName, final ValueType parsedValueType,
            final String parsedFormat, final Object parsedMissing, final DateTimeZone parsedTimeZone,
            Map<ParseField, Object> otherOptions);

    protected boolean internalToken(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        return false;
    }

    /**
     * Allows subclasses of {@link ValuesSourceParser} to parse extra
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
