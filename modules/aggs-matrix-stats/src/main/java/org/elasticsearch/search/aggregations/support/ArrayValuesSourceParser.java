/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder.CommonFields;
import org.elasticsearch.search.aggregations.Aggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ArrayValuesSourceParser<VS extends ValuesSource> implements Aggregator.Parser {

    public abstract static class NumericValuesSourceParser extends ArrayValuesSourceParser<ValuesSource.Numeric> {

        protected NumericValuesSourceParser(boolean formattable) {
            super(formattable, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        }
    }

    public abstract static class BytesValuesSourceParser extends ArrayValuesSourceParser<ValuesSource.Bytes> {

        protected BytesValuesSourceParser(boolean formattable) {
            super(formattable, CoreValuesSourceType.KEYWORD, ValueType.STRING);
        }
    }

    public abstract static class GeoPointValuesSourceParser extends ArrayValuesSourceParser<ValuesSource.GeoPoint> {

        protected GeoPointValuesSourceParser(boolean formattable) {
            super(formattable, CoreValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        }
    }

    private boolean formattable = false;
    private ValuesSourceType valuesSourceType = null;
    private ValueType targetValueType = null;

    private ArrayValuesSourceParser(boolean formattable, ValuesSourceType valuesSourceType, ValueType targetValueType) {
        this.valuesSourceType = valuesSourceType;
        this.targetValueType = targetValueType;
        this.formattable = formattable;
    }

    @Override
    public final ArrayValuesSourceAggregationBuilder<?> parse(String aggregationName, XContentParser parser) throws IOException {

        List<String> fields = null;
        String format = null;
        Map<String, Object> missingMap = null;
        Map<ParseField, Object> otherOptions = new HashMap<>();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (CommonFields.FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    fields = Collections.singletonList(parser.text());
                } else if (formattable && CommonFields.FORMAT.match(currentFieldName, parser.getDeprecationHandler())) {
                    format = parser.text();
                } else if (CommonFields.VALUE_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token "
                            + token
                            + " ["
                            + currentFieldName
                            + "] in ["
                            + aggregationName
                            + "]. "
                            + "Multi-field aggregations do not support scripts."
                    );
                } else if (token(aggregationName, currentFieldName, token, parser, otherOptions) == false) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CommonFields.MISSING.match(currentFieldName, parser.getDeprecationHandler())) {
                    missingMap = new HashMap<>();
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        parseMissingAndAdd(aggregationName, currentFieldName, parser, missingMap);
                    }
                } else if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token "
                            + token
                            + " ["
                            + currentFieldName
                            + "] in ["
                            + aggregationName
                            + "]. "
                            + "Multi-field aggregations do not support scripts."
                    );

                } else if (token(aggregationName, currentFieldName, token, parser, otherOptions) == false) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token "
                            + token
                            + " ["
                            + currentFieldName
                            + "] in ["
                            + aggregationName
                            + "]. "
                            + "Multi-field aggregations do not support scripts."
                    );
                } else if (CommonFields.FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    fields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            fields.add(parser.text());
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "]."
                            );
                        }
                    }
                } else if (token(aggregationName, currentFieldName, token, parser, otherOptions) == false) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "]."
                    );
                }
            } else if (token(aggregationName, currentFieldName, token, parser, otherOptions) == false) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "]."
                );
            }
        }

        ArrayValuesSourceAggregationBuilder<?> factory = createFactory(
            aggregationName,
            this.valuesSourceType,
            this.targetValueType,
            otherOptions
        );
        if (fields != null) {
            factory.fields(fields);
        }
        if (format != null) {
            factory.format(format);
        }
        if (missingMap != null) {
            factory.missingMap(missingMap);
        }
        return factory;
    }

    private void parseMissingAndAdd(
        final String aggregationName,
        final String currentFieldName,
        XContentParser parser,
        final Map<String, Object> missing
    ) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }

        if (token == XContentParser.Token.FIELD_NAME) {
            final String fieldName = parser.currentName();
            if (missing.containsKey(fieldName)) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Missing field [" + fieldName + "] already defined as [" + missing.get(fieldName) + "] in [" + aggregationName + "]."
                );
            }
            parser.nextToken();
            missing.put(fieldName, parser.objectText());
        } else {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "]"
            );
        }
    }

    /**
     * Creates a {@link ValuesSourceAggregationBuilder} from the information
     * gathered by the subclass. Options parsed in
     * {@link ArrayValuesSourceParser} itself will be added to the factory
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
     *            {@link #token(String, String, XContentParser.Token, XContentParser, Map)}
     *            method
     * @return the created factory
     */
    protected abstract ArrayValuesSourceAggregationBuilder<?> createFactory(
        String aggregationName,
        ValuesSourceType valuesSourceType,
        ValueType targetValueType,
        Map<ParseField, Object> otherOptions
    );

    /**
     * Allows subclasses of {@link ArrayValuesSourceParser} to parse extra
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
    protected abstract boolean token(
        String aggregationName,
        String currentFieldName,
        XContentParser.Token token,
        XContentParser parser,
        Map<ParseField, Object> otherOptions
    ) throws IOException;
}
