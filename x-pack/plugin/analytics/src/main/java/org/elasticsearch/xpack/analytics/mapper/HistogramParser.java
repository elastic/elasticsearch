/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class HistogramParser {

    private static final ParseField COUNTS_FIELD = new ParseField("counts");
    private static final ParseField VALUES_FIELD = new ParseField("values");

    private static final Set<String> ROOT_FIELD_NAMES = Set.of(COUNTS_FIELD.getPreferredName(), VALUES_FIELD.getPreferredName());

    public static boolean isHistogramSubFieldName(String subFieldName) {
        return ROOT_FIELD_NAMES.contains(subFieldName);
    }

    /**
     * A parsed histogram field, can represent either a T-Digest or a HDR histogram.
     * @param values the centroids, guaranteed to be distinct and in increasing order
     * @param counts the counts, guaranteed to be non-negative and of the same length as values
     */
    public record ParsedHistogram(List<Double> values, List<Long> counts) {}

    /**
     * Parses an XContent object into a histogram.
     * The parser is expected to point at the next token after {@link XContentParser.Token#START_OBJECT}.
     *
     * @param mappedFieldName the name of the field being parsed, used for error messages
     * @param parser the parser to use
     * @return the parsed histogram
     */
    public static ParsedHistogram parse(String mappedFieldName, XContentParser parser) throws IOException {
        ArrayList<Double> values = null;
        ArrayList<Long> counts = null;
        XContentParser.Token token = parser.currentToken();
        while (token != XContentParser.Token.END_OBJECT) {
            // should be a field
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String fieldName = parser.currentName();
            if (fieldName.equals(VALUES_FIELD.getPreferredName())) {
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                values = new ArrayList<>();
                token = parser.nextToken();
                double previousVal = -Double.MAX_VALUE;
                while (token != XContentParser.Token.END_ARRAY) {
                    // should be a number
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    double val = parser.doubleValue();
                    if (val < previousVal) {
                        // values must be in increasing order
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field ["
                                + mappedFieldName
                                + "], ["
                                + VALUES_FIELD
                                + "] values must be in increasing order, got ["
                                + val
                                + "] but previous value was ["
                                + previousVal
                                + "]"
                        );
                    }
                    values.add(val);
                    previousVal = val;
                    token = parser.nextToken();
                }
            } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                counts = new ArrayList<>();
                token = parser.nextToken();
                while (token != XContentParser.Token.END_ARRAY) {
                    // should be a number
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    long count = parser.longValue();
                    if (count < 0) {
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field [" + mappedFieldName + "], [" + COUNTS_FIELD + "] elements must be >= 0 but got " + count
                        );
                    }
                    counts.add(count);
                    token = parser.nextToken();
                }
            } else {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing field [" + mappedFieldName + "], with unknown parameter [" + fieldName + "]"
                );
            }
            token = parser.nextToken();
        }
        if (values == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + VALUES_FIELD.getPreferredName() + "]"
            );
        }
        if (counts == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + COUNTS_FIELD.getPreferredName() + "]"
            );
        }
        if (values.size() != counts.size()) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field ["
                    + mappedFieldName
                    + "], expected same length from ["
                    + VALUES_FIELD.getPreferredName()
                    + "] and "
                    + "["
                    + COUNTS_FIELD.getPreferredName()
                    + "] but got ["
                    + values.size()
                    + " != "
                    + counts.size()
                    + "]"
            );
        }
        return new ParsedHistogram(values, counts);
    }

}
