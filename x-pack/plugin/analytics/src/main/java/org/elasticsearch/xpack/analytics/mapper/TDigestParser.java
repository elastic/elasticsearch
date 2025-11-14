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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.analytics.mapper.TDigestFieldMapper.CENTROIDS_NAME;
import static org.elasticsearch.xpack.analytics.mapper.TDigestFieldMapper.COUNTS_NAME;

public class TDigestParser {

    private static final ParseField COUNTS_FIELD = new ParseField(COUNTS_NAME);
    private static final ParseField CENTROIDS_FIELD = new ParseField(CENTROIDS_NAME);

    /**
     * A parsed histogram field, can represent either a T-Digest
     * @param centroids the centroids, guaranteed to be distinct and in increasing order
     * @param counts the counts, guaranteed to be non-negative and of the same length as the centroids array
     */
    public record ParsedHistogram(List<Double> centroids, List<Long> counts) {}

    /**
     * Parses an XContent object into a histogram.
     * The parser is expected to point at the next token after {@link XContentParser.Token#START_OBJECT}.
     *
     * @param mappedFieldName the name of the field being parsed, used for error messages
     * @param parser the parser to use
     * @return the parsed histogram
     */
    public static ParsedHistogram parse(String mappedFieldName, XContentParser parser) throws IOException {
        ArrayList<Double> centroids = null;
        ArrayList<Long> counts = null;
        XContentParser.Token token = parser.currentToken();
        while (token != XContentParser.Token.END_OBJECT) {
            // should be a field
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String fieldName = parser.currentName();
            if (fieldName.equals(CENTROIDS_FIELD.getPreferredName())) {
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                centroids = new ArrayList<>();
                token = parser.nextToken();
                double previousVal = -Double.MAX_VALUE;
                while (token != XContentParser.Token.END_ARRAY) {
                    // should be a number
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    double val = parser.doubleValue();
                    if (val < previousVal) {
                        // centroids must be in increasing order
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field ["
                                + mappedFieldName
                                + "], ["
                                + CENTROIDS_FIELD
                                + "] centroids must be in increasing order, got ["
                                + val
                                + "] but previous value was ["
                                + previousVal
                                + "]"
                        );
                    }
                    centroids.add(val);
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
        if (centroids == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + CENTROIDS_FIELD.getPreferredName() + "]"
            );
        }
        if (counts == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + COUNTS_FIELD.getPreferredName() + "]"
            );
        }
        if (centroids.size() != counts.size()) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field ["
                    + mappedFieldName
                    + "], expected same length from ["
                    + CENTROIDS_FIELD.getPreferredName()
                    + "] and "
                    + "["
                    + COUNTS_FIELD.getPreferredName()
                    + "] but got ["
                    + centroids.size()
                    + " != "
                    + counts.size()
                    + "]"
            );
        }
        return new ParsedHistogram(centroids, counts);
    }

}
