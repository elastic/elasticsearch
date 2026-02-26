/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class TDigestParser {
    public static final String CENTROIDS_NAME = "centroids";
    public static final String COUNTS_NAME = "counts";
    public static final String SUM_FIELD_NAME = "sum";
    public static final String MIN_FIELD_NAME = "min";
    public static final String MAX_FIELD_NAME = "max";

    private static final ParseField COUNTS_FIELD = new ParseField(COUNTS_NAME);
    private static final ParseField CENTROIDS_FIELD = new ParseField(CENTROIDS_NAME);
    private static final ParseField SUM_FIELD = new ParseField(SUM_FIELD_NAME);
    private static final ParseField MAX_FIELD = new ParseField(MAX_FIELD_NAME);
    private static final ParseField MIN_FIELD = new ParseField(MIN_FIELD_NAME);

    /**
     * A parsed t-digest field
     * @param centroids the centroids, guaranteed to be distinct and in increasing order
     * @param counts the counts, guaranteed to be non-negative and of the same length as the centroids array
     */
    public record ParsedTDigest(List<Double> centroids, List<Long> counts, Double sum, Double min, Double max) {
        @Override
        public Double max() {
            if (max != null) {
                return max;
            }
            if (centroids != null && centroids.isEmpty() == false) {
                return centroids.get(centroids.size() - 1);
            }
            return Double.NaN;
        }

        @Override
        public Double min() {
            if (min != null) {
                return min;
            }
            if (centroids != null && centroids.isEmpty() == false) {
                return centroids.get(0);
            }
            return Double.NaN;
        }

        @Override
        public Double sum() {
            if (sum != null) {
                return sum;
            }
            if (centroids != null && centroids.isEmpty() == false) {
                double observedSum = 0;
                for (int i = 0; i < centroids.size(); i++) {
                    observedSum += centroids.get(i) * counts.get(i);
                }
                return observedSum;
            }
            return Double.NaN;
        }

        public Long count() {
            if (counts != null && counts.isEmpty() == false) {
                long observedCount = 0;
                for (Long count : counts) {
                    observedCount += count;
                }
                return observedCount;
            }
            return 0L;
        }
    }

    /**
     * Parses an XContent object into a histogram.
     * The parser is expected to point at the next token after {@link XContentParser.Token#START_OBJECT}.
     *
     * @param mappedFieldName the name of the field being parsed, used for error messages
     * @param parser the parser to use
     * @param documentParsingExceptionProvider factory function for generating document parsing exceptions. Required for visibility.
     * @return the parsed histogram
     */
    public static ParsedTDigest parse(
        String mappedFieldName,
        XContentParser parser,
        BiFunction<XContentLocation, String, RuntimeException> documentParsingExceptionProvider,
        ParsingExceptionProvider parsingExceptionProvider
    ) throws IOException {
        ArrayList<Double> centroids = null;
        ArrayList<Long> counts = null;
        Double sum = null;
        Double min = null;
        Double max = null;
        XContentParser.Token token = parser.currentToken();
        while (token != XContentParser.Token.END_OBJECT) {
            // should be a field
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser, parsingExceptionProvider);
            String fieldName = parser.currentName();
            if (fieldName.equals(CENTROIDS_FIELD.getPreferredName())) {
                centroids = getCentroids(mappedFieldName, parser, documentParsingExceptionProvider, parsingExceptionProvider);
            } else if (fieldName.equals(COUNTS_FIELD.getPreferredName())) {
                counts = getCounts(mappedFieldName, parser, documentParsingExceptionProvider, parsingExceptionProvider);
            } else if (fieldName.equals(SUM_FIELD.getPreferredName())) {
                token = parser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser, parsingExceptionProvider);
                sum = parser.doubleValue();
            } else if (fieldName.equals(MIN_FIELD.getPreferredName())) {
                token = parser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser, parsingExceptionProvider);
                min = parser.doubleValue();
            } else if (fieldName.equals(MAX_FIELD.getPreferredName())) {
                token = parser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser, parsingExceptionProvider);
                max = parser.doubleValue();
            } else {
                throw documentParsingExceptionProvider.apply(
                    parser.getTokenLocation(),
                    "error parsing field [" + mappedFieldName + "], with unknown parameter [" + fieldName + "]"
                );
            }
            token = parser.nextToken();
        }
        if (centroids == null) {
            throw documentParsingExceptionProvider.apply(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + CENTROIDS_FIELD.getPreferredName() + "]"
            );
        }
        if (counts == null) {
            throw documentParsingExceptionProvider.apply(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + COUNTS_FIELD.getPreferredName() + "]"
            );
        }
        if (centroids.size() != counts.size()) {
            throw documentParsingExceptionProvider.apply(
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
        if (centroids.isEmpty()) {
            sum = null;
            min = null;
            max = null;
        }
        return new ParsedTDigest(centroids, counts, sum, min, max);
    }

    private static ArrayList<Long> getCounts(
        String mappedFieldName,
        XContentParser parser,
        BiFunction<XContentLocation, String, RuntimeException> documentParsingExceptionProvider,
        ParsingExceptionProvider parsingExceptionProvider
    ) throws IOException {
        ArrayList<Long> counts;
        XContentParser.Token token;
        token = parser.nextToken();
        // should be an array
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser, parsingExceptionProvider);
        counts = new ArrayList<>();
        token = parser.nextToken();
        while (token != XContentParser.Token.END_ARRAY) {
            // should be a number
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser, parsingExceptionProvider);
            long count = parser.longValue();
            if (count < 0) {
                throw documentParsingExceptionProvider.apply(
                    parser.getTokenLocation(),
                    "error parsing field [" + mappedFieldName + "], [" + COUNTS_FIELD + "] elements must be >= 0 but got " + count
                );
            }
            counts.add(count);
            token = parser.nextToken();
        }
        return counts;
    }

    private static ArrayList<Double> getCentroids(
        String mappedFieldName,
        XContentParser parser,
        BiFunction<XContentLocation, String, RuntimeException> documentParsingExceptionProvider,
        ParsingExceptionProvider parsingExceptionProvider
    ) throws IOException {
        XContentParser.Token token;
        ArrayList<Double> centroids;
        token = parser.nextToken();
        // should be an array
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser, parsingExceptionProvider);
        centroids = new ArrayList<>();
        token = parser.nextToken();
        double previousVal = -Double.MAX_VALUE;
        while (token != XContentParser.Token.END_ARRAY) {
            // should be a number
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser, parsingExceptionProvider);
            double val = parser.doubleValue();
            if (val < previousVal) {
                // centroids must be in increasing order
                throw documentParsingExceptionProvider.apply(
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
        return centroids;
    }

    /**
     * Interface for throwing a parsing exception, needed for visibility
     */
    @FunctionalInterface
    public interface ParsingExceptionProvider {
        RuntimeException apply(XContentParser parser, XContentParser.Token expected, XContentParser.Token actual) throws IOException;
    }

    public static void ensureExpectedToken(
        XContentParser.Token expected,
        XContentParser.Token actual,
        XContentParser parser,
        ParsingExceptionProvider parsingExceptionProvider
    ) throws IOException {
        if (actual != expected) {
            throw parsingExceptionProvider.apply(parser, expected, actual);
        }
    }

}
