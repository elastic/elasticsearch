/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 *
 * A Parser for the XContent representation of an ExponentialHistogram.
 * Implements the inverse of {@link ExponentialHistogramXContent#serialize(XContentBuilder, ExponentialHistogram)}.
 *
 * <p>Example for full exponential histogram JSON:
 * <pre><code>{
 *   "my_histo": {
 *     "scale": 12,
 *     "sum": 1234,
 *     "min": -123.456,
 *     "max": 456.456,
 *     "zero": {
 *       "threshold": 0.123456,
 *       "count": 42
 *     },
 *     "positive": {
 *       "indices": [-1000000, -10, 25, 26, 99999999],
 *       "counts": [1, 2, 3, 4, 5]
 *     },
 *     "negative": {
 *       "indices": [-123, 0, 12345],
 *       "counts": [20, 30, 40]
 *     }
 *    }
 * </code></pre>
 */
public class ExponentialHistogramParser {

    private static final ParseField SCALE_FIELD = new ParseField(ExponentialHistogramXContent.SCALE_FIELD);
    private static final ParseField SUM_FIELD = new ParseField(ExponentialHistogramXContent.SUM_FIELD);
    private static final ParseField MIN_FIELD = new ParseField(ExponentialHistogramXContent.MIN_FIELD);
    private static final ParseField MAX_FIELD = new ParseField(ExponentialHistogramXContent.MAX_FIELD);
    private static final ParseField ZERO_FIELD = new ParseField(ExponentialHistogramXContent.ZERO_FIELD);
    private static final ParseField ZERO_COUNT_FIELD = new ParseField(ExponentialHistogramXContent.ZERO_COUNT_FIELD);
    private static final ParseField ZERO_THRESHOLD_FIELD = new ParseField(ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD);

    private static final ParseField POSITIVE_FIELD = new ParseField(ExponentialHistogramXContent.POSITIVE_FIELD);
    private static final ParseField NEGATIVE_FIELD = new ParseField(ExponentialHistogramXContent.NEGATIVE_FIELD);
    private static final ParseField BUCKET_INDICES_FIELD = new ParseField(ExponentialHistogramXContent.BUCKET_INDICES_FIELD);
    private static final ParseField BUCKET_COUNTS_FIELD = new ParseField(ExponentialHistogramXContent.BUCKET_COUNTS_FIELD);

    public static final FeatureFlag EXPONENTIAL_HISTOGRAM_FEATURE = new FeatureFlag("exponential_histogram");

    private static final Set<String> ROOT_FIELD_NAMES = Set.of(
        SCALE_FIELD.getPreferredName(),
        SUM_FIELD.getPreferredName(),
        MIN_FIELD.getPreferredName(),
        MAX_FIELD.getPreferredName(),
        ZERO_FIELD.getPreferredName(),
        POSITIVE_FIELD.getPreferredName(),
        NEGATIVE_FIELD.getPreferredName()
    );

    public static boolean isExponentialHistogramSubFieldName(String subFieldName) {
        return ROOT_FIELD_NAMES.contains(subFieldName);
    }

    /**
     * Represents a parsed exponential histogram.
     * The values are validated, excepted for {@link #sum()}, {@link #min()} and {@link #max()}.
     *
     * @param scale see {@link ExponentialHistogram#scale()}
     * @param zeroThreshold see {@link ZeroBucket#zeroThreshold()}
     * @param zeroCount see {@link ZeroBucket#count()}
     * @param negativeBuckets see {@link ExponentialHistogram#negativeBuckets()}
     * @param positiveBuckets see {@link ExponentialHistogram#positiveBuckets()}
     * @param sum see {@link ExponentialHistogram#sum()}, not validated. Might be null if not provided
     * @param min see {@link ExponentialHistogram#min()}, not validated. Might be null if not provided
     * @param max see {@link ExponentialHistogram#max()}, not validated. Might be null if not provided
     */
    public record ParsedExponentialHistogram(
        int scale,
        double zeroThreshold,
        long zeroCount,
        List<IndexWithCount> negativeBuckets,
        List<IndexWithCount> positiveBuckets,
        Double sum,
        Double min,
        Double max
    ) {}

    private record ParsedZeroBucket(long count, double threshold) {
        static final ParsedZeroBucket DEFAULT = new ParsedZeroBucket(0, 0);
    }

    /**
     * Parses an XContent object into an exponential histogram.
     * The parser is expected to point at the next token after {@link XContentParser.Token#START_OBJECT}.
     *
     * @param mappedFieldName the name of the field being parsed, used for error messages
     * @param parser the parser to use
     * @return the parsed histogram
     */
    public static ParsedExponentialHistogram parse(String mappedFieldName, XContentParser parser) throws IOException {
        Double sum = null;
        Double min = null;
        Double max = null;
        Integer scale = null;
        ParsedZeroBucket zeroBucket = ParsedZeroBucket.DEFAULT;
        List<IndexWithCount> negativeBuckets = Collections.emptyList();
        List<IndexWithCount> positiveBuckets = Collections.emptyList();

        XContentParser.Token token = parser.currentToken();
        while (token != XContentParser.Token.END_OBJECT) {

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String fieldName = parser.currentName();
            if (fieldName.equals(SCALE_FIELD.getPreferredName())) {
                token = parser.nextToken();

                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                scale = parser.intValue();
                if (scale > ExponentialHistogram.MAX_SCALE || scale < ExponentialHistogram.MIN_SCALE) {
                    throw new DocumentParsingException(
                        parser.getTokenLocation(),
                        "error parsing field ["
                            + mappedFieldName
                            + "], scale field must be in "
                            + "range ["
                            + ExponentialHistogram.MIN_SCALE
                            + ", "
                            + ExponentialHistogram.MAX_SCALE
                            + "] but got "
                            + scale
                    );
                }
            } else if (fieldName.equals(SUM_FIELD.getPreferredName())) {
                sum = parseDoubleAllowingInfinity(mappedFieldName, parser);
            } else if (fieldName.equals(MIN_FIELD.getPreferredName())) {
                min = parseDoubleAllowingInfinity(mappedFieldName, parser);
            } else if (fieldName.equals(MAX_FIELD.getPreferredName())) {
                max = parseDoubleAllowingInfinity(mappedFieldName, parser);
            } else if (fieldName.equals(ZERO_FIELD.getPreferredName())) {
                zeroBucket = parseZeroBucket(mappedFieldName, parser);
            } else if (fieldName.equals(POSITIVE_FIELD.getPreferredName())) {
                positiveBuckets = readAndValidateBuckets(mappedFieldName, POSITIVE_FIELD.getPreferredName(), parser);
            } else if (fieldName.equals(NEGATIVE_FIELD.getPreferredName())) {
                negativeBuckets = readAndValidateBuckets(mappedFieldName, NEGATIVE_FIELD.getPreferredName(), parser);
            } else {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing field [" + mappedFieldName + "], with unknown parameter [" + fieldName + "]"
                );
            }
            token = parser.nextToken();
        }
        if (scale == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field [" + mappedFieldName + "], expected field called [" + SCALE_FIELD.getPreferredName() + "]"
            );
        }

        return new ParsedExponentialHistogram(
            scale,
            zeroBucket.threshold(),
            zeroBucket.count(),
            negativeBuckets,
            positiveBuckets,
            sum,
            min,
            max
        );
    }

    private static double parseDoubleAllowingInfinity(String mappedFieldName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        boolean isValidNumber = token == XContentParser.Token.VALUE_NUMBER;
        if (token == XContentParser.Token.VALUE_STRING) {
            String text = parser.text();
            if (text.equals("-Infinity") || text.equals("Infinity")) {
                isValidNumber = true;
            }
        }
        if (isValidNumber) {
            return parser.doubleValue();
        }
        throw new DocumentParsingException(
            parser.getTokenLocation(),
            "error parsing field [" + mappedFieldName + "], expected a number but got " + token
        );
    }

    private static ParsedZeroBucket parseZeroBucket(String mappedFieldName, XContentParser subParser) throws IOException {
        long zeroCount = ParsedZeroBucket.DEFAULT.count();
        double zeroThreshold = ParsedZeroBucket.DEFAULT.threshold();

        XContentParser.Token token = subParser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, subParser);
        token = subParser.nextToken();
        while (token != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, subParser);
            String fieldName = subParser.currentName();
            if (fieldName.equals(ZERO_THRESHOLD_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                zeroThreshold = subParser.doubleValue();
                if (zeroThreshold < 0.0 || Double.isFinite(zeroThreshold) == false) {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field ["
                            + mappedFieldName
                            + "], zero.threshold field must be a non-negative, finite number but got "
                            + zeroThreshold
                    );
                }
            } else if (fieldName.equals(ZERO_COUNT_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                zeroCount = subParser.longValue();
                if (zeroCount < 0) {
                    throw new DocumentParsingException(
                        subParser.getTokenLocation(),
                        "error parsing field [" + mappedFieldName + "], zero.count field must be a non-negative number but got " + zeroCount
                    );
                }
            } else {
                throw new DocumentParsingException(
                    subParser.getTokenLocation(),
                    "error parsing field [" + mappedFieldName + "], with unknown parameter for zero sub-object [" + fieldName + "]"
                );
            }
            token = subParser.nextToken();
        }
        return new ParsedZeroBucket(zeroCount, zeroThreshold);
    }

    private static List<IndexWithCount> readAndValidateBuckets(String mappedFieldName, String containerFieldName, XContentParser parser)
        throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        List<Long> indices = Collections.emptyList();
        List<Long> counts = Collections.emptyList();

        while (token != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String fieldName = parser.currentName();
            if (fieldName.equals(BUCKET_INDICES_FIELD.getPreferredName())) {
                indices = new ArrayList<>();
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                token = parser.nextToken();
                while (token != XContentParser.Token.END_ARRAY) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    long index = parser.longValue();
                    if (index < ExponentialHistogram.MIN_INDEX || index > ExponentialHistogram.MAX_INDEX) {
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field ["
                                + mappedFieldName
                                + "], "
                                + containerFieldName
                                + "."
                                + BUCKET_INDICES_FIELD.getPreferredName()
                                + " values must all be in range ["
                                + ExponentialHistogram.MIN_INDEX
                                + ", "
                                + ExponentialHistogram.MAX_INDEX
                                + "] but got "
                                + index
                        );
                    }
                    indices.add(index);
                    token = parser.nextToken();
                }
            } else if (fieldName.equals(BUCKET_COUNTS_FIELD.getPreferredName())) {
                counts = new ArrayList<>();
                token = parser.nextToken();
                // should be an array
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                token = parser.nextToken();
                while (token != XContentParser.Token.END_ARRAY) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    long count = parser.longValue();
                    if (count <= 0) {
                        throw new DocumentParsingException(
                            parser.getTokenLocation(),
                            "error parsing field ["
                                + mappedFieldName
                                + "], "
                                + containerFieldName
                                + "."
                                + BUCKET_COUNTS_FIELD.getPreferredName()
                                + " values must all be greater than zero but got "
                                + count
                        );
                    }
                    counts.add(count);
                    token = parser.nextToken();
                }
            } else {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing field ["
                        + mappedFieldName
                        + "], with unknown parameter for "
                        + containerFieldName
                        + " sub-object ["
                        + fieldName
                        + "]"
                );
            }
            token = parser.nextToken();
        }

        if (indices.size() != counts.size()) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "error parsing field ["
                    + mappedFieldName
                    + "], expected same length from ["
                    + containerFieldName
                    + "."
                    + BUCKET_INDICES_FIELD.getPreferredName()
                    + "] and "
                    + "["
                    + containerFieldName
                    + "."
                    + BUCKET_COUNTS_FIELD.getPreferredName()
                    + "] but got ["
                    + indices.size()
                    + " != "
                    + counts.size()
                    + "]"
            );
        }

        List<IndexWithCount> results = new ArrayList<>(indices.size());
        for (int i = 0; i < indices.size(); i++) {
            results.add(new IndexWithCount(indices.get(i), counts.get(i)));
        }
        results.sort(Comparator.comparing(IndexWithCount::index));

        for (int i = 1; i < results.size(); i++) {
            long index = results.get(i).index();
            if (index == results.get(i - 1).index()) {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "error parsing field ["
                        + mappedFieldName
                        + "], expected entries of ["
                        + containerFieldName
                        + "."
                        + BUCKET_INDICES_FIELD.getPreferredName()
                        + "] to be unique, but got "
                        + index
                        + " multiple times"
                );
            }
        }
        return results;
    }
}
