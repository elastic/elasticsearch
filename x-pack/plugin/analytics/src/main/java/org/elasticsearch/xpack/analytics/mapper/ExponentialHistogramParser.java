/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfLongArrayIterator;
import org.elasticsearch.escf.EscfNumbers;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;

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

    private static final Set<String> ROOT_FIELD_NAMES = Set.of(
        SCALE_FIELD.getPreferredName(),
        SUM_FIELD.getPreferredName(),
        MIN_FIELD.getPreferredName(),
        MAX_FIELD.getPreferredName(),
        ZERO_FIELD.getPreferredName(),
        POSITIVE_FIELD.getPreferredName(),
        NEGATIVE_FIELD.getPreferredName()
    );

    /** The one relative key every histogram object needs, see {@link #parse}. */
    public static final String SCALE_KEY = ExponentialHistogramXContent.SCALE_FIELD;
    private static final String SUM_KEY = ExponentialHistogramXContent.SUM_FIELD;
    private static final String MIN_KEY = ExponentialHistogramXContent.MIN_FIELD;
    private static final String MAX_KEY = ExponentialHistogramXContent.MAX_FIELD;
    private static final String ZERO_COUNT_KEY = ExponentialHistogramXContent.ZERO_FIELD
        + "."
        + ExponentialHistogramXContent.ZERO_COUNT_FIELD;
    private static final String ZERO_THRESHOLD_KEY = ExponentialHistogramXContent.ZERO_FIELD
        + "."
        + ExponentialHistogramXContent.ZERO_THRESHOLD_FIELD;
    private static final String POSITIVE_INDICES_KEY = ExponentialHistogramXContent.POSITIVE_FIELD
        + "."
        + ExponentialHistogramXContent.BUCKET_INDICES_FIELD;
    private static final String POSITIVE_COUNTS_KEY = ExponentialHistogramXContent.POSITIVE_FIELD
        + "."
        + ExponentialHistogramXContent.BUCKET_COUNTS_FIELD;
    private static final String NEGATIVE_INDICES_KEY = ExponentialHistogramXContent.NEGATIVE_FIELD
        + "."
        + ExponentialHistogramXContent.BUCKET_INDICES_FIELD;
    private static final String NEGATIVE_COUNTS_KEY = ExponentialHistogramXContent.NEGATIVE_FIELD
        + "."
        + ExponentialHistogramXContent.BUCKET_COUNTS_FIELD;

    /** The relative keys {@link #parseColumns} reads: every leaf of a histogram object that {@link #parse} accepts. */
    public static final Set<String> COLUMN_KEYS = Set.of(
        SCALE_KEY,
        SUM_KEY,
        MIN_KEY,
        MAX_KEY,
        ZERO_COUNT_KEY,
        ZERO_THRESHOLD_KEY,
        POSITIVE_INDICES_KEY,
        POSITIVE_COUNTS_KEY,
        NEGATIVE_INDICES_KEY,
        NEGATIVE_COUNTS_KEY
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
                    throw new DocumentParsingException(parser.getTokenLocation(), scaleOutOfRangeMessage(mappedFieldName, scale));
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
            throw new DocumentParsingException(parser.getTokenLocation(), missingScaleMessage(mappedFieldName));
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
                        invalidZeroThresholdMessage(mappedFieldName, zeroThreshold)
                    );
                }
            } else if (fieldName.equals(ZERO_COUNT_FIELD.getPreferredName())) {
                token = subParser.nextToken();
                ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, subParser);
                zeroCount = subParser.longValue();
                if (zeroCount < 0) {
                    throw new DocumentParsingException(subParser.getTokenLocation(), negativeZeroCountMessage(mappedFieldName, zeroCount));
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
        final String indicesKey = containerFieldName + "." + BUCKET_INDICES_FIELD.getPreferredName();
        final String countsKey = containerFieldName + "." + BUCKET_COUNTS_FIELD.getPreferredName();
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
                            indexOutOfRangeMessage(mappedFieldName, indicesKey, index)
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
                            nonPositiveCountMessage(mappedFieldName, countsKey, count)
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
                lengthMismatchMessage(mappedFieldName, indicesKey, countsKey, indices.size(), counts.size())
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
                throw new DocumentParsingException(parser.getTokenLocation(), duplicateIndexMessage(mappedFieldName, indicesKey, index));
            }
        }
        return results;
    }

    private static String missingScaleMessage(String mappedFieldName) {
        return "error parsing field [" + mappedFieldName + "], expected field called [" + SCALE_KEY + "]";
    }

    private static String scaleOutOfRangeMessage(String mappedFieldName, long scale) {
        return "error parsing field ["
            + mappedFieldName
            + "], scale field must be in range ["
            + ExponentialHistogram.MIN_SCALE
            + ", "
            + ExponentialHistogram.MAX_SCALE
            + "] but got "
            + scale;
    }

    private static String invalidZeroThresholdMessage(String mappedFieldName, double zeroThreshold) {
        return "error parsing field ["
            + mappedFieldName
            + "], zero.threshold field must be a non-negative, finite number but got "
            + zeroThreshold;
    }

    private static String negativeZeroCountMessage(String mappedFieldName, long zeroCount) {
        return "error parsing field [" + mappedFieldName + "], zero.count field must be a non-negative number but got " + zeroCount;
    }

    private static String indexOutOfRangeMessage(String mappedFieldName, String indicesKey, long index) {
        return "error parsing field ["
            + mappedFieldName
            + "], "
            + indicesKey
            + " values must all be in range ["
            + ExponentialHistogram.MIN_INDEX
            + ", "
            + ExponentialHistogram.MAX_INDEX
            + "] but got "
            + index;
    }

    private static String nonPositiveCountMessage(String mappedFieldName, String countsKey, long count) {
        return "error parsing field [" + mappedFieldName + "], " + countsKey + " values must all be greater than zero but got " + count;
    }

    private static String lengthMismatchMessage(String mappedFieldName, String indicesKey, String countsKey, int indices, int counts) {
        return "error parsing field ["
            + mappedFieldName
            + "], expected same length from ["
            + indicesKey
            + "] and ["
            + countsKey
            + "] but got ["
            + indices
            + " != "
            + counts
            + "]";
    }

    private static String duplicateIndexMessage(String mappedFieldName, String indicesKey, long index) {
        return "error parsing field ["
            + mappedFieldName
            + "], expected entries of ["
            + indicesKey
            + "] to be unique, but got "
            + index
            + " multiple times";
    }

    /**
     * The columnar counterpart of {@link #parse}: reads the columns the encoder produces for a batch of histogram objects,
     * yielding the parsed histogram of each document that has one, in document order. Wherever {@link #parse} would fail,
     * this throws what {@code onUnsupported} builds, so that the caller can hand the batch to the row path instead. Where
     * {@link #parse} rejects a value, the message is the same; type errors are worded by {@link EscfNumbers} and the
     * array iterators.
     *
     * <p>A batch with a key outside {@link #COLUMN_KEYS}, or whose histogram is spread over more than one object, falls
     * back before reaching this, so the key checks here only guard against callers that skip those checks.
     *
     * @param mappedFieldName the name of the field being parsed, used for error messages
     */
    public static void parseColumns(
        String mappedFieldName,
        EscfColumn[] columns,
        String[] relativeKeys,
        Function<String, ? extends RuntimeException> onUnsupported,
        ObjIntConsumer<ParsedExponentialHistogram> consumer
    ) {
        assert columns.length == relativeKeys.length : columns.length + " columns vs " + relativeKeys.length + " keys";
        EscfColumn scale = null;
        EscfColumn sum = null;
        EscfColumn min = null;
        EscfColumn max = null;
        EscfColumn zeroCount = null;
        EscfColumn zeroThreshold = null;
        EscfColumn positiveIndices = null;
        EscfColumn positiveCounts = null;
        EscfColumn negativeIndices = null;
        EscfColumn negativeCounts = null;
        for (int k = 0; k < relativeKeys.length; k++) {
            final String key = relativeKeys[k];
            final EscfColumn column = columns[k];
            switch (key) {
                case SCALE_KEY -> {
                    throwDuplicateExceptionIfNotNull(scale, key, onUnsupported);
                    scale = column;
                }
                case SUM_KEY -> {
                    throwDuplicateExceptionIfNotNull(sum, key, onUnsupported);
                    sum = column;
                }
                case MIN_KEY -> {
                    throwDuplicateExceptionIfNotNull(min, key, onUnsupported);
                    min = column;
                }
                case MAX_KEY -> {
                    throwDuplicateExceptionIfNotNull(max, key, onUnsupported);
                    max = column;
                }
                case ZERO_COUNT_KEY -> {
                    throwDuplicateExceptionIfNotNull(zeroCount, key, onUnsupported);
                    zeroCount = column;
                }
                case ZERO_THRESHOLD_KEY -> {
                    throwDuplicateExceptionIfNotNull(zeroThreshold, key, onUnsupported);
                    zeroThreshold = column;
                }
                case POSITIVE_INDICES_KEY -> {
                    throwDuplicateExceptionIfNotNull(positiveIndices, key, onUnsupported);
                    positiveIndices = column;
                }
                case POSITIVE_COUNTS_KEY -> {
                    throwDuplicateExceptionIfNotNull(positiveCounts, key, onUnsupported);
                    positiveCounts = column;
                }
                case NEGATIVE_INDICES_KEY -> {
                    throwDuplicateExceptionIfNotNull(negativeIndices, key, onUnsupported);
                    negativeIndices = column;
                }
                case NEGATIVE_COUNTS_KEY -> {
                    throwDuplicateExceptionIfNotNull(negativeCounts, key, onUnsupported);
                    negativeCounts = column;
                }
                default -> throw onUnsupported.apply("column group has unexpected relative key [" + key + "]");
            }
        }
        if (columns.length == 0) {
            return;
        }
        final ColumnBuckets positive = new ColumnBuckets(
            mappedFieldName,
            POSITIVE_INDICES_KEY,
            positiveIndices,
            POSITIVE_COUNTS_KEY,
            positiveCounts,
            onUnsupported
        );
        final ColumnBuckets negative = new ColumnBuckets(
            mappedFieldName,
            NEGATIVE_INDICES_KEY,
            negativeIndices,
            NEGATIVE_COUNTS_KEY,
            negativeCounts,
            onUnsupported
        );
        final Function<String, RuntimeException> scaleErrors = keyedErrorHandler(SCALE_KEY, onUnsupported);
        final Function<String, RuntimeException> zeroThresholdErrors = keyedErrorHandler(ZERO_THRESHOLD_KEY, onUnsupported);
        final Function<String, RuntimeException> zeroCountErrors = keyedErrorHandler(ZERO_COUNT_KEY, onUnsupported);
        final Function<String, RuntimeException> sumErrors = keyedErrorHandler(SUM_KEY, onUnsupported);
        final Function<String, RuntimeException> minErrors = keyedErrorHandler(MIN_KEY, onUnsupported);
        final Function<String, RuntimeException> maxErrors = keyedErrorHandler(MAX_KEY, onUnsupported);
        final int docCount = columns[0].docCount();
        for (int row = 0; row < docCount; row++) {
            if (isPresentInAny(columns, row) == false) {
                continue;
            }
            if (scale == null || scale.isPresent(row) == false) {
                throw onUnsupported.apply(missingScaleMessage(mappedFieldName));
            }
            final long scaleValue = EscfNumbers.longValue(scale, row, scaleErrors);
            if (scaleValue > ExponentialHistogram.MAX_SCALE || scaleValue < ExponentialHistogram.MIN_SCALE) {
                throw onUnsupported.apply(scaleOutOfRangeMessage(mappedFieldName, scaleValue));
            }
            final double threshold = zeroThreshold != null && zeroThreshold.isPresent(row)
                ? EscfNumbers.doubleValue(zeroThreshold, row, zeroThresholdErrors)
                : ParsedZeroBucket.DEFAULT.threshold();
            if (threshold < 0.0 || Double.isFinite(threshold) == false) {
                throw onUnsupported.apply(invalidZeroThresholdMessage(mappedFieldName, threshold));
            }
            final long zeroCountValue = zeroCount != null && zeroCount.isPresent(row)
                ? EscfNumbers.longValue(zeroCount, row, zeroCountErrors)
                : ParsedZeroBucket.DEFAULT.count();
            if (zeroCountValue < 0) {
                throw onUnsupported.apply(negativeZeroCountMessage(mappedFieldName, zeroCountValue));
            }
            consumer.accept(
                new ParsedExponentialHistogram(
                    (int) scaleValue,
                    threshold,
                    zeroCountValue,
                    negative.read(row),
                    positive.read(row),
                    doubleAllowingInfinity(sum, row, sumErrors),
                    doubleAllowingInfinity(min, row, minErrors),
                    doubleAllowingInfinity(max, row, maxErrors)
                ),
                row
            );
        }
    }

    private static void throwDuplicateExceptionIfNotNull(
        EscfColumn previous,
        String key,
        Function<String, ? extends RuntimeException> onUnsupported
    ) {
        if (previous != null) {
            throw onUnsupported.apply("column group has duplicate [" + key + "] key");
        }
    }

    private static boolean isPresentInAny(EscfColumn[] columns, int row) {
        for (EscfColumn column : columns) {
            if (column.isPresent(row)) {
                return true;
            }
        }
        return false;
    }

    /** Prefixes the messages of {@code onUnsupported} with the relative key they are about. */
    private static Function<String, RuntimeException> keyedErrorHandler(
        String key,
        Function<String, ? extends RuntimeException> onUnsupported
    ) {
        return message -> onUnsupported.apply("[" + key + "] " + message);
    }

    /** The columnar counterpart of {@link #parseDoubleAllowingInfinity}, or {@code null} where the document has no value. */
    private static Double doubleAllowingInfinity(EscfColumn column, int row, Function<String, ? extends RuntimeException> onUnsupported) {
        if (column == null || column.isPresent(row) == false) {
            return null;
        }
        if (column.getTypeByte(row) == SourceValueType.STRING) {
            final String text = column.getStringValue(row).string();
            if (text.equals("Infinity")) {
                return Double.POSITIVE_INFINITY;
            }
            if (text.equals("-Infinity")) {
                return Double.NEGATIVE_INFINITY;
            }
        }
        return EscfNumbers.doubleValue(column, row, onUnsupported);
    }

    /**
     * One bucket range's {@code indices} and {@code counts} columns, read and checked as {@link #readAndValidateBuckets} does.
     * Either column may be missing from the batch or from a document, which {@link #parse} reads as an empty array.
     */
    private static final class ColumnBuckets {
        private final String mappedFieldName;
        private final String indicesKey;
        private final String countsKey;
        private final RowCursor indices;
        private final RowCursor counts;
        private final Function<String, ? extends RuntimeException> onUnsupported;
        private int lastRow = -1;

        ColumnBuckets(
            String mappedFieldName,
            String indicesKey,
            EscfColumn indices,
            String countsKey,
            EscfColumn counts,
            Function<String, ? extends RuntimeException> onUnsupported
        ) {
            this.mappedFieldName = mappedFieldName;
            this.indicesKey = indicesKey;
            this.countsKey = countsKey;
            this.onUnsupported = onUnsupported;
            this.indices = indices == null ? null : new RowCursor(indices, indicesKey, onUnsupported);
            this.counts = counts == null ? null : new RowCursor(counts, countsKey, onUnsupported);
        }

        /** The sorted buckets of document {@code row}; rows must be read in increasing order. */
        List<IndexWithCount> read(int row) {
            assert row > lastRow : "read row [" + row + "] after row [" + lastRow + "]";
            lastRow = row;
            final boolean hasIndices = indices != null && indices.advanceTo(row);
            final boolean hasCounts = counts != null && counts.advanceTo(row);
            if (hasIndices == false && hasCounts == false) {
                return Collections.emptyList();
            }
            final List<IndexWithCount> results = new ArrayList<>();
            // Both arrays are read to their end, checking every element, so that a length mismatch reports both lengths.
            boolean indicesLeft = hasIndices;
            boolean countsLeft = hasCounts;
            int indexCount = 0;
            int countCount = 0;
            while (true) {
                indicesLeft = indicesLeft && indices.values.nextArrayEntry();
                countsLeft = countsLeft && counts.values.nextArrayEntry();
                if (indicesLeft == false && countsLeft == false) {
                    break;
                }
                long index = 0;
                if (indicesLeft) {
                    indexCount++;
                    index = indices.values.value();
                    if (index < ExponentialHistogram.MIN_INDEX || index > ExponentialHistogram.MAX_INDEX) {
                        throw onUnsupported.apply(indexOutOfRangeMessage(mappedFieldName, indicesKey, index));
                    }
                }
                long count = 0;
                if (countsLeft) {
                    countCount++;
                    count = counts.values.value();
                    if (count <= 0) {
                        throw onUnsupported.apply(nonPositiveCountMessage(mappedFieldName, countsKey, count));
                    }
                }
                if (indicesLeft && countsLeft) {
                    results.add(new IndexWithCount(index, count));
                }
            }
            if (indexCount != countCount) {
                throw onUnsupported.apply(lengthMismatchMessage(mappedFieldName, indicesKey, countsKey, indexCount, countCount));
            }
            results.sort(Comparator.comparing(IndexWithCount::index));
            for (int i = 1; i < results.size(); i++) {
                if (results.get(i).index() == results.get(i - 1).index()) {
                    throw onUnsupported.apply(duplicateIndexMessage(mappedFieldName, indicesKey, results.get(i).index()));
                }
            }
            return results;
        }
    }

    /** An {@link EscfLongArrayIterator} that can be positioned on a given row, for columns read alongside others. */
    private static final class RowCursor {
        private final EscfLongArrayIterator values;
        private int row = -1;

        RowCursor(EscfColumn column, String key, Function<String, ? extends RuntimeException> onUnsupported) {
            this.values = EscfLongArrayIterator.of(column, keyedErrorHandler(key, onUnsupported));
        }

        /** Whether {@code target} has a value in this column, positioning on it if so. Targets must not decrease. */
        boolean advanceTo(int target) {
            while (row < target) {
                row = values.nextRow();
            }
            return row == target;
        }
    }
}
