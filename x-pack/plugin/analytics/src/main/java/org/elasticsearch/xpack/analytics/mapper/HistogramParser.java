/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfDoubleArrayIterator;
import org.elasticsearch.escf.EscfLongArrayIterator;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;

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

    /**
     * The columnar counterpart of {@link #parse}: reads the {@code values} and {@code counts} columns the encoder
     * produces for a batch of histogram objects, yielding the centroids of each document that has one, in document
     * order. Wherever {@link #parse} would fail, this throws what {@code onUnsupported} builds, so that the caller can
     * hand the batch to the row path instead.
     *
     * <p>That includes a duplicate relative key, which {@code ShardBatchMapper#resolveMappers} lets through for
     * group mappers on the grounds that {@code flattened} can merge aliased leaves; a histogram has nowhere to
     * put a second {@code values} array.
     *
     * <p>{@code consumer} receives each document's centroids with its row; the iterator is reused, so it is only valid
     * during the call.
     */
    public static void parseColumns(
        EscfColumn[] columns,
        String[] relativeKeys,
        Function<String, ? extends RuntimeException> onUnsupported,
        ObjIntConsumer<EncodedTDigest.CentroidIterator> consumer
    ) {
        assert columns.length == relativeKeys.length : columns.length + " columns vs " + relativeKeys.length + " keys";
        int valuesIdx = -1;
        int countsIdx = -1;
        for (int k = 0; k < relativeKeys.length; k++) {
            final String key = relativeKeys[k];
            if (VALUES_FIELD.getPreferredName().equals(key)) {
                if (valuesIdx != -1) {
                    throw onUnsupported.apply("column group has duplicate [" + VALUES_FIELD + "] key");
                }
                valuesIdx = k;
            } else if (COUNTS_FIELD.getPreferredName().equals(key)) {
                if (countsIdx != -1) {
                    throw onUnsupported.apply("column group has duplicate [" + COUNTS_FIELD + "] key");
                }
                countsIdx = k;
            } else {
                throw onUnsupported.apply("column group has unexpected relative key [" + key + "]");
            }
        }
        if (valuesIdx == -1 || countsIdx == -1) {
            throw onUnsupported.apply("column group is missing [" + (valuesIdx == -1 ? VALUES_FIELD : COUNTS_FIELD) + "]");
        }

        // parse() reads values with doubleValue() and counts with longValue(), after demanding an array of numbers
        // under each key; these mirror exactly that.
        final EscfDoubleArrayIterator values = EscfDoubleArrayIterator.of(
            columns[valuesIdx],
            message -> onUnsupported.apply("[" + VALUES_FIELD + "] " + message)
        );
        final EscfLongArrayIterator counts = EscfLongArrayIterator.of(
            columns[countsIdx],
            message -> onUnsupported.apply("[" + COUNTS_FIELD + "] " + message)
        );
        final ColumnCentroidIterator centroids = new ColumnCentroidIterator(values, counts, onUnsupported);
        while (true) {
            final int row = values.nextRow();
            final int countsRow = counts.nextRow();
            // parse() fails on a histogram object missing either key.
            if (row != countsRow) {
                throw onUnsupported.apply(
                    "document ["
                        + Math.min(row, countsRow)
                        + "] carries only one of ["
                        + VALUES_FIELD
                        + "] and ["
                        + COUNTS_FIELD
                        + "] for columnar batch indexing"
                );
            }
            if (row == DocIdSetIterator.NO_MORE_DOCS) {
                return;
            }
            centroids.reset();
            consumer.accept(centroids, row);
        }
    }

    /**
     * One document's {@code values} and {@code counts}, walked in step and checked as {@link #parse} checks them.
     *
     * <p>Unlike {@link #parse}, which validates each array in full before comparing their lengths, this interleaves
     * the checks, so a message names whichever problem comes first in array order.
     */
    private static final class ColumnCentroidIterator implements EncodedTDigest.CentroidIterator {

        private final EscfDoubleArrayIterator values;
        private final EscfLongArrayIterator counts;
        private final Function<String, ? extends RuntimeException> onUnsupported;
        private double previousMean;
        private double currentMean;
        private long currentCount;

        private ColumnCentroidIterator(
            EscfDoubleArrayIterator values,
            EscfLongArrayIterator counts,
            Function<String, ? extends RuntimeException> onUnsupported
        ) {
            this.values = values;
            this.counts = counts;
            this.onUnsupported = onUnsupported;
        }

        /** Starts over on the row both column iterators are now on. */
        void reset() {
            // Matches parse()'s seed, which rejects a leading -Double.MAX_VALUE-or-lower mean.
            this.previousMean = -Double.MAX_VALUE;
        }

        @Override
        public boolean next() {
            final boolean hasValue = values.nextArrayEntry();
            if (hasValue != counts.nextArrayEntry()) {
                throw onUnsupported.apply("[" + VALUES_FIELD + "] and [" + COUNTS_FIELD + "] have different lengths");
            }
            if (hasValue == false) {
                return false;
            }
            final double mean = values.value();
            if (mean < previousMean) {
                throw onUnsupported.apply("[" + VALUES_FIELD + "] values are not in increasing order");
            }
            previousMean = mean;
            // Fractional counts are truncated before this check, so -0.5 is accepted as 0, as by parse().
            final long count = counts.value();
            if (count < 0) {
                throw onUnsupported.apply("[" + COUNTS_FIELD + "] has a negative element");
            }
            currentMean = mean;
            currentCount = count;
            return true;
        }

        @Override
        public double currentMean() {
            return currentMean;
        }

        @Override
        public long currentCount() {
            return currentCount;
        }
    }
}
