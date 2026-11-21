/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

/**
 * Checks an {@link EscfNumericArrayIterator} against the x-content path it mirrors: a parser that demands
 * {@code START_ARRAY}, then {@code VALUE_NUMBER} per element, and reads each with {@code longValue()} or
 * {@code doubleValue()}.
 */
public abstract class AbstractEscfNumericArrayIteratorTestCase extends ESTestCase {

    private static final String FIELD = "f";

    /** A document's elements, as longs or as raw double bits, and whether reading it stopped on a failure. */
    protected record Row(int row, List<Long> values, boolean failed) {}

    /** Thrown by the iterators' error function, so the tests can tell it from any other failure. */
    protected static final class Unsupported extends RuntimeException {
        Unsupported(String message) {
            super(message);
        }
    }

    protected abstract EscfNumericArrayIterator iterator(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported);

    /** The iterator's current element, as a long or as raw double bits. */
    protected abstract long value(EscfNumericArrayIterator iterator);

    /** The parser's current number, read the way the iterator mirrors, in the shape of {@link #value}. */
    protected abstract long xContentValue(XContentParser parser) throws IOException;

    /**
     * Random batches, so the column comes out as an ARRAY when every document is uniform, as a UNION otherwise, and
     * as a scalar when no document has an array.
     */
    public void testMatchesXContent() throws IOException {
        for (int iteration = 0; iteration < 300; iteration++) {

            List<ElementShape> shapes = randomSubsetOf(randomIntBetween(1, ElementShape.values().length), ElementShape.values());
            final List<String> sources = new ArrayList<>();
            for (int d = 0, docs = between(1, 6); d < docs; d++) {
                sources.add(randomDocument(randomFrom(shapes)));
            }
            try (EscfBatch batch = encode(sources)) {
                final EscfColumn column = column(batch);
                if (column == null) {
                    continue;
                }
                assertThat(sources.toString(), escf(column), equalTo(xContent(sources)));
            }
        }
    }

    /** Restricts element literals per batch, to push the encoder towards ARRAY columns as well as UNION ones. */
    private enum ElementShape {
        INTEGERS,
        FRACTIONS,
        ANYTHING
    }

    private String randomDocument(ElementShape shape) {
        return switch (between(0, 9)) {
            case 0 -> "{}";
            case 1 -> "{\"" + FIELD + "\":" + randomFrom("3", "2.5", "\"x\"", "null", "true") + "}";
            default -> {
                final List<String> elements = new ArrayList<>();
                for (int e = 0, count = between(0, 5); e < count; e++) {
                    elements.add(switch (shape) {
                        case INTEGERS -> Integer.toString(between(-1000, 1000));
                        case FRACTIONS -> between(-100, 100) + ".5";
                        case ANYTHING -> switch (between(0, 8)) {
                            case 0 -> Integer.toString(between(-1000, 1000));
                            case 1 -> Long.toString(randomLong());
                            case 2 -> Long.toString(
                                randomFrom(Integer.MAX_VALUE + 1L, Integer.MIN_VALUE - 1L, Long.MAX_VALUE, Long.MIN_VALUE)
                            );
                            case 3 -> between(-100, 100) + randomFrom(".5", ".25", ".999");
                            case 4 -> Double.toString(randomDouble() * randomFrom(-1e-10, 1.0, 1e10, -1e18));
                            // Out of long range, just in and just out of it, float-exact 2^64, exponents, negative zero.
                            case 5 -> randomFrom(
                                "1e30",
                                "-1e30",
                                "9.3e18",
                                "-9.3e18",
                                "9.2e18",
                                "1.8446744073709552E19",
                                "1e2",
                                "-0.0",
                                "0.1"
                            );
                            case 6 -> "\"" + between(0, 9) + "\"";
                            case 7 -> randomFrom("true", "false", "null");
                            default -> "[" + between(0, 9) + "]";
                        };
                    });
                }
                yield "{\"" + FIELD + "\":[" + String.join(",", elements) + "]}";
            }
        };
    }

    /**
     * The reference: every document with a value for the field, in order, up to the first one that fails, which is
     * as far as a caller handing the batch to the row path on failure gets.
     */
    protected final List<Row> xContent(List<String> sources) throws IOException {
        final List<Row> rows = new ArrayList<>();
        for (int row = 0; row < sources.size(); row++) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, sources.get(row))) {
                parser.nextToken();
                if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
                    continue;
                }
                final Row result = xContentRow(parser, row);
                rows.add(result);
                if (result.failed()) {
                    break;
                }
            }
        }
        return rows;
    }

    private Row xContentRow(XContentParser parser, int row) throws IOException {
        final List<Long> values = new ArrayList<>();
        if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
            return new Row(row, values, true);
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                return new Row(row, values, true);
            }
            try {
                values.add(xContentValue(parser));
            } catch (IllegalArgumentException e) {
                return new Row(row, values, true);
            }
        }
        return new Row(row, values, false);
    }

    /** What the iterator reads from {@code column}, in the shape of {@link #xContent}. */
    protected final List<Row> escf(EscfColumn column) {
        final List<Row> rows = new ArrayList<>();
        final EscfNumericArrayIterator iterator;
        try {
            iterator = iterator(column, Unsupported::new);
        } catch (Unsupported e) {
            rows.add(new Row(column.presentDocs().nextDoc(), List.of(), true));
            return rows;
        }
        List<Long> values = new ArrayList<>();
        try {
            for (; iterator.nextRow() != DocIdSetIterator.NO_MORE_DOCS; values = new ArrayList<>()) {
                while (iterator.nextArrayEntry()) {
                    values.add(value(iterator));
                }
                rows.add(new Row(iterator.row(), values, false));
            }
        } catch (Unsupported e) {
            rows.add(new Row(iterator.row(), values, true));
        }
        return rows;
    }

    protected static EscfBatch encode(List<String> sources) throws IOException {
        final List<BytesReference> bytes = new ArrayList<>();
        for (String source : sources) {
            bytes.add(new BytesArray(source));
        }
        return EscfEncoder.encode(bytes, XContentType.JSON);
    }

    @Nullable
    protected static EscfColumn column(EscfBatch batch) {
        for (int c = 0; c < batch.schema().leafCount(); c++) {
            if (batch.schema().getFullPath(c).equals(FIELD)) {
                return batch.column(c);
            }
        }
        return null;
    }
}
