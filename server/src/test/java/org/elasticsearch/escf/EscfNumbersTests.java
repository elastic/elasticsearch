/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Checks {@link EscfNumbers} against the x-content path it mirrors: a parser that demands {@code VALUE_NUMBER} and reads
 * it with {@code longValue()} or {@code doubleValue()}.
 */
public class EscfNumbersTests extends ESTestCase {

    private static final String FIELD = "f";

    /** Thrown by the error function, so the tests can tell it from any other failure. */
    private static final class Unsupported extends RuntimeException {
        Unsupported(String message) {
            super(message);
        }
    }

    /** A number as a long or as raw double bits, or {@code null} where reading it fails. */
    private interface Reader {
        @Nullable
        Long escf(EscfColumn column, int row);

        long xContent(XContentParser parser) throws IOException;
    }

    private static final Reader LONG = new Reader() {
        @Override
        public Long escf(EscfColumn column, int row) {
            try {
                return EscfNumbers.longValue(column, row, Unsupported::new);
            } catch (Unsupported e) {
                return null;
            }
        }

        @Override
        public long xContent(XContentParser parser) throws IOException {
            return parser.longValue();
        }
    };

    private static final Reader DOUBLE = new Reader() {
        @Override
        public Long escf(EscfColumn column, int row) {
            try {
                return Double.doubleToLongBits(EscfNumbers.doubleValue(column, row, Unsupported::new));
            } catch (Unsupported e) {
                return null;
            }
        }

        @Override
        public long xContent(XContentParser parser) throws IOException {
            return Double.doubleToLongBits(parser.doubleValue());
        }
    };

    public void testLongValueMatchesXContent() throws IOException {
        assertMatchesXContent(LONG);
    }

    public void testDoubleValueMatchesXContent() throws IOException {
        assertMatchesXContent(DOUBLE);
    }

    public void testLongValueTruncatesFractions() throws IOException {
        try (EscfBatch batch = encode(List.of("{\"f\":2.9}", "{\"f\":-0.5}", "{\"f\":5000000000}"))) {
            final EscfColumn column = column(batch);
            assertThat(EscfNumbers.longValue(column, 0, Unsupported::new), equalTo(2L));
            assertThat(EscfNumbers.longValue(column, 1, Unsupported::new), equalTo(0L));
            assertThat(EscfNumbers.longValue(column, 2, Unsupported::new), equalTo(5_000_000_000L));
        }
    }

    public void testLongValueOutOfRange() throws IOException {
        try (EscfBatch batch = encode(List.of("{\"f\":1}", "{\"f\":1e30}"))) {
            Unsupported e = expectThrows(Unsupported.class, () -> EscfNumbers.longValue(column(batch), 1, Unsupported::new));
            assertThat(e.getMessage(), equalTo("value [1.0E30] in document [1] is out of range for a long"));
        }
    }

    public void testNotANumber() throws IOException {
        try (EscfBatch batch = encode(List.of("{\"f\":1}", "{\"f\":\"1\"}"))) {
            Unsupported e = expectThrows(Unsupported.class, () -> EscfNumbers.doubleValue(column(batch), 1, Unsupported::new));
            assertThat(e.getMessage(), equalTo("value in document [1] is [STRING], not a number"));
        }
    }

    /**
     * Binary formats can hold numbers JSON cannot: their parsers, like the JSON one, let {@code NaN} narrow to a long as
     * {@code 0}, while infinities and other out-of-range values fail.
     */
    public void testBinaryFormatsMatchXContent() throws IOException {
        final double[] values = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1e30, -1e30, 9.3e18, 2.5, -0.0 };
        for (XContentType type : List.of(XContentType.CBOR, XContentType.SMILE)) {
            final List<BytesReference> sources = new ArrayList<>();
            for (double value : values) {
                for (boolean asFloat : new boolean[] { false, true }) {
                    try (XContentBuilder builder = XContentBuilder.builder(type.xContent())) {
                        builder.startObject();
                        if (asFloat) {
                            builder.field(FIELD, (float) value);
                        } else {
                            builder.field(FIELD, value);
                        }
                        sources.add(BytesReference.bytes(builder.endObject()));
                    }
                }
            }
            try (EscfBatch batch = EscfEncoder.encode(sources, type)) {
                final EscfColumn column = column(batch);
                for (int row = 0; row < sources.size(); row++) {
                    for (Reader reader : List.of(LONG, DOUBLE)) {
                        final Long expected = xContent(reader, type, sources.get(row));
                        assertThat(type + " row " + row, reader.escf(column, row), equalTo(expected));
                    }
                }
                assertThat(LONG.escf(column, 0), equalTo(0L));
                assertThat(LONG.escf(column, 1), equalTo(0L));
            }
        }
    }

    /** Random batches, so the column comes out as a LONG, DOUBLE or STRING column when its documents are uniform, and a UNION otherwise. */
    private void assertMatchesXContent(Reader reader) throws IOException {
        for (int iteration = 0; iteration < 300; iteration++) {
            final List<String> literals = randomSubsetOf(between(1, 4), LITERALS);
            final List<String> sources = new ArrayList<>();
            for (int d = 0, docs = between(1, 6); d < docs; d++) {
                sources.add(randomBoolean() ? "{}" : "{\"" + FIELD + "\":" + randomFrom(literals) + "}");
            }
            try (EscfBatch batch = encode(sources)) {
                final EscfColumn column = column(batch);
                if (column == null) {
                    continue;
                }
                for (int row = 0; row < sources.size(); row++) {
                    final Long expected = xContent(reader, XContentType.JSON, new BytesArray(sources.get(row)));
                    if (column.isPresent(row) == false) {
                        assertThat(sources.get(row), equalTo("{}"));
                        continue;
                    }
                    assertThat(sources + " row " + row, reader.escf(column, row), equalTo(expected));
                }
            }
        }
    }

    private static final List<String> LITERALS = List.of(
        "0",
        "3",
        "-1000",
        Long.toString(Integer.MAX_VALUE + 1L),
        Long.toString(Long.MAX_VALUE),
        Long.toString(Long.MIN_VALUE),
        "2.5",
        "-0.5",
        "0.1",
        "-0.0",
        "1e2",
        "1e30",
        "-1e30",
        "9.2e18",
        "9.3e18",
        "-9.3e18",
        "1.8446744073709552E19",
        "\"1\"",
        "\"x\"",
        "null",
        "true",
        "false",
        "[1]"
    );

    /** What the x-content path reads from the field of {@code source}, or {@code null} where it fails. */
    @Nullable
    private Long xContent(Reader reader, XContentType type, BytesReference source) throws IOException {
        try (XContentParser parser = createParser(type.xContent(), source)) {
            parser.nextToken();
            if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
                return null;
            }
            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                return null;
            }
            try {
                return reader.xContent(parser);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    private static EscfBatch encode(List<String> sources) throws IOException {
        final List<BytesReference> bytes = new ArrayList<>();
        for (String source : sources) {
            bytes.add(new BytesArray(source));
        }
        return EscfEncoder.encode(bytes, XContentType.JSON);
    }

    @Nullable
    private static EscfColumn column(EscfBatch batch) {
        for (int c = 0; c < batch.schema().leafCount(); c++) {
            if (batch.schema().getFullPath(c).equals(FIELD)) {
                return batch.column(c);
            }
        }
        return null;
    }
}
