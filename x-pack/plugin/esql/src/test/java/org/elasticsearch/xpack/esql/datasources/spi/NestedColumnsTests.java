/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class NestedColumnsTests extends ESTestCase {

    private static final NestedColumns COLUMNS = NestedColumns.of(
        List.of(
            new NestedColumns.NestedColumn(
                "answers",
                Map.of(
                    "type",
                    DataType.KEYWORD,
                    "ttl",
                    DataType.LONG,
                    "at",
                    DataType.DATE_NANOS,
                    "score",
                    DataType.DOUBLE,
                    "big",
                    DataType.UNSIGNED_LONG,
                    "m",
                    DataType.UNSUPPORTED
                ),
                null
            ),
            new NestedColumns.NestedColumn("hops", Map.of("host", DataType.KEYWORD), null, "[hops.links] cannot be read yet"),
            new NestedColumns.NestedColumn("spans", Map.of("id", DataType.INTEGER), "[spans] is a list of structures in [a] but not in [b]")
        )
    );

    /** The columns survive the generic stream a relation's source metadata is written with, beside the keys already there. */
    public void testRoundTripThroughSourceMetadataStream() throws IOException {
        Map<String, Object> written = COLUMNS.writeTo(Map.of("_sample.rows", 3));
        Map<String, Object> read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(written);
            try (StreamInput in = out.bytes().streamInput()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) in.readGenericValue();
                read = value;
            }
        }
        assertThat(NestedColumns.read(read), equalTo(COLUMNS));
        assertThat(read.get("_sample.rows"), equalTo(3));
    }

    /** No key is written for no columns, so a source without any looks as it did before. */
    public void testNoneWritesNoKey() {
        assertThat(NestedColumns.NONE.writeTo(Map.of("a", 1)), not(hasKey(NestedColumns.KEY)));
        assertThat(NestedColumns.read(Map.of("a", 1)), equalTo(NestedColumns.NONE));
        assertThat(NestedColumns.read(null), equalTo(NestedColumns.NONE));
    }

    /** A record that is not one writeTo writes is refused by name, not read as a cast failure. */
    public void testMalformedRecordRefused() {
        List<Object> malformed = List.of(
            List.of(),
            Map.of("answers", "not a map"),
            Map.of("answers", Map.of()),
            Map.of("answers", Map.of("leaves", Map.of("ttl", 3))),
            Map.of("answers", Map.of("leaves", Map.of("ttl", "no_such_type"))),
            Map.of("answers", Map.of("leaves", Map.of(), "conflict", 1))
        );
        for (Object record : malformed) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> NestedColumns.read(Map.of(NestedColumns.KEY, record))
            );
            assertThat(e.getMessage(), startsWith("malformed nested column description at ["));
        }
    }

    /** The cache weight reaches every field name, type name and reason, deeper than a two-level map estimate. */
    public void testEstimatedBytesIsDeep() {
        Map<String, Object> written = COLUMNS.writeTo(null);
        long weight = NestedColumns.estimatedBytes(written);
        assertThat(weight, greaterThan(HeapEstimates.mapBytes(written)));
        Map<String, Object> wider = NestedColumns.of(
            List.of(
                new NestedColumns.NestedColumn("answers", Map.of("type", DataType.KEYWORD, "ttl", DataType.LONG, "x", DataType.LONG), null)
            )
        ).writeTo(null);
        Map<String, Object> narrower = NestedColumns.of(
            List.of(new NestedColumns.NestedColumn("answers", Map.of("type", DataType.KEYWORD, "ttl", DataType.LONG), null))
        ).writeTo(null);
        assertThat(NestedColumns.estimatedBytes(wider), greaterThan(NestedColumns.estimatedBytes(narrower)));
        assertThat(NestedColumns.estimatedBytes(Map.of()), equalTo(0L));
    }

    /** The key sits outside the statistics prefix, which multi-file merges strip from the anchor file's metadata. */
    public void testKeyIsNotAStatisticsKey() {
        assertFalse(NestedColumns.KEY.startsWith(SourceStatisticsSerializer.STATS_KEY_PREFIX));
    }
}
