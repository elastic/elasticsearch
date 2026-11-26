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
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class NestedColumnsTests extends ESTestCase {

    private static final NestedColumns COLUMNS = NestedColumns.of(
        List.of(
            new NestedColumns.NestedColumn("answers", Map.of("type", DataType.KEYWORD, "ttl", DataType.LONG), null),
            new NestedColumns.NestedColumn("hops", Map.of("host", DataType.KEYWORD), "[hops.links] cannot be read yet")
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

    /** The key sits outside the statistics prefix, which multi-file merges strip from the anchor file's metadata. */
    public void testKeyIsNotAStatisticsKey() {
        assertFalse(NestedColumns.KEY.startsWith(SourceStatisticsSerializer.STATS_KEY_PREFIX));
    }
}
