/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the behaviour {@link RowSource} owns on top of the {@link DocumentSource} contract, using rows
 * produced by the production batch encoder ({@link EscfEncoder}). Agreement with {@link BytesSource}
 * over the same documents lives in {@link DocumentSourceCompatibilityTests}.
 */
public class RowSourceTests extends ESTestCase {

    /**
     * Starts several first readers of {@link RowSource#originalBytes()} at once and expects all of
     * them, and a later reader, to receive the same serialization, which the monitor around the lazy
     * materialization guarantees.
     */
    public void testConcurrentReadersShareOneMaterialization() throws IOException {
        final BytesReference source = new BytesArray("""
            {"host": "server-1", "metrics": {"cpu": 0.25, "mem": 2048}}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            final RowSource rowSource = new RowSource(schemaTree, batch.row(0), XContentType.JSON);

            final int readers = between(4, 16);
            final BytesReference[] seen = new BytesReference[readers];
            startInParallel(readers, i -> seen[i] = rowSource.originalBytes());

            for (BytesReference bytes : seen) {
                assertSame(seen[0], bytes);
            }
            assertSame(seen[0], rowSource.originalBytes());
            assertThat(toMap(seen[0]), equalTo(toMap(source)));
        }
    }

    public void testRejectsNullComponents() throws IOException {
        try (EscfBatch batch = EscfEncoder.encode(List.of(new BytesArray("""
            {"host": "server-1"}""")), XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            final SourceRow row = batch.row(0);

            expectThrows(NullPointerException.class, () -> new RowSource(null, row, XContentType.JSON));
            expectThrows(NullPointerException.class, () -> new RowSource(schemaTree, null, XContentType.JSON));
            expectThrows(NullPointerException.class, () -> new RowSource(schemaTree, row, null));
        }
    }

    private static Map<String, Object> toMap(BytesReference bytes) {
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }
}
