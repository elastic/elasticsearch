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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serialization round-trip tests for {@link EscfBatch}: an in-memory batch's bytes, when parsed back
 * via the byte constructor, reconstruct identical sources, and {@link EscfBatch#slice} reconstructs
 * the corresponding sub-range.
 */
public class EscfBatchSerializationTests extends ESTestCase {

    private static final String[] DOCS = {
        "{\"i\":1,\"s\":\"alice\",\"arr\":[1,2,3]}",
        "{\"i\":2,\"s\":\"bob\",\"tags\":[\"x\",\"y\"]}",
        "{\"i\":3,\"d\":2.5,\"nested\":{\"k\":7}}",
        "{\"i\":4,\"mixed\":[1,\"two\"],\"flag\":true}",
        "{\"s\":\"eve\",\"a\":null}" };

    public void testSerializeDeserializeMatches() throws IOException {
        try (EscfBatch inMemory = encode(DOCS)) {
            BytesReference bytes = inMemory.data();
            try (EscfBatch parsed = EscfBatch.parse(bytes, () -> {})) {
                assertEquals(inMemory.docCount(), parsed.docCount());
                assertEquals(inMemory.columnCount(), parsed.columnCount());
                for (int i = 0; i < DOCS.length; i++) {
                    assertEquals("row " + i, reconstruct(inMemory, i), reconstruct(parsed, i));
                    assertEquals("row " + i + " vs source", asMap(DOCS[i]), reconstruct(parsed, i));
                }
                // Re-serializing the parsed batch yields identical bytes.
                assertEquals(bytes, parsed.data());
            }
        }
    }

    public void testSlice() throws IOException {
        try (EscfBatch batch = encode(DOCS)) {
            SourceBatch sliced = batch.slice(1, 4);
            assertEquals(3, sliced.docCount());
            for (int i = 0; i < 3; i++) {
                assertEquals("sliced row " + i, asMap(DOCS[i + 1]), reconstruct(sliced, i));
            }
            // A sliced batch also serializes and round-trips.
            try (EscfBatch reparsed = EscfBatch.parse(sliced.data(), () -> {})) {
                for (int i = 0; i < 3; i++) {
                    assertEquals("reparsed sliced row " + i, asMap(DOCS[i + 1]), reconstruct(reparsed, i));
                }
            }
        }
    }

    /** Stored spellings of floating-point values in a dense DOUBLE column travel through the wire format (#163555). */
    public void testLexicalFormsSurviveSerialization() throws IOException {
        String[] docs = { "{\"d\":-1.90,\"s\":\"a\"}", "{\"d\":3.14}", "{\"d\":1e3,\"s\":\"b\"}" };
        try (EscfBatch inMemory = encode(docs)) {
            assertEquals(EscfColumnKind.DOUBLE, columnKind(inMemory, "d"));
            BytesReference bytes = inMemory.data();
            try (EscfBatch parsed = EscfBatch.parse(bytes, () -> {})) {
                assertEquals(List.of("-1.90", "3.14", "1e3"), spellings(parsed, "d"));
                for (int i = 0; i < docs.length; i++) {
                    assertEquals("row " + i + " vs source", asMap(docs[i]), reconstruct(parsed, i));
                }
                assertEquals(bytes, parsed.data());
            }
            // a slice re-serializes the sidecar at its new base
            try (EscfBatch reparsed = EscfBatch.parse(inMemory.slice(1, 3).data(), () -> {})) {
                assertEquals(List.of("3.14", "1e3"), spellings(reparsed, "d"));
            }
        }
    }

    /** The utf8 renderings of every value of the column at {@code path}, in cursor order; {@code null} for JSON null. */
    private static List<String> spellings(EscfBatch batch, String path) {
        List<String> out = new ArrayList<>();
        var cursor = EscfColumnTransforms.utf8Cursor(column(batch, path), true);
        while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            out.add(cursor.value() == null ? null : cursor.value().utf8ToString());
        }
        return out;
    }

    private static byte columnKind(EscfBatch batch, String path) {
        return column(batch, path).kind();
    }

    private static EscfColumn column(EscfBatch batch, String path) {
        for (int i = 0; i < batch.schema().leafCount(); i++) {
            if (path.equals(batch.schema().getFullPath(i))) {
                return batch.column(i);
            }
        }
        throw new AssertionError("Column '" + path + "' not found in batch schema");
    }

    private static EscfBatch encode(String[] docs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(docs.length);
        for (String doc : docs) {
            sources.add(new BytesArray(doc));
        }
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    private static Map<String, Object> reconstruct(SourceBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }
}
