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
import org.elasticsearch.common.io.stream.MockBytesRefRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasables;
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

import static org.hamcrest.Matchers.greaterThan;

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

    public void testSerializesOnItsRecyclerAndReleasesOnClose() throws IOException {
        MockBytesRefRecycler recycler = new MockBytesRefRecycler();
        List<String> docs = randomDocs();
        int partitions = randomIntBetween(1, 4);
        List<List<String>> docsByPartition = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            docsByPartition.add(new ArrayList<>());
        }
        EscfBatch[] batches = new EscfBatch[partitions];
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (String doc : docs) {
                int partition = randomIntBetween(0, partitions - 1);
                encoder.parseToScratch(new BytesArray(doc), XContentType.JSON);
                encoder.commitScratchTo(partition);
                docsByPartition.get(partition).add(doc);
            }
            for (int p = 0; p < partitions; p++) {
                if (encoder.hasPartition(p)) {
                    batches[p] = encoder.buildPartition(p);
                }
            }
        }
        try {
            for (int p = 0; p < partitions; p++) {
                EscfBatch batch = batches[p];
                if (batch == null) {
                    continue;
                }
                List<String> partitionDocs = docsByPartition.get(p);
                int pagesBeforeSerialize = recycler.activePageCount();
                BytesReference bytes = batch.data();
                assertThat(
                    "serialize draws pages from the batch's recycler",
                    recycler.activePageCount(),
                    greaterThan(pagesBeforeSerialize)
                );
                assertSame(bytes, batch.data());
                assertSame(bytes, batch.slice(0, batch.docCount()).data());
                try (EscfBatch parsed = EscfBatch.parse(bytes, () -> {})) {
                    for (int i = 0; i < batch.docCount(); i++) {
                        assertEquals("row " + i + " of " + partitionDocs, reconstruct(batch, i), reconstruct(parsed, i));
                    }
                }

                int from = randomIntBetween(0, batch.docCount() - 1);
                int to = randomIntBetween(from + 1, batch.docCount());
                int pagesBeforeSlice = recycler.activePageCount();
                try (EscfBatch reparsed = EscfBatch.parse(batch.slice(from, to).data(), () -> {})) {
                    assertEquals("slices take no pages of their own", pagesBeforeSlice, recycler.activePageCount());
                    for (int i = 0; i < to - from; i++) {
                        assertEquals(
                            "row " + (from + i) + " of slice [" + from + ", " + to + ") of " + partitionDocs,
                            reconstruct(batch, from + i),
                            reconstruct(reparsed, i)
                        );
                    }
                }
            }
        } finally {
            Releasables.close(batches);
        }
        assertEquals("closing the batches releases every page they took", 0, recycler.activePageCount());
    }

    private static List<String> randomDocs() {
        String[] optionalFields = { "d", "s", "b", "n", "arr_l", "arr_s", "arr_m", "obj", "u" };
        List<String> docs = new ArrayList<>();
        for (int i = randomIntBetween(1, 200); i > 0; i--) {
            StringBuilder doc = new StringBuilder("{\"l\":").append(randomLong());
            for (String field : optionalFields) {
                if (randomBoolean()) {
                    doc.append(",\"").append(field).append("\":").append(randomValue(field));
                }
            }
            docs.add(doc.append('}').toString());
        }
        return docs;
    }

    private static String randomValue(String field) {
        return switch (field) {
            case "d" -> Double.toString(randomDouble());
            case "s" -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\"";
            case "b" -> Boolean.toString(randomBoolean());
            case "n" -> "null";
            case "arr_l" -> randomList(1, 4, () -> Long.toString(randomLong())).toString();
            case "arr_s" -> randomList(1, 4, () -> "\"" + randomAlphaOfLength(3) + "\"").toString();
            case "arr_m" -> "[" + randomLong() + ",\"" + randomAlphaOfLength(3) + "\"]";
            case "obj" -> "{\"k\":" + randomInt() + "}";
            case "u" -> randomBoolean() ? Long.toString(randomLong()) : "\"" + randomAlphaOfLength(4) + "\"";
            default -> throw new AssertionError("unknown field [" + field + "]");
        };
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
