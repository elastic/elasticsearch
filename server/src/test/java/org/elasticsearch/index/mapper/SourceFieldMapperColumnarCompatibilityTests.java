/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Parity tests for the {@code columnar_stored} source blob that {@link SourceFieldMapper#postColumnarParse} builds, aimed at two
 * batch-path optimizations. The loader pruning in {@link ColumnarSourceWriter} only builds synthetic field loaders for the fields that
 * have a column in the batch, so those scenarios use a mapping that is much wider than any document. The reused blob buffer streams
 * every document's JSON into one buffer behind a precomputed {@code SingularIgnoredSourceEncoding} header and
 * {@code XContentDataHelper} type byte, where the row path encodes each document through fresh builders and intermediate copies.
 * All scenarios assert — through the {@link AbstractColumnarMapperCompatibilityTestCase} harness, which compares the blob bytes of
 * both paths — that neither optimization changes the reconstructed source or the blob encoding.
 */
public class SourceFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /**
     * A mapping that declares far more fields than any test document carries: the populated fields cover the loader kinds the batch
     * path supports (keyword, long, date, text with a keyword multi-field, object sub-fields, ignore_above fallback), and the
     * {@code unused_*} fields — including a whole unused object — exist only so that the batch path has loaders to prune.
     */
    private static XContentBuilder wideMapping() throws IOException {
        return mapping(b -> {
            b.startObject("present_kw").field("type", "keyword").endObject();
            b.startObject("present_long").field("type", "long").endObject();
            b.startObject("present_date").field("type", "date").endObject();
            b.startObject("present_text");
            {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("keyword").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("capped").field("type", "keyword").field("ignore_above", 8).endObject();
            b.startObject("obj");
            {
                b.startObject("properties");
                b.startObject("inner_kw").field("type", "keyword").endObject();
                b.startObject("unused_inner_long").field("type", "long").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("unused_kw").field("type", "keyword").endObject();
            b.startObject("unused_long").field("type", "long").endObject();
            b.startObject("unused_date").field("type", "date").endObject();
            b.startObject("unused_text");
            {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("keyword").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("unused_obj");
            {
                b.startObject("properties");
                b.startObject("inner_kw").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    private static XContentBuilder testMapping() throws IOException {
        return mapping(b -> {
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("num").field("type", "long").endObject();
            b.startObject("date").field("type", "date").endObject();
            b.startObject("text");
            {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("keyword").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("capped").field("type", "keyword").field("ignore_above", 8).endObject();
            b.startObject("obj");
            {
                b.startObject("properties");
                b.startObject("inner_kw").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    /**
     * Documents that each carry a different sparse subset of the mapped fields, including one carrying nothing at all: every
     * pruned-away loader must stay invisible in the blob, and a field absent from one document but present in another must still
     * round-trip (pruning is per batch, not per document).
     */
    public void testSparseDocumentsUnderWideMapping() throws IOException {
        assertColumnarMatchesXContent(
            wideMapping(),
            columnarSettings(),
            batch(
                "sparse subsets",
                1L,
                doc("d1", 1L, "{\"present_kw\":\"alpha\",\"present_long\":42}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"present_date\":\"2026-10-17T12:00:00Z\",\"obj\":{\"inner_kw\":\"nested-value\"}}"),
                doc("d4", 4L, "{\"present_text\":\"some full text value\",\"present_kw\":[\"a\",\"b\"]}"),
                doc("d5", 5L, "{\"present_kw\":\"omega\",\"present_long\":7,\"present_date\":\"2026-01-01\"}")
            )
        );
    }

    /**
     * An over-length {@code ignore_above} value takes the {@code _ignored_source}/{@code ._original} fallback route, whose entries
     * are merged by the object-level loaders rather than the field's own: the scenario checks that this route survives pruning
     * unchanged while most of the mapping's loaders are pruned away.
     */
    public void testIgnoreAboveFallbackSurvivesPruning() throws IOException {
        assertColumnarMatchesXContent(
            wideMapping(),
            columnarSettings(),
            batch(
                "ignore_above fallback",
                1L,
                doc("d1", 1L, "{\"capped\":\"waytoolongforthelimit\"}"),
                doc("d2", 2L, "{\"capped\":\"short\",\"present_kw\":\"beta\"}"),
                doc("d3", 3L, "{\"capped\":[\"tiny\",\"anotherwaytoolongvalue\"]}")
            )
        );
    }

    /**
     * Batches whose column sets differ run back-to-back against the same mapper service, so the per-thread loader cache in
     * {@link ColumnarSourceWriter} must hand each batch a loader pruned to its own columns rather than one left over from the
     * previous batch.
     */
    public void testAlternatingColumnSetsAcrossBatches() throws IOException {
        assertColumnarMatchesXContent(
            wideMapping(),
            columnarSettings(),
            batch("only keyword", 1L, doc("d1", 1L, "{\"present_kw\":\"one\"}"), doc("d2", 2L, "{\"present_kw\":\"two\"}")),
            batch("only long and object", 1L, doc("d3", 3L, "{\"present_long\":1,\"obj\":{\"inner_kw\":\"x\"}}")),
            batch("only keyword again", 1L, doc("d4", 4L, "{\"present_kw\":\"three\"}")),
            batch(
                "wider mix",
                1L,
                doc("d5", 5L, "{\"present_kw\":\"four\",\"present_long\":9,\"present_date\":\"2026-05-05\",\"present_text\":\"t\"}")
            )
        );
    }

    /**
     * Documents ordered from large to small, ending in an empty one: the batch path writes each blob over the previous one in the
     * same buffer, so any stale byte past the reset point — the bug class a reused, in-place-exposed buffer invites — would surface
     * as a longer blob than the row path produces.
     */
    public void testShrinkingDocumentsReuseTheBlobBuffer() throws IOException {
        final String big = "x".repeat(500);
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch(
                "shrinking docs",
                1L,
                doc("d1", 1L, "{\"kw\":\"" + big + "\",\"text\":\"" + big + " " + big + "\",\"num\":123456789012345}"),
                doc("d2", 2L, "{\"kw\":[\"alpha\",\"beta\"],\"obj\":{\"inner_kw\":\"inner\"}}"),
                doc("d3", 3L, "{\"num\":7}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    /**
     * Growing and shrinking across consecutive documents, with values whose encoded forms differ in kind (dates, multi-values,
     * the {@code ignore_above} fallback that embeds its own {@code _ignored_source} entry inside the blob): every document must
     * come out byte-identical to the row path's fresh-builder encoding regardless of what the previous document left in the buffer.
     */
    public void testAlternatingDocumentSizes() throws IOException {
        final String big = "y".repeat(300);
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch(
                "alternating sizes",
                1L,
                doc("d1", 1L, "{\"num\":1}"),
                doc("d2", 2L, "{\"kw\":\"" + big + "\",\"date\":\"2026-10-17T12:00:00Z\"}"),
                doc("d3", 3L, "{\"capped\":\"waytoolongforthelimit\"}"),
                doc("d4", 4L, "{\"text\":\"" + big + "\",\"kw\":[\"a\",\"b\",\"c\"],\"capped\":\"short\"}"),
                doc("d5", 5L, "{\"date\":\"2026-01-01\"}")
            )
        );
    }

    /**
     * Several batches against the same mapper service: the buffer and the prefix are per batch, so each batch must stand on its
     * own, including a batch whose first document is empty (nothing but the prefix in the buffer).
     */
    public void testBufferIsPerBatch() throws IOException {
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch("first", 1L, doc("d1", 1L, "{\"kw\":\"one\",\"num\":1}")),
            batch("empty leading doc", 1L, doc("d2", 2L, "{}"), doc("d3", 3L, "{\"kw\":\"two\"}")),
            batch("wider", 1L, doc("d4", 4L, "{\"kw\":\"three\",\"date\":\"2026-05-05\",\"obj\":{\"inner_kw\":\"x\"}}"))
        );
    }
}
