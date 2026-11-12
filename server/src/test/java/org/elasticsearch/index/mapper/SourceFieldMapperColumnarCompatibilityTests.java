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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Parity tests for the {@code columnar_stored} source blob that {@link SourceFieldMapper#postColumnarParse} builds, aimed at the
 * reused blob buffer: the batch path streams every document's JSON into one buffer behind a precomputed
 * {@code SingularIgnoredSourceEncoding} header and {@code XContentDataHelper} type byte, where the row path encodes each document
 * through fresh builders and intermediate copies. These scenarios assert — through the
 * {@link AbstractColumnarMapperCompatibilityTestCase} harness, which compares the blob bytes of both paths — that the prefix bytes
 * and the buffer reuse reproduce the row path's encoding exactly.
 */
public class SourceFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
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
