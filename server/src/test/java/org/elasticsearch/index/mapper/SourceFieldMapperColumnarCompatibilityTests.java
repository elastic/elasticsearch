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
 * Parity tests for the {@code columnar_stored} source blob that {@link SourceFieldMapper#postColumnarParse} builds, aimed at the
 * column-to-slot binding of {@link ColumnarSourceWriter.BatchWriter}: the batch path binds each column to its leaf-reader slot once
 * per batch and streams values straight into the slots, instead of assembling a per-document field list and reading it back. These
 * scenarios assert — through the {@link AbstractColumnarMapperCompatibilityTestCase} harness, which compares the blob bytes of both
 * paths — that the bound sinks reproduce exactly what per-document field repopulation produces, in particular the per-document slot
 * clearing, the first-value-wins rule of single-valued slots, and the add-every-value rule of multi-valued ones.
 */
public class SourceFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /**
     * A mapping that declares more fields than any test document carries: the populated fields cover the slot kinds the batch path
     * feeds (keyword, long, date, text with a keyword multi-field, object sub-fields, ignore_above fallback), and the
     * {@code unused_*} fields register slots that no column of the batch binds to, which must stay absent from every document.
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

    /**
     * Documents that each carry a different sparse subset of the mapped fields, including one carrying nothing at all: a column's
     * cursor only reports the documents that hold a value, so a slot filled for one document must come back cleared for the next —
     * a stale slot would leak a value into a document that never had the field.
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
     * are merged by the object-level loaders rather than the field's own: the scenario checks that the batch writer feeds those
     * binary columns into their slots just as per-document repopulation would.
     */
    public void testIgnoreAboveFallbackSurvivesSlotBinding() throws IOException {
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
     * Batches whose column sets differ run back-to-back against the same mapper service: each batch binds its own columns against
     * the thread's cached leaf reader, so bindings from a previous batch must neither linger nor be required for the next one.
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
}
