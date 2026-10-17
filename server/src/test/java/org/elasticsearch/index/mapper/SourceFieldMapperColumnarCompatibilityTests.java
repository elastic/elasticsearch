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
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

/**
 * Tests for the {@code columnar_stored} source blob that {@link SourceFieldMapper} builds, aimed at the blob's content type
 * ({@link SourceFieldMapper#COLUMNAR_BLOB_XCONTENT_TYPE}): the blob is encoded in SMILE rather than JSON, and every reader
 * dispatches on the {@link XContentDataHelper} type byte, so the reconstructed {@code _source} the user sees must stay JSON and
 * byte-identical in content to what a JSON blob produced. The batch-versus-row parity scenarios run through the
 * {@link AbstractColumnarMapperCompatibilityTestCase} harness, which compares the blob bytes of both paths; the round-trip
 * scenarios read the source back through the regular synthetic source loader.
 */
public class SourceFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
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

    /** The intent of the change, pinned: the blob must not silently fall back to JSON. */
    public void testBlobContentTypeIsSmile() {
        assertEquals(XContentType.SMILE, SourceFieldMapper.COLUMNAR_BLOB_XCONTENT_TYPE);
    }

    /**
     * The batch and row paths encode through the same builder factory, so their SMILE blobs must stay byte-identical across value
     * kinds whose SMILE encodings differ from JSON the most: strings (no escaping), longs and dates (binary numbers), repeated
     * field names in objects (back-references), and the {@code ignore_above} fallback whose JSON-encoded sub-value is transcoded
     * into the SMILE blob.
     */
    public void testBatchAndRowPathsProduceIdenticalSmileBlobs() throws IOException {
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch(
                "mixed kinds",
                1L,
                doc("d1", 1L, "{\"kw\":\"quote \\\" and backslash \\\\ and emoji \\u00e9\",\"num\":123456789012345}"),
                doc("d2", 2L, "{\"date\":\"2026-10-17T12:00:00Z\",\"obj\":{\"inner_kw\":\"nested\"}}"),
                doc("d3", 3L, "{\"capped\":\"waytoolongforthelimit\",\"kw\":[\"a\",\"b\"]}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"text\":\"full text value\",\"num\":-7}")
            )
        );
    }

    /**
     * End-to-end round trip through the synthetic source loader: the stored blob is SMILE, but reconstruction copies it into the
     * caller's JSON builder, so the {@code _source} the user reads back must be the same JSON a JSON blob produced.
     */
    public void testSourceReadBackFromSmileBlobIsJson() throws IOException {
        final DocumentMapper mapper = createMapperService(columnarSettings(), testMapping()).documentMapper();
        final String synthetic = syntheticSource(mapper, b -> {
            b.field("kw", "escaped \" value");
            b.field("num", 123456789012345L);
            b.field("date", "2026-10-17T12:00:00.000Z");
            b.startObject("obj").field("inner_kw", "nested").endObject();
        });
        // Object sub-fields come back under their dotted path: that is how the columnar_stored blob reconstructs objects,
        // independent of the blob's content type.
        assertEquals("""
            {"date":"2026-10-17T12:00:00.000Z","kw":"escaped \\" value","num":123456789012345,"obj.inner_kw":"nested"}""", synthetic);
    }

    /**
     * The {@code ignore_above} fallback value travels as a JSON-encoded {@code _ignored_source} sub-entry that is transcoded into
     * the SMILE blob at index time and back to JSON on read: the original value must survive both transcodings.
     */
    public void testIgnoreAboveFallbackSurvivesSmileRoundTrip() throws IOException {
        final DocumentMapper mapper = createMapperService(columnarSettings(), testMapping()).documentMapper();
        final String synthetic = syntheticSource(mapper, b -> { b.field("capped", "waytoolongforthelimit"); });
        assertEquals("""
            {"capped":"waytoolongforthelimit"}""", synthetic);
    }
}
