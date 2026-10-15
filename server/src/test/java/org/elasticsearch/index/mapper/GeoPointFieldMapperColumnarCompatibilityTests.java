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

import java.io.IOException;

/**
 * Parity tests for {@link GeoPointFieldMapper#mapColumnGroupBatch} against the row path.
 * <p>
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives the group mapper automatically
 * via {@link org.elasticsearch.index.mapper.ColumnGroupResolver}: for each object-form geo_point
 * leaf, it recognises the {@code loc.lat} / {@code loc.lon} paths as owned by the {@code geo_point}
 * group mapper and calls {@link GeoPointFieldMapper#mapColumnGroupBatch} with the paired columns.
 * <p>
 * Fallback scenarios — shapes that cause {@code mapColumnGroupBatch} or {@code doMapColumnBatch} to
 * throw {@link UnsupportedOperationException} — are covered by the unit-level tests in
 * {@link GeoPointFieldMapperTests} and the parse-level tests in
 * {@link org.elasticsearch.action.bulk.ShardBatchMapperParseTests}.
 */
public class GeoPointFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "loc";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /** Object-form geo_point: the ESCF encoder produces {@code loc.lat} and {@code loc.lon} sub-leaves. */
    public void testObjectFormSingleDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch("object form single doc", 1L, doc("d1", 1L, "{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}"))
        );
    }

    /** Multiple documents in one batch: verifies dense-column handling. */
    public void testObjectFormMultipleDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch(
                "object form multiple docs",
                1L,
                doc("d1", 1L, "{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}"),
                doc("d2", 2L, "{\"loc\":{\"lat\":48.9,\"lon\":2.3}}"),
                doc("d3", 3L, "{\"loc\":{\"lat\":40.7,\"lon\":-74.0}}")
            )
        );
    }

    /** Some documents have the field, others do not: verifies sparse-column handling. */
    public void testObjectFormSparseDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch(
                "sparse docs",
                1L,
                doc("d1", 1L, "{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"loc\":{\"lat\":48.9,\"lon\":2.3}}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"loc\":{\"lat\":35.7,\"lon\":139.7}}")
            )
        );
    }

    /** Boundary coordinates: ±90 lat, ±180 lon, and (0, 0). */
    public void testBoundaryCoordinates() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch(
                "boundary coordinates",
                1L,
                doc("d1", 1L, "{\"loc\":{\"lat\":90.0,\"lon\":180.0}}"),
                doc("d2", 2L, "{\"loc\":{\"lat\":-90.0,\"lon\":-180.0}}"),
                doc("d3", 3L, "{\"loc\":{\"lat\":0.0,\"lon\":0.0}}")
            )
        );
    }

    /**
     * Coordinates given as JSON integers (LONG kind in the ESCF column). The mapper accepts both
     * LONG and DOUBLE coordinate columns.
     */
    public void testIntegerCoordinatesLongKind() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch(
                "integer coordinates (LONG kind)",
                1L,
                doc("d1", 1L, "{\"loc\":{\"lat\":51,\"lon\":-1}}"),
                doc("d2", 2L, "{\"loc\":{\"lat\":48,\"lon\":2}}")
            )
        );
    }

    /**
     * {@code ignore_malformed: true} with valid coordinates — the malformed path is not exercised,
     * so the columnar fast path produces the same output as the row path.
     */
    public void testIgnoreMalformedWithValidData() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch(
                "ignore_malformed with valid data",
                1L,
                doc("d1", 1L, "{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"loc\":{\"lat\":35.7,\"lon\":139.7}}")
            )
        );
    }

    /**
     * A geo_point at its own path (JSON null) falls through to {@link GeoPointFieldMapper#doMapColumnBatch},
     * which accepts an all-null column and emits nothing. The x-content path also emits nothing for a null
     * geo_point without a {@code null_value}.
     */
    public void testNullAtOwnPath() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch("null at own path", 1L, doc("d1", 1L, "{\"loc\":null}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"loc\":null}"))
        );
    }

    /**
     * A batch mixing a null own-path leaf with object-form sub-leaves: the harness routes the null
     * to {@code doMapColumnBatch} (leaf mapper) and the sub-leaves to {@code mapColumnGroupBatch}
     * (group mapper).
     */
    public void testMixedNullAndObjectForm() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()),
            columnarSettings(),
            batch(
                "mixed null and object form",
                1L,
                doc("d1", 1L, "{\"loc\":null}"),
                doc("d2", 2L, "{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    /**
     * A string-form geo_point at its own path causes the columnar path to fall back via
     * {@link GeoPointFieldMapper#doMapColumnBatch}. This verifies the
     * {@link UnsupportedOperationException} is thrown for non-null own-path values.
     */
    public void testOwnPathStringFallsBack() throws IOException {
        final MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"loc\":\"51.5,-0.1\"}"));
    }

    /**
     * A non-columnar (standard) index mode disables the fast path: {@link FieldMapper#supportsColumnarParse}
     * returns {@code false} outside strict-columnar and TSDB modes.
     */
    public void testNonColumnarModeFallsBack() throws IOException {
        final MapperService ms = createMapperService(mapping(b -> b.startObject(FIELD).field("type", "geo_point").endObject()));
        final var mapper = (GeoPointFieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertFalse(
            "geo_point must not take the columnar path outside strict-columnar and TSDB index modes",
            mapper.supportsColumnarParse(ms.getIndexSettings())
        );
    }
}
