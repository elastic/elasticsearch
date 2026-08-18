/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Columnar ↔ x-content compatibility tests for the time-series metadata mappers
 * ({@code _tsid}, {@code _id}, {@code _ts_routing_hash}, {@code @timestamp}).
 *
 * <p>Covers standard and synthetic {@code _id} variants, single-doc and multi-doc batches.
 * Targets the modern {@code index.dimensions} coordinator-tsid path only; {@code routing_path}
 * and older indices fall back to the row path and are not tested here.
 *
 * <p>Document sources contain only {@code @timestamp}; the {@code dim} dimension is declared in
 * the mapping but absent from sources to keep keyword fields out of the ESCF schema (keyword
 * doc-values encodings are not yet supported on the columnar path).
 */
public class TsidExtractingIdFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final BytesRef TSID_A = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final BytesRef TSID_B = new BytesRef(new byte[] { 0x10, 0x20, 0x30, 0x40, 0x50 });

    private static final int ROUTING_HASH_1 = 42;
    private static final int ROUTING_HASH_2 = 99;
    private static final String ROUTING_1 = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH_1);
    private static final String ROUTING_2 = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH_2);

    private static final long TIMESTAMP_1 = 1622505600000L; // 2021-06-01T00:00:00.000Z
    private static final long TIMESTAMP_2 = 1622592000000L; // 2021-06-02T00:00:00.000Z
    private static final long TIMESTAMP_3 = 1622678400000L; // 2021-06-03T00:00:00.000Z

    private static final String SOURCE_T1 = "{\"@timestamp\":\"2021-06-01T00:00:00.000Z\"}";
    private static final String SOURCE_T2 = "{\"@timestamp\":\"2021-06-02T00:00:00.000Z\"}";
    private static final String SOURCE_T3 = "{\"@timestamp\":\"2021-06-03T00:00:00.000Z\"}";

    private static Settings standardIdSettings() {
        return baseSettingsBuilder().put(IndexSettings.SYNTHETIC_ID.getKey(), false).build();
    }

    private static Settings syntheticIdSettings() {
        return baseSettingsBuilder().build();
    }

    private static Settings.Builder baseSettingsBuilder() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
    }

    public void testStandardId() throws IOException {
        final String id1 = TsidExtractingIdFieldMapper.createId(ROUTING_HASH_1, TSID_A, TIMESTAMP_1);
        final String id2 = TsidExtractingIdFieldMapper.createId(ROUTING_HASH_2, TSID_B, TIMESTAMP_2);
        final String id3 = TsidExtractingIdFieldMapper.createId(ROUTING_HASH_1, TSID_A, TIMESTAMP_3);

        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
        }),
            standardIdSettings(),
            batch("standard-id single doc", 1L, doc(id1, ROUTING_1, TSID_A, 100L, SOURCE_T1)),
            batch(
                "standard-id multi-doc",
                2L,
                doc(id1, ROUTING_1, TSID_A, 101L, SOURCE_T1),
                doc(id2, ROUTING_2, TSID_B, 102L, SOURCE_T2),
                // same tsid, different timestamp → different id
                doc(id3, ROUTING_1, TSID_A, 103L, SOURCE_T3)
            )
        );
    }

    public void testSyntheticId() throws IOException {
        final String id1 = TsidExtractingIdFieldMapper.createSyntheticId(TSID_A, TIMESTAMP_1, ROUTING_HASH_1);
        final String id2 = TsidExtractingIdFieldMapper.createSyntheticId(TSID_B, TIMESTAMP_2, ROUTING_HASH_2);
        final String id3 = TsidExtractingIdFieldMapper.createSyntheticId(TSID_A, TIMESTAMP_3, ROUTING_HASH_1);

        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
        }),
            syntheticIdSettings(),
            batch("synthetic-id single doc", 1L, doc(id1, ROUTING_1, TSID_A, 100L, SOURCE_T1)),
            batch(
                "synthetic-id multi-doc",
                2L,
                doc(id1, ROUTING_1, TSID_A, 101L, SOURCE_T1),
                doc(id2, ROUTING_2, TSID_B, 102L, SOURCE_T2),
                // same tsid, different timestamp → different synthetic id
                doc(id3, ROUTING_1, TSID_A, 103L, SOURCE_T3)
            )
        );
    }
}
