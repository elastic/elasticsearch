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
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Columnar ↔ x-content compatibility tests for the time-series metadata mappers:
 * {@link TimeSeriesIdFieldMapper} ({@code _tsid}), {@link TsidExtractingIdFieldMapper}
 * ({@code _id} derivation), {@link TimeSeriesRoutingHashFieldMapper} ({@code _ts_routing_hash}),
 * and {@link DataStreamTimestampFieldMapper} ({@code @timestamp} bounds validation).
 *
 * <p>Coverage: both {@code standard _id} ({@code index.mapping.synthetic_id=false}) and
 * {@code synthetic _id} (default for modern indices) variants, single-doc and multi-doc batches,
 * and docs with different tsids and timestamps to exercise column ordering.
 *
 * <p>Scope: these tests target the modern coordinator-tsid path only
 * ({@link org.elasticsearch.index.IndexVersions#TIME_SERIES_ROUTING_HASH_IN_ID} or later,
 * {@code index.dimensions} routing). Older indices and {@code routing_path}-based tsdb
 * fall back to the row path and are not tested here.
 *
 * <p>Index settings used in every scenario:
 * <ul>
 *   <li>{@code index.mode=time_series} — enables all four metadata mappers</li>
 *   <li>{@code index.dimensions=dim} — selects {@code ForIndexDimensions} routing, which is the
 *       path where the coordinating node pre-computes the tsid and passes it via
 *       {@code IndexRequest#tsid()}; our tests simulate this by setting a stable tsid on each
 *       {@link Doc}</li>
 *   <li>Timestamp bounds wide enough for any test timestamp</li>
 *   <li>Recovery source disabled — prevents {@code _recovery_source} fields on the x-content path
 *       that the columnar path cannot yet produce</li>
 * </ul>
 *
 * <p>Document sources contain <em>only {@code @timestamp}</em>. The {@code dim} dimension field is
 * declared in the mapping but absent from every source, keeping it out of the ESCF schema so that
 * {@link KeywordFieldMapper#supportsColumnarParse} is never consulted. This is intentional: keyword
 * fields in {@code time_series} mode use doc-values encodings that the columnar path does not yet
 * support.
 */
public class TsidExtractingIdFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    // Stable test tsids — arbitrary bytes representing pre-computed coordinator-side tsids.
    private static final BytesRef TSID_A = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final BytesRef TSID_B = new BytesRef(new byte[] { 0x10, 0x20, 0x30, 0x40, 0x50 });

    // Routing hashes and their encoded string forms (as set by the coordinating node).
    private static final int ROUTING_HASH_1 = 42;
    private static final int ROUTING_HASH_2 = 99;
    private static final String ROUTING_1 = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH_1);
    private static final String ROUTING_2 = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH_2);

    // Timestamps within the wide bounds configured below.
    private static final long TIMESTAMP_1 = 1622505600000L; // 2021-06-01T00:00:00.000Z
    private static final long TIMESTAMP_2 = 1622592000000L; // 2021-06-02T00:00:00.000Z
    private static final long TIMESTAMP_3 = 1622678400000L; // 2021-06-03T00:00:00.000Z

    // Source JSON strings — only @timestamp so the dim field never enters the ESCF schema.
    private static final String SOURCE_T1 = "{\"@timestamp\":\"2021-06-01T00:00:00.000Z\"}";
    private static final String SOURCE_T2 = "{\"@timestamp\":\"2021-06-02T00:00:00.000Z\"}";
    private static final String SOURCE_T3 = "{\"@timestamp\":\"2021-06-03T00:00:00.000Z\"}";

    /**
     * Settings for the standard {@code _id} variant ({@code index.mapping.synthetic_id=false}).
     * The id is the base-64 encoding of {@code (routing_hash LE | tsid | timestamp LE)}.
     */
    private static Settings standardIdSettings() {
        return baseSettingsBuilder().put(IndexSettings.SYNTHETIC_ID.getKey(), false).build();
    }

    /**
     * Settings for the synthetic {@code _id} variant (default for modern indices, i.e.
     * {@code index.mapping.synthetic_id=true} equivalent). The id encodes
     * {@code tsid | timestamp | routing_hash} using a dedicated codec.
     */
    private static Settings syntheticIdSettings() {
        return baseSettingsBuilder().build();
    }

    private static Settings.Builder baseSettingsBuilder() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            // index.dimensions selects ForIndexDimensions routing: coordinator pre-computes tsid.
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            // Wide bounds so any modern timestamp is valid.
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            // Disable recovery source: prevents _recovery_source on the x-content path.
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
    }

    /**
     * {@code index.mapping.synthetic_id=false}: standard base-64 {@code _id}. Tests single-doc
     * and multi-doc batches. Each Doc carries a coordinator-computed tsid and the expected
     * derived id so both paths agree on the output.
     */
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

    /**
     * Synthetic {@code _id} (default for indices at or after
     * {@link IndexVersions#TIME_SERIES_USE_SYNTHETIC_ID_DEFAULT_PROD}): the id encodes
     * {@code tsid | timestamp | routing_hash} via the TSDB codec. Tests single-doc and
     * multi-doc batches across two distinct tsids.
     */
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
