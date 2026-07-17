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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Columnar ↔ x-content compatibility tests for the metadata mappers implemented on the
 * {@code columnar_mappers} branch. See {@link AbstractColumnarMapperCompatibilityTestCase} for
 * the test harness and the rationale for synthetic source + recovery-disabled as the base settings.
 *
 * <p>Coverage: {@link ProvidedIdFieldMapper} ({@code mode=document}/{@code columnar}),
 * {@link SourceFieldMapper} (no-op and synthetic-recovery branches),
 * {@link VersionFieldMapper}, {@link SeqNoFieldMapper} ({@code POINTS_AND_DOC_VALUES},
 * {@code DOC_VALUES_ONLY}, {@code disable_sequence_numbers}),
 * and {@link RoutingFieldMapper} ({@code doc_values=true} and {@code doc_values=false}).
 */
public class MetadataMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    /** Base settings builder: synthetic source + recovery source disabled (see class Javadoc). */
    private static Settings.Builder syntheticSourceSettingsBuilder() {
        return Settings.builder()
            // Synthetic source: no stored _source; SourceFieldMapper produces nothing on either path.
            .put("index.mapping.source.mode", "synthetic")
            // Disable recovery source: prevents _recovery_source fields on the x-content path that
            // the columnar path cannot yet produce (SourceFieldMapper.supportsColumnarParse would be false).
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
    }

    private static Settings syntheticSourceSettings() {
        return syntheticSourceSettingsBuilder().build();
    }

    /** {@code _routing doc_values=true}: routing lands in sorted doc values; no {@code _field_names} divergence. */
    public void testRoutingDocValues() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(RoutingFieldMapper.NAME).field("doc_values", true).endObject()),
            syntheticSourceSettings(),
            batch("no routing - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("with routing - single doc", 1L, doc("doc2", "my-route", 200L, "{}")),
            // Mixed batch: doc 0 has no routing (SPARSE column null entry), docs 1-2 have routing.
            batch(
                "mixed routing batch",
                2L,
                doc("batch-1", null, 300L, 1L, "{}"),
                doc("batch-2", "route-a", 301L, 2L, "{}"),
                doc("batch-3", "route-b", 302L, 3L, "{}")
            )
        );
    }

    /**
     * {@code _routing doc_values=false}: both paths produce a {@code _field_names/_routing} indexed
     * entry so that exists queries on {@code _routing} work for indices without routing doc values.
     */
    public void testRoutingWithoutDocValues() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(RoutingFieldMapper.NAME).field("doc_values", false).endObject()),
            syntheticSourceSettings(),
            batch("with routing - doc_values=false", 1L, doc("doc1", "my-route", 100L, "{}")),
            batch(
                "mixed routing batch - doc_values=false",
                2L,
                doc("batch-1", null, 300L, 1L, "{}"),
                doc("batch-2", "route-a", 301L, 2L, "{}"),
                doc("batch-3", "route-b", 302L, 3L, "{}")
            )
        );
    }

    /**
     * Synthetic recovery ({@code index.recovery.use_synthetic_source=true}): both paths write
     * {@code _recovery_source_size} as a {@code NumericDocValuesField}. Recovery source is
     * deliberately enabled here (unlike other tests) to exercise this branch of
     * {@link SourceFieldMapper#preColumnarParse}.
     */
    public void testSourceSyntheticRecovery() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.mapping.source.mode", "synthetic")
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .build();
        assertColumnarMatchesXContent(
            topMapping(b -> {}),
            settings,
            batch("empty source - single doc", 1L, doc("doc1", 100L, "{}")),
            // TODO: use realistic non-empty source once a user data mapper supports the columnar
            // path — empty source avoids content fields that only x-content produces today.
            batch("empty source batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"), doc("batch-3", 103L, "{}"))
        );
    }

    /** {@code _id mode=document}: stored {@code StringField} on both paths. */
    public void testIdDocumentMode() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(IdFieldMapper.NAME).field("mode", "document").endObject()),
            syntheticSourceSettings(),
            batch("document mode - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("document mode - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"), doc("batch-3", 103L, "{}"))
        );
    }

    /** {@code _id mode=columnar}: {@code ColumnarIdField.TYPE} (BINARY doc values + indexed, not stored) on both paths. */
    public void testIdColumnarMode() throws IOException {
        assertColumnarMatchesXContent(
            topMapping(b -> b.startObject(IdFieldMapper.NAME).field("mode", "columnar").endObject()),
            syntheticSourceSettings(),
            batch("columnar id - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("columnar id - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"))
        );
    }

    /** {@code _seq_no index_options=DOC_VALUES_ONLY}: DV-only field on both paths (no BKD point). */
    public void testSeqNoDocValuesOnly() throws IOException {
        final Settings settings = syntheticSourceSettingsBuilder().put(
            IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(),
            SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY
        ).build();
        assertColumnarMatchesXContent(
            topMapping(b -> {}),
            settings,
            batch("doc_values_only - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("doc_values_only - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"))
        );
    }

    /**
     * {@code disable_sequence_numbers=true}: exercises the {@code sequenceNumbersDisabled()} branch
     * in {@link SeqNoFieldMapper#postColumnarParse}. Produced fields are identical to
     * {@code DOC_VALUES_ONLY}; the code path differs.
     */
    public void testSeqNoDisabled() throws IOException {
        // disable_sequence_numbers requires DOC_VALUES_ONLY; set both explicitly for clarity.
        final Settings settings = syntheticSourceSettingsBuilder().put(
            IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(),
            SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY
        ).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true).build();
        assertColumnarMatchesXContent(
            topMapping(b -> {}),
            settings,
            batch("seq_no_disabled - single doc", 1L, doc("doc1", 100L, "{}")),
            batch("seq_no_disabled - batch", 2L, doc("batch-1", 101L, "{}"), doc("batch-2", 102L, "{}"))
        );
    }
}
