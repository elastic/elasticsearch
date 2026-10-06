/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

/**
 * Describes one keyword doc-values layout in a duel: the settings that select it, the expected {@code FieldInfo}
 * format name, and whether the ColumNAR selector guard must be asserted for it. The two layouts under test are
 * the {@link #es819} columnar-mode baseline and the {@link #columnar} ColumNAR contender.
 */
public final class KeywordIndexConfig {

    private final String indexName;
    private final String layoutLabel;
    private final Settings settings;
    private final String expectedDocValuesFormatName;
    private final boolean enforceColumnarSelectorGuard;

    private KeywordIndexConfig(
        final String indexName,
        final String layoutLabel,
        final Settings settings,
        final String expectedDocValuesFormatName,
        boolean enforceColumnarSelectorGuard
    ) {
        this.indexName = indexName;
        this.layoutLabel = layoutLabel;
        this.settings = settings;
        this.expectedDocValuesFormatName = expectedDocValuesFormatName;
        this.enforceColumnarSelectorGuard = enforceColumnarSelectorGuard;
    }

    public static KeywordIndexConfig es819(final String indexName) {
        return new KeywordIndexConfig(
            indexName,
            "es819",
            baseSettings(IndexMode.COLUMNAR).put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
                .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), false)
                .build(),
            "ES8193TSDB",
            false
        );
    }

    public static KeywordIndexConfig columnar(final String indexName) {
        return new KeywordIndexConfig(
            indexName,
            "columnar",
            baseSettings(IndexMode.COLUMNAR).put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
                .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
                .build(),
            "ColumNAR",
            true
        );
    }

    /**
     * @param newIndexName the index name for the copy
     * @return a copy of this layout under a different index name, used to create a second baseline index for
     *         baseline-versus-baseline validation. Every other part of the contract is preserved.
     */
    public KeywordIndexConfig withIndexName(final String newIndexName) {
        return new KeywordIndexConfig(newIndexName, layoutLabel, settings, expectedDocValuesFormatName, enforceColumnarSelectorGuard);
    }

    public String indexName() {
        return indexName;
    }

    public String layoutLabel() {
        return layoutLabel;
    }

    /**
     * @return the index settings. Built-in layouts use a single shard, disable the query cache, and pin
     *         {@code index.use_time_series_doc_values_format} so the randomized framework cannot flip a layout
     *         onto a different codec.
     */
    public Settings settings() {
        return settings;
    }

    /**
     * @return the doc-values format name this layout must record for the keyword field on every segment, so a
     *         duel can assert the exact codec that encoded it rather than only that it differs from ColumNAR.
     */
    public String expectedDocValuesFormatName() {
        return expectedDocValuesFormatName;
    }

    /**
     * @return whether the duel must assert {@link org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector}
     *         engages for this layout. Only a ColumNAR-backed layout requests the guard, so the feature-flag
     *         assumption and the selector assertion apply to it and not to other codecs.
     */
    public boolean enforcesColumnarSelectorGuard() {
        return enforceColumnarSelectorGuard;
    }

    private static Settings.Builder baseSettings(final IndexMode mode) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode.getName())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put("index.queries.cache.enabled", false);
    }
}
