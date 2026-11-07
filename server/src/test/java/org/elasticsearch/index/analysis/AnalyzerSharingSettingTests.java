/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies the {@link AnalysisRegistry#SHARED_ANALYZERS_ENABLED_SETTING} escape hatch end to end on a real
 * node: {@code NodeConstruction} must wire the registry to cluster settings, so a cluster-settings update —
 * not a direct call on the registry, as the unit tests use — stops and resumes analyzer sharing.
 */
public class AnalyzerSharingSettingTests extends ESSingleNodeTestCase {

    public void testEscapeHatchWiredToClusterSettings() {
        assumeTrue("analyzer sharing feature flag must be enabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());
        AnalysisRegistry registry = getInstanceFromNode(AnalysisRegistry.class);
        Settings recipe = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase")
            .build();

        createIndex("a", recipe);
        createIndex("b", recipe);
        long hitsWhileShared = registry.cacheHits();
        assertThat(hitsWhileShared, greaterThan(0L));

        try {
            updateClusterSettings(Settings.builder().put(AnalysisRegistry.SHARED_ANALYZERS_ENABLED_SETTING.getKey(), false).build());
            createIndex("c", recipe);
            assertEquals(hitsWhileShared, registry.cacheHits());
        } finally {
            updateClusterSettings(Settings.builder().putNull(AnalysisRegistry.SHARED_ANALYZERS_ENABLED_SETTING.getKey()).build());
        }

        createIndex("d", recipe);
        assertThat(registry.cacheHits(), greaterThan(hitsWhileShared));
    }
}
