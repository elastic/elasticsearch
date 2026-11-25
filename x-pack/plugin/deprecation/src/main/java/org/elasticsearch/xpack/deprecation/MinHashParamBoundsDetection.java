/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.MinHashIndexParamBounds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Finds {@code min_hash} filters in index or template settings that exceed {@link MinHashIndexParamBounds}.
 */
final class MinHashParamBoundsDetection {

    static final String DEPRECATION_MESSAGE = "min_hash token filter parameters exceed the limits for new indices";
    static final String DEPRECATION_HELP_URL = "https://www.elastic.co/docs/reference/text-analysis/analysis-minhash-tokenfilter";

    private MinHashParamBoundsDetection() {}

    /**
     * @return a description for each {@code min_hash} filter in the settings that exceeds the bounds
     */
    static List<String> getViolations(@Nullable Settings settings) {
        if (settings == null || settings.isEmpty()) {
            return List.of();
        }

        List<String> violations = new ArrayList<>();
        Map<String, Settings> filters = settings.getGroups(AnalysisRegistry.INDEX_ANALYSIS_FILTER);
        for (Map.Entry<String, Settings> filter : filters.entrySet()) {
            Settings filterSettings = filter.getValue();
            if ("min_hash".equals(filterSettings.get("type")) == false) {
                continue;
            }

            String violation;
            try {
                violation = MinHashIndexParamBounds.violation(filterSettings);
            } catch (SettingsException e) {
                // The filter's settings cannot be parsed. We should never hit this scenario, as such a filter cannot be added in the first
                // place. If we somehow do, the index will fail to open downstream.
                continue;
            }

            if (violation != null) {
                violations.add("[" + filter.getKey() + "]: " + violation);
            }
        }
        return violations;
    }
}
