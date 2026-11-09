/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.ukrainian;

import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalysisUkrainianFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisUkrainianFactoryTests() {
        super(new AnalysisUkrainianPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("morfologik", Void.class); // on the classpath via lucene-analysis-morfologik, not exposed
        return filters;
    }

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        return Map.of(
            "ukrainian",
            settings().affects("stopwords", List.of("foo")).affects("stopwords_case", "true").affects("stem_exclusion", List.of("foo"))
        );
    }
}
