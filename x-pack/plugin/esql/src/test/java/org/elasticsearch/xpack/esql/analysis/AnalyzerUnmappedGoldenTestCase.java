/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Shared analysis-stage setup and setting prefixes for golden tests of unmapped field behavior.
 *
 * Concrete tests use {@link #load(String)} or {@link #nullify(String)} around their complete pipeline and declare version coverage
 * directly on each builder chain.
 */
abstract class AnalyzerUnmappedGoldenTestCase extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    AnalyzerUnmappedGoldenTestCase(String mode) {
        super(mode);
    }

    protected static String nullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    protected static String load(String query) {
        return "SET unmapped_fields=\"load\"; " + query;
    }

    @Override
    protected TestBuilder builder(String query) {
        return super.builder(query).stages(STAGES.clone());
    }

    @Override
    protected List<String> filteredWarnings() {
        var filtered = new ArrayList<>(super.filteredWarnings());
        filtered.add(
            "has no implicit conversion from KEYWORD, so it will not be loaded from _source; values will be null in those indices"
        );
        return filtered;
    }
}
