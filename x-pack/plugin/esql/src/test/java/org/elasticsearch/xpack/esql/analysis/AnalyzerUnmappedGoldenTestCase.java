/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/** Shared analysis setup and mode-specific builders for analyzer tests of unmapped field behavior. */
abstract class AnalyzerUnmappedGoldenTestCase extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    AnalyzerUnmappedGoldenTestCase(String mode) {
        super(mode);
    }

    protected TestBuilder nullify(String query, String... variants) {
        return builder("SET unmapped_fields=\"nullify\"; " + query).nestedPath(ArrayUtils.prepend("nullify", variants));
    }

    protected TestBuilder load(String query, String... variants) {
        return builder("SET unmapped_fields=\"load\"; " + query).nestedPath(ArrayUtils.prepend("load", variants));
    }

    /** Runs the same query in both unmapped-field modes. */
    protected void runInBothModes(String query, String... variants) {
        nullify(query, variants).run();
        load(query, variants).run();
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
