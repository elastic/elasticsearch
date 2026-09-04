/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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

    protected TestBuilder loadAll(String query, String... variants) {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL.isEnabled());
        return builder("SET unmapped_fields=\"LOAD_ALL\"; " + query).nestedPath(ArrayUtils.prepend("load_all", variants));
    }

    /**
     * Like {@link #loadAll(String, String...)}, but uses an explicit set of {@link Stage}s instead of the
     * class-level {@code STAGES = {ANALYSIS}}. Use this when additional pipeline stages (e.g.
     * {@link Stage#LOCAL_PHYSICAL_OPTIMIZATION}) are needed to capture physical plan behavior.
     */
    protected TestBuilder loadAll(EnumSet<Stage> stages, String query, String... variants) {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL.isEnabled());
        return super.builder("SET unmapped_fields=\"LOAD_ALL\"; " + query).stages(stages)
            .nestedPath(ArrayUtils.prepend("load_all", variants));
    }

    /** Runs the same query in the nullify and load modes. */
    protected void runInNullifyAndLoadModes(String query, String... variants) {
        nullify(query, variants).run();
        load(query, variants).run();
    }

    /** Runs the same query and views in the nullify and load modes. */
    protected void runInNullifyAndLoadModes(String query, Map<String, String> views, String... variants) {
        nullify(query, variants).views(views).run();
        load(query, variants).views(views).run();
    }

    @Override
    protected TestBuilder builder(String query) {
        return super.builder(query).stages(STAGES);
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
