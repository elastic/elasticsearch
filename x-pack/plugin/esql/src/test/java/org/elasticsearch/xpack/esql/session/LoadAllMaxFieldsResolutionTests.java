/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import static org.hamcrest.Matchers.is;

/**
 * A limit of 0 on the fields {@code LOAD_ALL} discovers makes it behave like {@code LOAD}, which is settled when the query
 * settings are resolved, so that every phase of the query sees {@code LOAD}.
 */
public class LoadAllMaxFieldsResolutionTests extends ESTestCase {

    private static ResolvedSettings resolvedTo(UnmappedResolution resolution) {
        return ResolvedSettings.EMPTY.withOverride(QuerySettings.UNMAPPED_FIELDS, resolution);
    }

    private static PlannerSettings limit(int maxFields) {
        return PlannerSettings.DEFAULTS.loadAllMaxFields(maxFields);
    }

    public void testLoadAllBecomesLoadWhenTheLimitIsZero() {
        ResolvedSettings settled = EsqlSession.applyLoadAllMaxFields(resolvedTo(UnmappedResolution.LOAD_ALL), limit(0));
        assertThat(QuerySettings.UNMAPPED_FIELDS.get(settled), is(UnmappedResolution.LOAD));
    }

    public void testLoadAllStaysLoadAllWhenTheLimitIsPositive() {
        ResolvedSettings resolved = resolvedTo(UnmappedResolution.LOAD_ALL);
        ResolvedSettings settled = EsqlSession.applyLoadAllMaxFields(resolved, limit(between(1, 100_000)));
        assertThat(QuerySettings.UNMAPPED_FIELDS.get(settled), is(UnmappedResolution.LOAD_ALL));
    }

    public void testOtherResolutionsAreLeftAloneWhateverTheLimit() {
        for (UnmappedResolution resolution : new UnmappedResolution[] {
            UnmappedResolution.DEFAULT,
            UnmappedResolution.NULLIFY,
            UnmappedResolution.LOAD }) {
            ResolvedSettings settled = EsqlSession.applyLoadAllMaxFields(resolvedTo(resolution), limit(0));
            assertThat(QuerySettings.UNMAPPED_FIELDS.get(settled), is(resolution));
        }
    }
}
