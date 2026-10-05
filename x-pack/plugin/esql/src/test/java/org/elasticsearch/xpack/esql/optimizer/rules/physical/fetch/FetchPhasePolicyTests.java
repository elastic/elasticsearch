/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.hamcrest.Matchers.equalTo;

public class FetchPhasePolicyTests extends ESTestCase {

    private static final TransportVersion CURRENT = TransportVersion.current();
    private static final TransportVersion OLD = TransportVersionUtils.getPreviousVersion(FetchPhasePolicy.PLANNER_MINIMUM);

    /** A build without the feature never plans it, not even when a query asks for it. */
    public void testFeatureFlagWins() {
        assertThat(decide(FetchPhaseMode.UNAVAILABLE, null, CURRENT), equalTo(Outcome.DISABLED_FEATURE_FLAG));
        assertThat(decide(FetchPhaseMode.UNAVAILABLE, true, CURRENT), equalTo(Outcome.DISABLED_FEATURE_FLAG));
        assertThat(decide(FetchPhaseMode.UNAVAILABLE, false, CURRENT), equalTo(Outcome.DISABLED_FEATURE_FLAG));
    }

    public void testSettingDecidesWithoutPragma() {
        assertThat(decide(FetchPhaseMode.DISABLED, null, CURRENT), equalTo(Outcome.DISABLED_SETTING));
        assertThat(decide(FetchPhaseMode.ENABLED, null, CURRENT), equalTo(Outcome.ENABLED));
    }

    public void testPragmaOverridesSetting() {
        assertThat(decide(FetchPhaseMode.DISABLED, true, CURRENT), equalTo(Outcome.ENABLED));
        assertThat(decide(FetchPhaseMode.ENABLED, false, CURRENT), equalTo(Outcome.DISABLED_PRAGMA));
        assertThat(decide(FetchPhaseMode.DISABLED, false, CURRENT), equalTo(Outcome.DISABLED_PRAGMA));
    }

    /** A cluster with a node that cannot read the fetch phase plans loads eagerly instead of failing. */
    public void testOldNodeFallsBack() {
        assertThat(decide(FetchPhaseMode.ENABLED, null, OLD), equalTo(Outcome.MIXED_VERSION_FALLBACK));
        assertThat(decide(FetchPhaseMode.DISABLED, true, OLD), equalTo(Outcome.MIXED_VERSION_FALLBACK));
        assertThat(decide(FetchPhaseMode.ENABLED, null, FetchPhasePolicy.PLANNER_MINIMUM), equalTo(Outcome.ENABLED));
    }

    public void testFromContext() {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build());
        PhysicalOptimizerContext context = new PhysicalOptimizerContext(
            EsqlTestUtils.configuration(pragmas),
            OLD,
            EsqlFlags.withFetchPhase(false)
        );
        assertThat(FetchPhasePolicy.from(context), equalTo(new FetchPhasePolicy(FetchPhaseMode.DISABLED, true, OLD)));
    }

    private static Outcome decide(FetchPhaseMode mode, Boolean pragma, TransportVersion minimumVersion) {
        return new FetchPhasePolicy(mode, pragma, minimumVersion).decide();
    }
}
