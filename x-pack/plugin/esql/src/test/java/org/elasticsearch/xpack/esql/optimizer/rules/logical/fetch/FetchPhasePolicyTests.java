/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.hamcrest.Matchers.equalTo;

public class FetchPhasePolicyTests extends ESTestCase {

    private static final TransportVersion CURRENT = TransportVersion.current();
    private static final TransportVersion OLD = TransportVersionUtils.getPreviousVersion(FetchPhasePolicy.PLANNER_MINIMUM);

    /** A build without the feature never plans it, not even when a query asks for it. */
    public void testFeatureFlagWins() {
        for (Boolean pragma : new Boolean[] { null, true, false }) {
            FetchPhasePolicy policy = new FetchPhasePolicy(FetchPhaseMode.UNAVAILABLE, pragma, CURRENT);
            assertThat(policy.decide(), equalTo(Outcome.DISABLED_FEATURE_FLAG));
            assertThat(policy.mode(), equalTo(FetchPhaseMode.DISABLED));
        }
    }

    /** Without the pragma, the cluster's mode decides, and the setting turns on the cost decision. */
    public void testSettingDecidesWithoutPragma() {
        assertDecision(FetchPhaseMode.DISABLED, null, CURRENT, Outcome.DISABLED_SETTING, FetchPhaseMode.DISABLED);
        assertDecision(FetchPhaseMode.AUTO, null, CURRENT, Outcome.ENABLED, FetchPhaseMode.AUTO);
        assertDecision(FetchPhaseMode.ALWAYS, null, CURRENT, Outcome.ENABLED, FetchPhaseMode.ALWAYS);
    }

    /** The pragma forces the fetch on every plan that can use it, or turns it off, whatever the setting says. */
    public void testPragmaOverridesSetting() {
        assertDecision(FetchPhaseMode.DISABLED, true, CURRENT, Outcome.ENABLED, FetchPhaseMode.ALWAYS);
        assertDecision(FetchPhaseMode.AUTO, true, CURRENT, Outcome.ENABLED, FetchPhaseMode.ALWAYS);
        assertDecision(FetchPhaseMode.AUTO, false, CURRENT, Outcome.DISABLED_PRAGMA, FetchPhaseMode.DISABLED);
        assertDecision(FetchPhaseMode.DISABLED, false, CURRENT, Outcome.DISABLED_PRAGMA, FetchPhaseMode.DISABLED);
    }

    /** A cluster with a node that cannot read the fetch phase plans loads eagerly instead of failing. */
    public void testOldNodeFallsBack() {
        assertThat(new FetchPhasePolicy(FetchPhaseMode.AUTO, null, OLD).decide(), equalTo(Outcome.MIXED_VERSION_FALLBACK));
        assertThat(new FetchPhasePolicy(FetchPhaseMode.DISABLED, true, OLD).decide(), equalTo(Outcome.MIXED_VERSION_FALLBACK));
        assertThat(new FetchPhasePolicy(FetchPhaseMode.AUTO, null, FetchPhasePolicy.PLANNER_MINIMUM).decide(), equalTo(Outcome.ENABLED));
    }

    public void testFromContext() {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build());
        LogicalOptimizerContext context = new LogicalOptimizerContext(
            EsqlTestUtils.configuration(pragmas),
            FoldContext.small(),
            OLD,
            EsqlFlags.withFetchPhase(FetchPhaseMode.DISABLED)
        );
        assertThat(FetchPhasePolicy.from(context), equalTo(new FetchPhasePolicy(FetchPhaseMode.DISABLED, true, OLD)));
    }

    private static void assertDecision(
        FetchPhaseMode clusterMode,
        Boolean pragma,
        TransportVersion minimumVersion,
        Outcome outcome,
        FetchPhaseMode mode
    ) {
        FetchPhasePolicy policy = new FetchPhasePolicy(clusterMode, pragma, minimumVersion);
        assertThat(policy.decide(), equalTo(outcome));
        assertThat(policy.mode(), equalTo(mode));
    }
}
