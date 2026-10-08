/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;

import java.util.HashSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class EsqlFlagsTests extends ESTestCase {

    /** The setting only takes effect on builds that carry the feature. */
    public void testFetchPhaseModeNeedsFeatureFlag() {
        assertThat(EsqlFlags.fetchPhaseMode(false, true), equalTo(FetchPhaseMode.UNAVAILABLE));
        assertThat(EsqlFlags.fetchPhaseMode(false, false), equalTo(FetchPhaseMode.UNAVAILABLE));
        assertThat(EsqlFlags.fetchPhaseMode(true, true), equalTo(FetchPhaseMode.ENABLED));
        assertThat(EsqlFlags.fetchPhaseMode(true, false), equalTo(FetchPhaseMode.DISABLED));
    }

    public void testFetchPhaseFromClusterSettings() {
        boolean enabled = randomBoolean();
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(EsqlFlags.ESQL_FETCH_PHASE.getKey(), enabled).build(),
            new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS)
        );
        assertThat(
            new EsqlFlags(clusterSettings).fetchPhaseMode(),
            equalTo(EsqlFlags.fetchPhaseMode(EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled(), enabled))
        );
    }

    /** Off by default: the runtime of the fetch phase is not complete yet. */
    public void testFetchPhaseIsOffByDefault() {
        assertFalse(EsqlFlags.ESQL_FETCH_PHASE.getDefault(Settings.EMPTY));
        assertThat(
            EsqlFlags.DEFAULTS.fetchPhaseMode(),
            equalTo(EsqlFlags.fetchPhaseMode(EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled(), false))
        );
        assertThat(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS, hasItem(EsqlFlags.ESQL_FETCH_PHASE));
    }

    /** Planner tests pin the mode, so they behave the same whether the test JVM carries the feature flag or not. */
    public void testWithFetchPhase() {
        assertThat(EsqlFlags.withFetchPhase(true).fetchPhaseMode(), equalTo(FetchPhaseMode.ENABLED));
        assertThat(EsqlFlags.withFetchPhase(false).fetchPhaseMode(), equalTo(FetchPhaseMode.DISABLED));
    }
}
