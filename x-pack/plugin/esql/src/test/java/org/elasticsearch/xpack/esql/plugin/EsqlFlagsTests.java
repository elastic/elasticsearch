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

public class EsqlFlagsTests extends ESTestCase {

    /** A build without the feature can't turn the fetch phase on. A build with it leaves it to the pragma. */
    public void testFetchPhaseModeNeedsFeatureFlag() {
        assertThat(EsqlFlags.fetchPhaseMode(false), equalTo(FetchPhaseMode.UNAVAILABLE));
        assertThat(EsqlFlags.fetchPhaseMode(true), equalTo(FetchPhaseMode.DISABLED));
    }

    /** Off by default: the defaults and the flags built from the cluster settings never plan the fetch phase. */
    public void testFetchPhaseIsOffByDefault() {
        FetchPhaseMode off = EsqlFlags.fetchPhaseMode(EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assertThat(EsqlFlags.DEFAULTS.fetchPhaseMode(), equalTo(off));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        assertThat(new EsqlFlags(clusterSettings).fetchPhaseMode(), equalTo(off));
    }

    /** Planner tests pin the mode, so they behave the same whether the test JVM carries the feature flag or not. */
    public void testWithFetchPhase() {
        for (FetchPhaseMode mode : FetchPhaseMode.values()) {
            assertThat(EsqlFlags.withFetchPhase(mode).fetchPhaseMode(), equalTo(mode));
        }
    }
}
