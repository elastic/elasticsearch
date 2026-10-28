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

    /** Nothing can run a fetch phase plan yet, so the defaults and the flags built from the cluster settings keep it off. */
    public void testProductionFlagsNeverPlanTheFetchPhase() {
        assertThat(EsqlFlags.DEFAULTS.fetchPhaseMode(), equalTo(FetchPhaseMode.DISABLED));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        assertThat(new EsqlFlags(clusterSettings).fetchPhaseMode(), equalTo(FetchPhaseMode.DISABLED));
    }

    public void testWithFetchPhase() {
        assertThat(EsqlFlags.withFetchPhase(true).fetchPhaseMode(), equalTo(FetchPhaseMode.ENABLED));
        assertThat(EsqlFlags.withFetchPhase(false).fetchPhaseMode(), equalTo(FetchPhaseMode.DISABLED));
    }
}
