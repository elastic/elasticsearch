/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;

import java.util.HashSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class EsqlFlagsTests extends ESTestCase {

    /** Nothing can run a fetch phase plan yet, so the defaults and the flags built from the cluster settings keep it off. */
    public void testProductionFlagsNeverPlanTheFetchPhase() {
        assertThat(EsqlFlags.DEFAULTS.fetchPhaseMode(), equalTo(FetchPhaseMode.DISABLED));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        assertThat(new EsqlFlags(clusterSettings).fetchPhaseMode(), equalTo(FetchPhaseMode.DISABLED));
    }

    public void testWithFetchPhase() {
        for (FetchPhaseMode mode : FetchPhaseMode.values()) {
            assertThat(EsqlFlags.withFetchPhase(mode).fetchPhaseMode(), equalTo(mode));
        }
    }

    public void testParserLimitsDefaults() {
        assertThat(EsqlFlags.DEFAULTS.maxBinaryOperators(), equalTo(EsqlParser.MAX_BINARY_OPERATORS));
        assertThat(EsqlFlags.DEFAULTS.maxLeftParentheses(), equalTo(EsqlParser.MAX_LEFT_PARENTHESES));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        assertThat(new EsqlFlags(clusterSettings), equalTo(EsqlFlags.DEFAULTS));
    }

    public void testParserLimitsFollowDynamicClusterSettings() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        clusterSettings.applySettings(
            Settings.builder()
                .put(EsqlFlags.ESQL_MAX_BINARY_OPERATORS.getKey(), 7)
                .put(EsqlFlags.ESQL_MAX_LEFT_PARENTHESES.getKey(), 3)
                .build()
        );
        EsqlFlags flags = new EsqlFlags(clusterSettings);
        assertThat(flags.maxBinaryOperators(), equalTo(7));
        assertThat(flags.maxLeftParentheses(), equalTo(3));
        assertThat(flags, equalTo(EsqlFlags.withParserLimits(7, 3)));
    }

    public void testParserLimitsMustBePositive() {
        for (var setting : new Setting<?>[] { EsqlFlags.ESQL_MAX_BINARY_OPERATORS, EsqlFlags.ESQL_MAX_LEFT_PARENTHESES }) {
            Settings invalid = Settings.builder().put(setting.getKey(), 0).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(invalid));
            assertThat(e.getMessage(), containsString(setting.getKey()));
        }
    }
}
