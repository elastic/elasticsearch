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

    public void testParserLimitsDefaults() {
        assertThat(EsqlFlags.DEFAULTS.maxBinaryOperatorsPerExpression(), equalTo(EsqlParser.MAX_BINARY_OPERATORS));
        // the parentheses setting counts the opening and the closing ones, the parser uses half of it
        assertThat(EsqlFlags.DEFAULTS.maxParenthesesCount(), equalTo(2 * EsqlParser.MAX_LEFT_PARENTHESES));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        assertThat(new EsqlFlags(clusterSettings), equalTo(EsqlFlags.DEFAULTS));
    }

    public void testParserLimitsFollowDynamicClusterSettings() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS));
        clusterSettings.applySettings(
            Settings.builder()
                .put(EsqlFlags.ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION.getKey(), 7)
                .put(EsqlFlags.ESQL_MAX_PARENTHESES_COUNT.getKey(), 3)
                .build()
        );
        EsqlFlags flags = new EsqlFlags(clusterSettings);
        assertThat(flags.maxBinaryOperatorsPerExpression(), equalTo(7));
        assertThat(flags.maxParenthesesCount(), equalTo(3));
        assertThat(flags, equalTo(EsqlFlags.withParserLimits(7, 3)));
    }

    public void testParserLimitsMinimums() {
        assertThat(
            EsqlFlags.ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION.get(settingWithValue(EsqlFlags.ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION, 1)),
            equalTo(1)
        );
        assertThat(EsqlFlags.ESQL_MAX_PARENTHESES_COUNT.get(settingWithValue(EsqlFlags.ESQL_MAX_PARENTHESES_COUNT, 2)), equalTo(2));
        // at least one opening parenthesis must be allowed, the parser uses half of the parentheses setting
        for (var entry : new Setting<?>[] { EsqlFlags.ESQL_MAX_BINARY_OPERATORS_PER_EXPRESSION, EsqlFlags.ESQL_MAX_PARENTHESES_COUNT }) {
            int belowMinimum = entry == EsqlFlags.ESQL_MAX_PARENTHESES_COUNT ? 1 : 0;
            Settings invalid = settingWithValue(entry, belowMinimum);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> entry.get(invalid));
            assertThat(e.getMessage(), containsString(entry.getKey()));
        }
    }

    private static Settings settingWithValue(Setting<?> setting, int value) {
        return Settings.builder().put(setting.getKey(), value).build();
    }
}
