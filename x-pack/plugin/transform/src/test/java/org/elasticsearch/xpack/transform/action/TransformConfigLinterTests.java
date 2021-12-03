/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.latest.Latest;
import org.elasticsearch.xpack.transform.transforms.pivot.Pivot;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class TransformConfigLinterTests extends ESTestCase {

    public void testGetWarnings_Pivot_WithScriptBasedRuntimeFields() {
        PivotConfig pivotConfig = new PivotConfig(
            GroupConfigTests.randomGroupConfig(() -> new TermsGroupSource(randomAlphaOfLengthBetween(1, 20), null, false)),
            AggregationConfigTests.randomAggregationConfig(),
            null
        );
        Function function = new Pivot(pivotConfig, new SettingsConfig(), Version.CURRENT, Collections.emptySet());
        SourceConfig sourceConfig = SourceConfigTests.randomSourceConfig();
        assertThat(TransformConfigLinter.getWarnings(function, sourceConfig, null), is(empty()));

        SyncConfig syncConfig = TimeSyncConfigTests.randomTimeSyncConfig();

        assertThat(TransformConfigLinter.getWarnings(function, sourceConfig, syncConfig), is(empty()));

        Map<String, Object> runtimeMappings = new HashMap<>() {
            {
                put("rt-field-A", singletonMap("type", "keyword"));
                put("rt-field-B", singletonMap("script", "some script"));
                put("rt-field-C", singletonMap("script", "some other script"));
            }
        };
        sourceConfig = new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            QueryConfigTests.randomQueryConfig(),
            runtimeMappings
        );
        assertThat(TransformConfigLinter.getWarnings(function, sourceConfig, syncConfig), is(empty()));

        syncConfig = new TimeSyncConfig("rt-field-B", null);
        assertThat(
            TransformConfigLinter.getWarnings(function, sourceConfig, syncConfig),
            contains("sync time field is a script-based runtime field, this transform might run slowly, please check your configuration.")
        );
    }

    public void testGetWarnings_Latest_WithScriptBasedRuntimeFields() {
        LatestConfig latestConfig = new LatestConfig(singletonList("rt-field-B"), "field-T");
        Function function = new Latest(latestConfig);
        SourceConfig sourceConfig = SourceConfigTests.randomSourceConfig();
        assertThat(TransformConfigLinter.getWarnings(function, sourceConfig, null), is(empty()));

        SyncConfig syncConfig = new TimeSyncConfig("rt-field-C", null);

        Map<String, Object> runtimeMappings = new HashMap<>() {
            {
                put("rt-field-A", singletonMap("type", "keyword"));
                put("rt-field-B", singletonMap("script", "some script"));
                put("rt-field-C", singletonMap("script", "some other script"));
            }
        };
        sourceConfig = new SourceConfig(
            generateRandomStringArray(10, 10, false, false),
            QueryConfigTests.randomQueryConfig(),
            runtimeMappings
        );

        assertThat(
            TransformConfigLinter.getWarnings(function, sourceConfig, syncConfig),
            contains(
                "all the group-by fields are script-based runtime fields, "
                    + "this transform might run slowly, please check your configuration.",
                "sync time field is a script-based runtime field, this transform might run slowly, please check your configuration."
            )
        );
    }

    public void testGetWarnings_Pivot_CouldNotFindAnyOptimization() {
        PivotConfig pivotConfig = new PivotConfig(
            GroupConfigTests.randomGroupConfig(
                () -> new HistogramGroupSource(
                    randomAlphaOfLengthBetween(1, 20),
                    null,
                    true,
                    randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false)
                )
            ),
            AggregationConfigTests.randomAggregationConfig(),
            null
        );
        Function function = new Pivot(pivotConfig, new SettingsConfig(), Version.CURRENT, Collections.emptySet());
        SourceConfig sourceConfig = SourceConfigTests.randomSourceConfig();
        SyncConfig syncConfig = TimeSyncConfigTests.randomTimeSyncConfig();
        assertThat(
            TransformConfigLinter.getWarnings(function, sourceConfig, syncConfig),
            contains(
                "could not find any optimizations for continuous execution, "
                    + "this transform might run slowly, please check your configuration."
            )
        );
    }
}
