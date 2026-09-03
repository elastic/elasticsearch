/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.xpack.transform.transforms.Function;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class LatestTests extends ESTestCase {

    public void testValidateConfig() {
        LatestConfig latestConfig = LatestConfigTests.randomLatestConfig();
        Function latest = new Latest(latestConfig);
        latest.validateConfig(ActionTestUtils.assertNoFailureListener(isValid -> assertThat(isValid, is(true))));
    }

    public void testGetPerformanceCriticalFields() {
        LatestConfig latestConfig = new LatestConfig(Arrays.asList("field-A", "field-B"), "field-C");
        Function latest = new Latest(latestConfig);
        assertThat(latest.getPerformanceCriticalFields(), contains("field-A", "field-B"));
    }

    public void testBuildChangeCollectorUsesBoundedWindowWhenSortMatchesSyncField() {
        Latest latest = new Latest(new LatestConfig(List.of("host"), "@timestamp"));
        // sort field == sync field: bounded change detection is correct and avoids scanning old data.
        assertFalse(latest.buildChangeCollector("@timestamp").queryForChanges());
    }

    public void testBuildChangeCollectorUsesTwoPhaseWhenSortDiffersFromSyncField() {
        Latest latest = new Latest(new LatestConfig(List.of("host"), "event.sort"));
        // sort field != sync field: the two-phase (all-history) detection is required for correctness (gh#90643).
        assertTrue(latest.buildChangeCollector("@timestamp").queryForChanges());
    }

    public void testDisablingBoundedChangeDetectionForcesTwoPhaseEvenWhenSortMatchesSyncField() {
        SettingsConfig settings = new SettingsConfig.Builder().setAlignChangeDetection(false).build();
        Latest latest = new Latest(new LatestConfig(List.of("host"), "@timestamp"), settings);
        // The setting is a kill switch: disabling it forces two-phase detection even where bounded would be safe.
        assertTrue(latest.buildChangeCollector("@timestamp").queryForChanges());
    }

    public void testBoundedSettingCannotForceBoundedWhenSortDiffersFromSyncField() {
        SettingsConfig settings = new SettingsConfig.Builder().setAlignChangeDetection(true).build();
        Latest latest = new Latest(new LatestConfig(List.of("host"), "event.sort"), settings);
        // Bounded detection only applies when sort == sync; it can never be forced on for a non-monotonic config.
        assertTrue(latest.buildChangeCollector("@timestamp").queryForChanges());
    }
}
