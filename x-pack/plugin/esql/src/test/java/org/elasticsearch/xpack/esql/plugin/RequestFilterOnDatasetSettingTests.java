/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.dsltranslate.RequestFilterRewriter;

import static org.hamcrest.Matchers.containsString;

/**
 * The opt-in gate for applying the request filter to datasets. The setting is off by default in every build, and the
 * feature flag decides whether it may be turned on at all — so a release build cannot enable the feature even
 * deliberately. Mirrors {@code IndexSettingsTests#testSliceEnabledSettingRequiresFeatureFlag}, the shape this follows.
 */
public class RequestFilterOnDatasetSettingTests extends ESTestCase {

    /** The whole point of the gate: shipping this code cannot change what an existing dataset query returns. */
    public void testDefaultsToDisabledInEveryBuild() {
        assertFalse(EsqlPlugin.REQUEST_FILTER_ON_DATASET_ENABLED.get(Settings.EMPTY));
    }

    /** In a release build the flag is off, and then enabling the setting is refused outright. */
    public void testEnablingRequiresTheFeatureFlag() {
        assumeFalse(
            "request-filter-on-dataset feature flag must be disabled",
            RequestFilterRewriter.REQUEST_FILTER_ON_DATASET_FEATURE_FLAG.isEnabled()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlPlugin.REQUEST_FILTER_ON_DATASET_ENABLED.get(
                Settings.builder().put(EsqlPlugin.REQUEST_FILTER_ON_DATASET_ENABLED.getKey(), true).build()
            )
        );
        assertThat(e.getMessage(), containsString("unknown setting [esql.query.request_filter_on_dataset.enabled]"));
    }

    /** In a snapshot build the flag is on, so the setting can be turned on deliberately for testing and benchmarking. */
    public void testEnablingIsAllowedWhenTheFeatureFlagIsOn() {
        assumeTrue(
            "request-filter-on-dataset feature flag must be enabled",
            RequestFilterRewriter.REQUEST_FILTER_ON_DATASET_FEATURE_FLAG.isEnabled()
        );
        assertTrue(
            EsqlPlugin.REQUEST_FILTER_ON_DATASET_ENABLED.get(
                Settings.builder().put(EsqlPlugin.REQUEST_FILTER_ON_DATASET_ENABLED.getKey(), true).build()
            )
        );
    }
}
