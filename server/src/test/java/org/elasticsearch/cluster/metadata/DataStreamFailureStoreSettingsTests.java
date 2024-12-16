/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.Stream;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;
import static org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING;
import static org.hamcrest.Matchers.is;

public class DataStreamFailureStoreSettingsTests extends ESTestCase {

    public void testFailureStoreEnabledForDataStreamName_defaultSettings() {
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings()
        );

        // The default should return false for any input.
        // The following will include some illegal names, but it's still valid to test how the method treats them.
        Stream.generate(() -> randomAsciiAlphanumOfLengthBetween(random(), 1, 20))
            .limit(100)
            .forEach(name -> assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName(name), is(false)));
        Stream.generate(() -> randomUnicodeOfLengthBetween(1, 20))
            .limit(100)
            .forEach(name -> assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName(name), is(false)));
    }

    public void testFailureStoreEnabledForDataStreamName_exactMatches() {
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                // Match exactly 'foo' and 'bar' — whitespace should be stripped:
                Settings.builder().put(DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "  foo  , bar  ").build()
            )
        );

        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("foo"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("bar"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("food"), is(false));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("tbar"), is(false));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName(".foo"), is(false));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("barf"), is(false));
    }

    public void testFailureStoreEnabledForDataStreamName_wildcardMatches() {
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder().put(DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "  foo*  , *bar  ,  a*z  ").build()
            )
        );

        // These tests aren't exhaustive as the library used is tested thoroughly, but they provide a basic check of the correct usage:
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("foo"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("bar"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("food"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("tbar"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("az"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("a123z"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName(".foo"), is(false));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("barf"), is(false));
    }

    public void testFailureStoreEnabledForDataStreamName_respondsToSettingsChange() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder().put(DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "foo").build()
        );
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings = DataStreamFailureStoreSettings.create(clusterSettings);

        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("foo"), is(true));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("bar"), is(false));

        clusterSettings.applySettings(Settings.builder().put(DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "bar").build());

        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("foo"), is(false));
        assertThat(dataStreamFailureStoreSettings.failureStoreEnabledForDataStreamName("bar"), is(true));
    }
}
