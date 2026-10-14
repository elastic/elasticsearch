/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class HashInIdRoutingSettingProviderTests extends ESTestCase {

    private static final String INDEX_KEY = IndexMetadata.INDEX_ROUTING_HASH_IN_ID.getKey();

    public void testDisabledByDefault() {
        assertTrue(provide(provider(Settings.EMPTY), "test", Settings.EMPTY).isEmpty());
    }

    public void testEnabledAddsSetting() {
        assertThat(provide(enabledProvider(), "test", Settings.EMPTY).get(INDEX_KEY), equalTo("true"));
    }

    public void testEnabledSkipsDotPrefixedIndex() {
        assertTrue(provide(enabledProvider(), ".test", Settings.EMPTY).isEmpty());
    }

    public void testEnabledAppliesToDataStreamBackingIndex() {
        assertThat(provide(enabledProvider(), ".ds-logs-000001", "logs", Settings.EMPTY).get(INDEX_KEY), equalTo("true"));
    }

    public void testEnabledKeepsExplicitSetting() {
        Settings explicit = Settings.builder().put(INDEX_KEY, randomBoolean()).build();
        assertTrue(provide(enabledProvider(), "test", explicit).isEmpty());
    }

    private static HashInIdRoutingSettingProvider enabledProvider() {
        return provider(Settings.builder().put(HashInIdRoutingSettingProvider.HASH_IN_ID_ROUTING_ENABLED_SETTING.getKey(), true).build());
    }

    private static HashInIdRoutingSettingProvider provider(Settings nodeSettings) {
        return new HashInIdRoutingSettingProvider(
            new ClusterSettings(nodeSettings, Set.of(HashInIdRoutingSettingProvider.HASH_IN_ID_ROUTING_ENABLED_SETTING))
        );
    }

    private static Settings provide(HashInIdRoutingSettingProvider provider, String indexName, Settings templateAndRequestSettings) {
        return provide(provider, indexName, null, templateAndRequestSettings);
    }

    private static Settings provide(
        HashInIdRoutingSettingProvider provider,
        String indexName,
        String dataStreamName,
        Settings templateAndRequestSettings
    ) {
        Settings.Builder additionalSettings = Settings.builder();
        provider.provideAdditionalSettings(
            indexName,
            dataStreamName,
            null,
            false,
            null,
            Instant.ofEpochMilli(0L),
            templateAndRequestSettings,
            List.of(),
            IndexVersion.current(),
            additionalSettings
        );
        return additionalSettings.build();
    }
}
