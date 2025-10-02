/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.metadata.DataStreamFailureStoreDefinition.INDEX_FAILURE_STORE_VERSION_SETTING_NAME;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamFailureStoreDefinitionTests extends ESTestCase {

    public void testSettingsFiltering() {
        // Empty
        Settings.Builder builder = Settings.builder();
        Settings.Builder expectedBuilder = Settings.builder();
        assertThat(DataStreamFailureStoreDefinition.filterUserDefinedSettings(builder).keys(), equalTo(expectedBuilder.keys()));

        // All supported settings
        builder.put(INDEX_FAILURE_STORE_VERSION_SETTING_NAME, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(DataTier.TIER_PREFERENCE, "data_cold")
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-10")
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "." + randomAlphaOfLength(4), randomAlphaOfLength(4))
            .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "." + randomAlphaOfLength(4), randomAlphaOfLength(4))
            .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "." + randomAlphaOfLength(4), randomAlphaOfLength(4));
        // We expect no changes
        expectedBuilder = Settings.builder().put(builder.build());
        assertThat(DataStreamFailureStoreDefinition.filterUserDefinedSettings(builder).keys(), equalTo(expectedBuilder.keys()));

        // Remove unsupported settings
        String randomSetting = randomAlphaOfLength(10);
        builder.put(INDEX_FAILURE_STORE_VERSION_SETTING_NAME, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(DataTier.TIER_PREFERENCE, "data_cold")
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-10")
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
            .put(IndexMetadata.LIFECYCLE_NAME, "my-policy")
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "." + randomAlphaOfLength(4), randomAlphaOfLength(4))
            .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "." + randomAlphaOfLength(4), randomAlphaOfLength(4))
            .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "." + randomAlphaOfLength(4), randomAlphaOfLength(4))
            .put(IndexSettings.MODE.getKey(), randomFrom(IndexMode.values()))
            .put(randomSetting, randomAlphaOfLength(10));
        // We expect no changes
        expectedBuilder = Settings.builder().put(builder.build());
        assertThat(
            DataStreamFailureStoreDefinition.filterUserDefinedSettings(builder).keys().size(),
            equalTo(expectedBuilder.keys().size() - 3)
        );
        assertThat(
            DataStreamFailureStoreDefinition.filterUserDefinedSettings(builder).keys().contains(IndexSettings.MODE.getKey()),
            equalTo(false)
        );
        assertThat(DataStreamFailureStoreDefinition.filterUserDefinedSettings(builder).keys().contains(randomSetting), equalTo(false));
    }

}
