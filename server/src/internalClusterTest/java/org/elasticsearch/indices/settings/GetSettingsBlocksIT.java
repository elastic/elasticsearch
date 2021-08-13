/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class GetSettingsBlocksIT extends ESIntegTestCase {
    public void testGetSettingsWithBlocks() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("index.refresh_interval", -1)
                        .put("index.merge.policy.expunge_deletes_allowed", "30")
                        .put(FieldMapper.IGNORE_MALFORMED_SETTING.getKey(), false)));

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE)) {
            try {
                enableIndexBlock("test", block);
                GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();
                assertThat(response.getIndexToSettings().size(), greaterThanOrEqualTo(1));
                assertThat(response.getSetting("test", "index.refresh_interval"), equalTo("-1"));
                assertThat(response.getSetting("test", "index.merge.policy.expunge_deletes_allowed"), equalTo("30"));
                assertThat(response.getSetting("test", FieldMapper.IGNORE_MALFORMED_SETTING.getKey()), equalTo("false"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetSettings("test"));
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
