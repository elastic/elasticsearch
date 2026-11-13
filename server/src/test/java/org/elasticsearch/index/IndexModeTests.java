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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class IndexModeTests extends ESTestCase {

    public void testRequiresTimestampMatchesDefaultMapping() {
        for (IndexMode mode : IndexMode.values()) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.MODE.getKey(), mode.getName());
            if (mode == IndexMode.TIME_SERIES) {
                settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host");
            }
            IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), Settings.EMPTY);
            CompressedXContent defaultMapping = mode.getDefaultMapping(indexSettings);
            boolean enablesTimestamp = defaultMapping != null
                && defaultMapping.string().contains("\"" + DataStreamTimestampFieldMapper.NAME + "\":{\"enabled\":true}");
            assertThat(mode.getName(), mode.requiresTimestamp(), equalTo(enablesTimestamp));
        }
    }
}
