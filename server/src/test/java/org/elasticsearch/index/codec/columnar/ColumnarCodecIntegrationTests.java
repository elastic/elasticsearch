/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class ColumnarCodecIntegrationTests extends ESSingleNodeTestCase {

    public void testColumnarCodecSettingIsSetOnColumnarIndex() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode)
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .build();

        final IndexService indexService = createIndex("columnar-index", settings, "@timestamp", "type=date", "kw", "type=keyword");
        assertTrue("mode=" + mode, indexService.getIndexSettings().isColumnarCodecEnabled());
    }
}
