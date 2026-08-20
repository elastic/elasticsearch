/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;

public class ColumnarLuceneSyntheticSourceChangesSnapshotTests extends LuceneSyntheticSourceChangesSnapshotTests {
    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.name())
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY.name())
            // Workaround for https://github.com/elastic/elasticsearch/issues/150408: seq no pruning
            // during merges drops _seq_no from soft-deleted docs, causing LuceneSyntheticSourceChangesSnapshot
            // to miss DELETE tombstones and stale ops.
            .put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), false)
            .build();
    }

    @Override
    protected String defaultMapping() {
        return """
            {
                "dynamic": false,
                "properties": {
                    "value": {
                        "type": "keyword"
                    }
                }
            }
            """;
    }
}
