/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesMetricsTests extends ESTestCase {

    public void testGetStatsWithoutCacheSkipsShardsWithAbsentMode() {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", Settings.EMPTY);
        assertEquals(IndexMode.STANDARD, indexSettings.getMode());

        final IndexShard shard = mock(IndexShard.class);
        when(shard.isSystem()).thenReturn(false);
        when(shard.indexSettings()).thenReturn(indexSettings);

        final IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenAnswer(inv -> List.of(shard).iterator());

        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenAnswer(inv -> List.of(indexService).iterator());

        final IndexMode[] subset = Arrays.stream(IndexMode.values()).filter(m -> m != IndexMode.STANDARD).toArray(IndexMode[]::new);

        final Map<IndexMode, IndexStats> result = IndicesMetrics.getStatsWithoutCache(indicesService, subset);

        for (IndexMode mode : subset) {
            assertThat(result, hasKey(mode));
        }
        assertThat(result, not(hasKey(IndexMode.STANDARD)));
    }
}
