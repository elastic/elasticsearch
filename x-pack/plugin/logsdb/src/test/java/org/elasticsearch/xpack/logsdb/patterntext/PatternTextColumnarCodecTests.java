/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class PatternTextColumnarCodecTests extends ESSingleNodeTestCase {

    private static final String INDEX = "pattern-text-columnar-index";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MapperExtrasPlugin.class, XPackPlugin.class, LogsDBPlugin.class);
    }

    private static Settings columnarSettings(final IndexMode mode) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode)
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testPatternTextFieldFlushes() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(INDEX, columnarSettings(mode), "@timestamp", "type=date", "msg", "type=pattern_text");
        prepareIndex(INDEX).setSource("@timestamp", "2024-01-01T00:00:00Z", "msg", "Error 123 at line 456").get();

        final BroadcastResponse refresh = indicesAdmin().prepareRefresh(INDEX).get();
        assertEquals("mode=" + mode + " " + Arrays.toString(refresh.getShardFailures()), 0, refresh.getFailedShards());
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("msg", "Error")), 1);
    }
}
