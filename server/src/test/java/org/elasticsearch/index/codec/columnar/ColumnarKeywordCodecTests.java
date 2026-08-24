/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class ColumnarKeywordCodecTests extends ESSingleNodeTestCase {

    private static final String INDEX = "columnar-index";

    public void testColumnarCodecSettingIsSetOnColumnarIndex() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final IndexService indexService = createIndex(INDEX, columnarSettings(mode), "@timestamp", "type=date", "kw", "type=keyword");
        assertTrue("mode=" + mode, indexService.getIndexSettings().isColumnarCodecEnabled());

        final GetSettingsResponse settings = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, INDEX).get();
        assertEquals("true", settings.getSetting(INDEX, IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey()));
    }

    public void testKeywordRoundTripsThroughColumnarCodec() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(INDEX, columnarSettings(mode), "@timestamp", "type=date", "kw", "type=keyword");
        prepareIndex(INDEX).setSource("@timestamp", "2024-01-01T00:00:00Z", "kw", "hello").get();
        prepareIndex(INDEX).setSource("@timestamp", "2024-01-01T00:00:01Z", "kw", "world").get();
        indicesAdmin().prepareRefresh(INDEX).get();

        assertKeywordFieldUsesColumnarFormat();

        assertHitCount(client().prepareSearch(INDEX).setSize(0), 2);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "hello")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "world")), 1);
    }

    private static Settings columnarSettings(IndexMode mode) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode)
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .build();
    }

    private void assertKeywordFieldUsesColumnarFormat() throws IOException {
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            boolean asserted = false;
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo("kw");
                if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    assertEquals(ColumnarFormat.NAME, fieldInfo.getAttribute("PerFieldDocValuesFormat.format"));
                    asserted = true;
                }
            }
            assertTrue("expected a keyword doc-values field to assert on", asserted);
        }
    }
}
