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
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public void testMultiValuedKeywordRoundTripsThroughColumnarCodec() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(INDEX, columnarSettings(mode), "@timestamp", "type=date", "kw", "type=keyword");
        prepareIndex(INDEX).setSource("@timestamp", "2024-01-01T00:00:00Z", "kw", List.of("red", "green", "blue")).get();
        indicesAdmin().prepareRefresh(INDEX).get();

        assertKeywordFieldUsesColumnarFormat();

        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "red")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "green")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "blue")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "yellow")), 0);
    }

    /**
     * The codec stores a document's values separately and puts them back together on the way out, so an array
     * with an inline null has to come back through {@code _source} exactly as it went in — position and all.
     */
    public void testMultiValuedKeywordWithNullsRoundTripsThroughSource() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(INDEX, columnarSettings(mode), "@timestamp", "type=date", "kw", "type=keyword");
        final List<String> arrays = List.of(
            "[\"red\", null, \"blue\"]",
            "[null, \"green\"]",
            "[\"solo\"]",
            "[\"\", null, \"\"]",
            "[\"dup\", \"dup\"]"
        );
        for (int i = 0; i < arrays.size(); i++) {
            prepareIndex(INDEX).setId(Integer.toString(i))
                .setSource("{\"@timestamp\":\"2024-01-01T00:00:0" + i + "Z\",\"kw\":" + arrays.get(i) + "}", XContentType.JSON)
                .get();
        }
        indicesAdmin().prepareRefresh(INDEX).get();

        assertKeywordFieldUsesColumnarFormat();

        for (int i = 0; i < arrays.size(); i++) {
            final Map<String, Object> source = client().prepareGet(INDEX, Integer.toString(i)).get().getSourceAsMap();
            assertEquals("doc " + i, expectedValues(arrays.get(i)), source.get("kw"));
        }
    }

    /**
     * The codec's payload has to reconstruct {@code _source} exactly as the encoding it replaces does, including for the shapes that
     * carry no value at all — an empty array, and an array holding nothing but nulls. Rather than hardcode what those render as, this
     * indexes the same documents with the codec on and off and requires the two to agree.
     */
    public void testValuelessArraysRenderAsTheyDoWithoutTheCodec() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final List<String> arrays = List.of("[null]", "[]", "[null, null]", "[\"a\"]", "[\"a\", null]");

        final String withCodec = INDEX + "-codec";
        final String withoutCodec = INDEX + "-no-codec";
        createIndex(withCodec, columnarSettings(mode, true), "@timestamp", "type=date", "kw", "type=keyword");
        createIndex(withoutCodec, columnarSettings(mode, false), "@timestamp", "type=date", "kw", "type=keyword");

        for (String index : List.of(withCodec, withoutCodec)) {
            for (int i = 0; i < arrays.size(); i++) {
                prepareIndex(index).setId(Integer.toString(i))
                    .setSource("{\"@timestamp\":\"2024-01-01T00:00:0" + i + "Z\",\"kw\":" + arrays.get(i) + "}", XContentType.JSON)
                    .get();
            }
            indicesAdmin().prepareRefresh(index).get();
        }

        for (int i = 0; i < arrays.size(); i++) {
            final Map<String, Object> codec = client().prepareGet(withCodec, Integer.toString(i)).get().getSourceAsMap();
            final Map<String, Object> plain = client().prepareGet(withoutCodec, Integer.toString(i)).get().getSourceAsMap();
            assertEquals(arrays.get(i), plain.get("kw"), codec.get("kw"));
            assertEquals(arrays.get(i) + " field presence", plain.containsKey("kw"), codec.containsKey("kw"));
        }
    }

    /** The values of {@code array}, as {@code _source} renders them: a lone value is not wrapped in a list. */
    private static Object expectedValues(String array) {
        final List<Object> values = new ArrayList<>();
        for (String element : array.substring(1, array.length() - 1).split(",")) {
            final String trimmed = element.trim();
            values.add(trimmed.equals("null") ? null : trimmed.substring(1, trimmed.length() - 1));
        }
        return values.size() == 1 ? values.get(0) : values;
    }

    private static Settings columnarSettings(IndexMode mode) {
        return columnarSettings(mode, true);
    }

    private static Settings columnarSettings(IndexMode mode, boolean codecEnabled) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode)
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), codecEnabled)
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
