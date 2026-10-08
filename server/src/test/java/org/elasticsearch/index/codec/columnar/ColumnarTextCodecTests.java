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
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * A text field written and read through the ColumNAR codec, end to end. The counterpart of
 * {@link ColumnarKeywordCodecTests}: the two fields are written in the same payload and differ in what they
 * are written with, so what is checked here is that a text column survives the format.
 */
public class ColumnarTextCodecTests extends ESSingleNodeTestCase {

    private static final String INDEX = "columnar-text-index";
    private static final String FIELD = "body";

    public void testTextIsStoredByTheCodec() throws IOException {
        assumeColumnarCodecEnabled();
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(INDEX, columnarSettings(mode, true), "@timestamp", "type=date", FIELD, "type=text");
        index(0, "\"a line of a log\"");
        indicesAdmin().prepareRefresh(INDEX).get();

        assertEquals("doc-values format of [" + FIELD + "]", ColumnarFormat.NAME, docValuesFormat());
    }

    /** With the codec off the same field keeps the format it had, so the gate is what moves it. */
    public void testTextIsNotStoredByTheCodecWhenItIsOff() throws IOException {
        assumeColumnarCodecEnabled();
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        createIndex(INDEX, columnarSettings(mode, false), "@timestamp", "type=date", FIELD, "type=text");
        index(0, "\"a line of a log\"");
        indicesAdmin().prepareRefresh(INDEX).get();

        assertNotEquals(ColumnarFormat.NAME, docValuesFormat());
    }

    public void testTextIsSearchableAndReadableThroughTheCodec() throws IOException {
        assumeColumnarCodecEnabled();
        createIndex(
            INDEX,
            columnarSettings(randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR), true),
            "@timestamp",
            "type=date",
            FIELD,
            "type=text"
        );
        index(0, "\"the quick brown fox\"");
        index(1, "[\"first sentence\", \"second sentence\"]");
        indicesAdmin().prepareRefresh(INDEX).get();

        assertEquals(ColumnarFormat.NAME, docValuesFormat());
        // The terms are still indexed, and the values still come back whole from the column.
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery(FIELD, "quick")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery(FIELD, "sentence")), 1);
        assertEquals("the quick brown fox", client().prepareGet(INDEX, "0").get().getSourceAsMap().get(FIELD));
        assertEquals(List.of("first sentence", "second sentence"), client().prepareGet(INDEX, "1").get().getSourceAsMap().get(FIELD));
    }

    /**
     * The codec stores a document's values separately and puts them back together on the way out, so an array
     * with an inline null has to come back through {@code _source} exactly as it went in, position and all.
     */
    public void testArraysAndNullsRoundTripThroughSource() throws IOException {
        assumeColumnarCodecEnabled();
        createIndex(
            INDEX,
            columnarSettings(randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR), true),
            "@timestamp",
            "type=date",
            FIELD,
            "type=text"
        );
        // Each array as it goes in, beside what _source has to render it as; a lone value is not wrapped in a list.
        final List<Map.Entry<String, Object>> cases = List.of(
            Map.entry("[\"first\", null, \"third\"]", Arrays.asList("first", null, "third")),
            Map.entry("[null, \"only value\"]", Arrays.asList(null, "only value")),
            Map.entry("[\"solo\"]", "solo"),
            Map.entry("[\"\", null, \"\"]", Arrays.asList("", null, "")),
            Map.entry("[\"same\", \"same\"]", List.of("same", "same"))
        );
        for (int i = 0; i < cases.size(); i++) {
            index(i, cases.get(i).getKey());
        }
        indicesAdmin().prepareRefresh(INDEX).get();

        assertEquals(ColumnarFormat.NAME, docValuesFormat());
        for (int i = 0; i < cases.size(); i++) {
            final Map<String, Object> source = client().prepareGet(INDEX, Integer.toString(i)).get().getSourceAsMap();
            assertEquals(cases.get(i).getKey(), cases.get(i).getValue(), source.get(FIELD));
        }
    }

    private void index(int id, String json) {
        prepareIndex(INDEX).setId(Integer.toString(id))
            .setSource("{\"@timestamp\":\"2024-01-01T00:00:0" + id + "Z\",\"" + FIELD + "\":" + json + "}", XContentType.JSON)
            .get();
    }

    private void assumeColumnarCodecEnabled() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
    }

    private static Settings columnarSettings(IndexMode mode, boolean codecEnabled) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), mode)
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), codecEnabled)
            .build();
    }

    /** The doc-values format the field's values were actually written with. */
    private String docValuesFormat() throws IOException {
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            String format = null;
            boolean found = false;
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(FIELD);
                if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    if (found) {
                        assertEquals(
                            "leaves disagree on the doc-values format",
                            format,
                            fieldInfo.getAttribute("PerFieldDocValuesFormat.format")
                        );
                    }
                    format = fieldInfo.getAttribute("PerFieldDocValuesFormat.format");
                    found = true;
                }
            }
            assertTrue("expected a text doc-values field to assert on", found);
            return format;
        }
    }
}
