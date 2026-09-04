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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
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
import java.util.Arrays;
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

        assertKeywordFieldUsesColumnarFormat(INDEX);

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

        assertKeywordFieldUsesColumnarFormat(INDEX);

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
        // Each array as it goes in, beside what _source has to render it as; a lone value is not wrapped in a list.
        final List<Map.Entry<String, Object>> cases = List.of(
            Map.entry("[\"red\", null, \"blue\"]", Arrays.asList("red", null, "blue")),
            Map.entry("[null, \"green\"]", Arrays.asList(null, "green")),
            Map.entry("[\"solo\"]", "solo"),
            Map.entry("[\"\", null, \"\"]", Arrays.asList("", null, "")),
            Map.entry("[\"dup\", \"dup\"]", List.of("dup", "dup"))
        );
        for (int i = 0; i < cases.size(); i++) {
            prepareIndex(INDEX).setId(Integer.toString(i))
                .setSource("{\"@timestamp\":\"2024-01-01T00:00:0" + i + "Z\",\"kw\":" + cases.get(i).getKey() + "}", XContentType.JSON)
                .get();
        }
        indicesAdmin().prepareRefresh(INDEX).get();

        assertKeywordFieldUsesColumnarFormat(INDEX);

        for (int i = 0; i < cases.size(); i++) {
            final Map<String, Object> source = client().prepareGet(INDEX, Integer.toString(i)).get().getSourceAsMap();
            assertEquals(cases.get(i).getKey(), cases.get(i).getValue(), source.get("kw"));
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

        // Without these the two indices agreeing would prove nothing if neither of them reached the codec.
        assertKeywordFieldUsesColumnarFormat(withCodec);
        assertKeywordFieldAvoidsColumnarFormat(withoutCodec);

        for (int i = 0; i < arrays.size(); i++) {
            final Map<String, Object> codec = client().prepareGet(withCodec, Integer.toString(i)).get().getSourceAsMap();
            final Map<String, Object> plain = client().prepareGet(withoutCodec, Integer.toString(i)).get().getSourceAsMap();
            assertEquals(arrays.get(i), plain.get("kw"), codec.get("kw"));
            assertEquals(arrays.get(i) + " field presence", plain.containsKey("kw"), codec.containsKey("kw"));
        }
    }

    /**
     * A {@code multi_value: false} field is stored by the codec like any other, but it records no {@code .offsets} sidecar and so is not
     * array-ordered. It still writes a payload, so every reader of it has to decode one — this pins that against the same field with the
     * codec off.
     */
    public void testSingleValuedFieldRendersAsItDoesWithoutTheCodec() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final String mapping = """
            {"properties":{"@timestamp":{"type":"date"},"kw":{"type":"keyword","doc_values":{"multi_value":false}}}}""";
        final List<String> values = List.of("\"solo\"", "\"\"", "null");

        final String withCodec = INDEX + "-sv-codec";
        final String withoutCodec = INDEX + "-sv-no-codec";
        for (boolean codecEnabled : new boolean[] { true, false }) {
            final String index = codecEnabled ? withCodec : withoutCodec;
            indicesAdmin().prepareCreate(index).setSettings(columnarSettings(mode, codecEnabled)).setMapping(mapping).get();
            final BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < values.size(); i++) {
                bulk.add(
                    prepareIndex(index).setId(Integer.toString(i))
                        .setSource("{\"@timestamp\":\"2024-01-01T00:00:0" + i + "Z\",\"kw\":" + values.get(i) + "}", XContentType.JSON)
                );
            }
            final var response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
            indicesAdmin().prepareRefresh(index).get();
        }

        // Without these the two indices agreeing would prove nothing if neither of them reached the codec.
        assertKeywordFieldUsesColumnarFormat(withCodec);
        assertKeywordFieldAvoidsColumnarFormat(withoutCodec);

        for (int i = 0; i < values.size(); i++) {
            final Map<String, Object> codec = client().prepareGet(withCodec, Integer.toString(i)).get().getSourceAsMap();
            final Map<String, Object> plain = client().prepareGet(withoutCodec, Integer.toString(i)).get().getSourceAsMap();
            assertEquals(values.get(i), plain.get("kw"), codec.get("kw"));
        }
    }

    /**
     * With no inverted index to fall back on, the term-family queries scan the binary doc values themselves. The columnar payload is not
     * one of the encodings those queries are told about up front — {@code KeywordFieldType} hands them
     * {@code usesArrayOrderInlineNull()}, which is false for it — so they have to recognise it per segment from the field's own
     * attributes. This pins that, including the empty-term case, which rewrites to a length query and has to recognise the payload again.
     */
    public void testDocValuesOnlyQueriesReadTheColumnarPayload() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());

        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);
        final String mapping = """
            {"properties":{"@timestamp":{"type":"date"},"kw":{"type":"keyword","index":false}}}""";
        indicesAdmin().prepareCreate(INDEX).setSettings(columnarSettings(mode)).setMapping(mapping).get();

        final List<String> docs = List.of("[\"red\", null, \"blue\"]", "[\"green\"]", "[\"\"]", "[null]", "[]");
        for (int i = 0; i < docs.size(); i++) {
            prepareIndex(INDEX).setId(Integer.toString(i))
                .setSource("{\"@timestamp\":\"2024-01-01T00:00:0" + i + "Z\",\"kw\":" + docs.get(i) + "}", XContentType.JSON)
                .get();
        }
        indicesAdmin().prepareRefresh(INDEX).get();

        assertKeywordFieldUsesColumnarFormat(INDEX);

        // A value in a multi-valued document, one in a single-valued document, and one that is not there at all.
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "red")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "blue")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "green")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "yellow")), 0);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termsQuery("kw", "red", "green")), 2);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.prefixQuery("kw", "bl")), 1);
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.wildcardQuery("kw", "gr*n")), 1);
        // The empty string is a real value here; a null slot and an empty array are not, and must not match it.
        assertHitCount(client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", "")), 1);
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

    private void assertKeywordFieldUsesColumnarFormat(String index) throws IOException {
        assertEquals("doc-values format of [kw] in [" + index + "]", ColumnarFormat.NAME, keywordDocValuesFormat(index));
    }

    private void assertKeywordFieldAvoidsColumnarFormat(String index) throws IOException {
        assertNotEquals("doc-values format of [kw] in [" + index + "]", ColumnarFormat.NAME, keywordDocValuesFormat(index));
    }

    /** The doc-values format the {@code kw} field's values were written with in {@code index}. */
    private String keywordDocValuesFormat(String index) throws IOException {
        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            String format = null;
            boolean found = false;
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo("kw");
                if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    final String leafFormat = fieldInfo.getAttribute("PerFieldDocValuesFormat.format");
                    if (found) {
                        assertEquals("leaves of [" + index + "] disagree on the doc-values format", format, leafFormat);
                    }
                    format = leafFormat;
                    found = true;
                }
            }
            assertTrue("expected a keyword doc-values field in [" + index + "] to assert on", found);
            return format;
        }
    }
}
