/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;
import org.junit.After;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

public class PatternTextNestedObjectTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MapperExtrasPlugin.class, XPackPlugin.class, LogsDBPlugin.class);
    }

    private static final String INDEX = "test_index";

    private static final String SHORT_MESSAGE = "some message 123 ";
    private static final String LONG_MESSAGE = SHORT_MESSAGE.repeat(((32 * 1024) / SHORT_MESSAGE.length()) + 1);

    private static final Settings SYNTHETIC_SETTING = Settings.builder()
        .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
        .build();

    @After
    public void cleanup() {
        assertAcked(admin().indices().prepareDelete(INDEX).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get());
    }

    public void testInObject() {
        String mapping = """
                {
                  "properties": {
                    "obj": {
                      "type": "object",
                      "properties": {
                        "field_pattern_text": {
                          "type": "pattern_text"
                        }
                      }
                    }
                  }
                }
            """;

        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(SYNTHETIC_SETTING).setMapping(mapping);
        createIndex(INDEX, createRequest);

        String message = randomBoolean() ? SHORT_MESSAGE : LONG_MESSAGE;
        indexDoc("""
            {
                "obj.field_pattern_text": "%"
            }
            """.replace("%", message));

        var actualMappings = getMapping();
        assertEquals("pattern_text", ObjectPath.eval("properties.obj.properties.field_pattern_text.type", actualMappings));

        var query = randomBoolean()
            ? QueryBuilders.matchQuery("obj.field_pattern_text", SHORT_MESSAGE)
            : QueryBuilders.matchPhraseQuery("obj.field_pattern_text", SHORT_MESSAGE);
        var searchRequest = client().prepareSearch(INDEX).setQuery(query).setSize(1);

        assertNoFailuresAndResponse(searchRequest, searchResponse -> {
            assertEquals(Set.of(message), getFieldValuesFromSource(searchResponse, "obj.field_pattern_text"));
        });
    }

    public void testInObjectInObject() {
        String mapping = """
                {
                  "properties": {
                    "obj": {
                      "type": "object",
                      "properties": {
                        "inner": {
                            "type": "object",
                            "properties": {
                                "field_pattern_text": {
                                  "type": "pattern_text"
                                }
                            }
                        }
                      }
                    }
                  }
                }
            """;

        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(SYNTHETIC_SETTING).setMapping(mapping);
        createIndex(INDEX, createRequest);

        String message = randomBoolean() ? SHORT_MESSAGE : LONG_MESSAGE;
        indexDoc("""
            {
                "obj.inner.field_pattern_text": "%"
            }
            """.replace("%", message));

        var actualMappings = getMapping();
        assertEquals("pattern_text", ObjectPath.eval("properties.obj.properties.inner.properties.field_pattern_text.type", actualMappings));

        var query = randomBoolean()
            ? QueryBuilders.matchQuery("obj.inner.field_pattern_text", SHORT_MESSAGE)
            : QueryBuilders.matchPhraseQuery("obj.inner.field_pattern_text", SHORT_MESSAGE);
        var searchRequest = client().prepareSearch(INDEX).setQuery(query).setSize(1);

        assertNoFailuresAndResponse(searchRequest, searchResponse -> {
            assertEquals(Set.of(message), getFieldValuesFromSource(searchResponse, "obj.inner.field_pattern_text"));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/134830")
    public void testInNested() {
        String mapping = """
                {
                  "properties": {
                    "obj": {
                      "type": "nested",
                      "properties": {
                        "field_pattern_text": {
                          "type": "pattern_text"
                        }
                      }
                    }
                  }
                }
            """;

        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(SYNTHETIC_SETTING).setMapping(mapping);
        createIndex(INDEX, createRequest);

        String message = randomBoolean() ? SHORT_MESSAGE : LONG_MESSAGE;
        indexDoc("""
            {
                "obj.field_pattern_text": "%"
            }
            """.replace("%", message));

        var actualMappings = getMapping();
        assertEquals("pattern_text", ObjectPath.eval("properties.obj.properties.field_pattern_text.type", actualMappings));

        var innerQuery = randomBoolean()
            ? QueryBuilders.matchQuery("obj.field_pattern_text", SHORT_MESSAGE)
            : QueryBuilders.matchPhraseQuery("obj.field_pattern_text", SHORT_MESSAGE);
        var query = QueryBuilders.nestedQuery("obj", innerQuery, ScoreMode.Avg);
        var searchRequest = client().prepareSearch(INDEX).setQuery(query).setSize(1);

        assertNoFailuresAndResponse(searchRequest, searchResponse -> {
            assertEquals(Set.of(message), getFieldValuesFromSource(searchResponse, "obj.field_pattern_text"));
        });
    }

    public void testInPassthrough() {
        String mapping = """
                {
                  "properties": {
                    "obj": {
                      "type": "passthrough",
                      "priority": 1,
                      "properties": {
                        "field_pattern_text": {
                          "type": "pattern_text"
                        }
                      }
                    }
                  }
                }
            """;

        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(SYNTHETIC_SETTING).setMapping(mapping);
        createIndex(INDEX, createRequest);

        String message = randomBoolean() ? SHORT_MESSAGE : LONG_MESSAGE;
        indexDoc("""
            {
                "obj.field_pattern_text": "%"
            }
            """.replace("%", message));

        var actualMappings = getMapping();
        assertEquals("pattern_text", ObjectPath.eval("properties.obj.properties.field_pattern_text.type", actualMappings));

        var query = randomBoolean()
            ? QueryBuilders.matchQuery("field_pattern_text", SHORT_MESSAGE)
            : QueryBuilders.matchPhraseQuery("field_pattern_text", SHORT_MESSAGE);
        var searchRequest = client().prepareSearch(INDEX).setQuery(query).setSize(1);

        assertNoFailuresAndResponse(searchRequest, searchResponse -> {
            assertEquals(Set.of(message), getFieldValuesFromSource(searchResponse, "obj.field_pattern_text"));
        });
    }

    static Set<Object> getFieldValuesFromSource(SearchResponse response, String path) {
        var values = new HashSet<>();
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            var sourceAsMap = hit.getSourceAsMap();
            Object value = ObjectPath.eval(path, sourceAsMap);
            values.add(value);
        }
        return values;
    }

    Map<String, Object> getMapping() {
        return indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, INDEX).get().mappings().get(INDEX).sourceAsMap();
    }

    private void indexDoc(String doc) {
        BulkRequest bulkRequest = new BulkRequest();
        var indexRequest = new IndexRequest(INDEX).opType(DocWriteRequest.OpType.CREATE).source(doc, XContentType.JSON);
        bulkRequest.add(indexRequest);
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        safeGet(indicesAdmin().refresh(new RefreshRequest(INDEX).indicesOptions(IndicesOptions.lenientExpandOpenHidden())));
    }
}
