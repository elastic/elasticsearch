/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InnerHitsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("5", script -> "5");
        }
    }

    public void testSimpleNested() throws Exception {
        assertAcked(
            prepareCreate("articles").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("comments")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("message")
                    .field("type", "text")
                    .field("fielddata", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(
            prepareIndex("articles").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "quick brown fox")
                        .startArray("comments")
                        .startObject()
                        .field("message", "fox eat quick")
                        .endObject()
                        .startObject()
                        .field("message", "fox ate rabbit x y z")
                        .endObject()
                        .startObject()
                        .field("message", "rabbit got away")
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        requests.add(
            prepareIndex("articles").setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "big gray elephant")
                        .startArray("comments")
                        .startObject()
                        .field("message", "elephant captured")
                        .endObject()
                        .startObject()
                        .field("message", "mice squashed by elephant x")
                        .endObject()
                        .startObject()
                        .field("message", "elephant scared by mice x y")
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        indexRandom(true, requests);

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.Avg).innerHit(new InnerHitBuilder("comment"))
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("1"));
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getTotalHits().value, equalTo(2L));
                assertThat(innerHits.getHits().length, equalTo(2));
                assertThat(innerHits.getAt(0).getId(), equalTo("1"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(innerHits.getAt(1).getId(), equalTo("1"));
                assertThat(innerHits.getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(1).getNestedIdentity().getOffset(), equalTo(1));
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments", matchQuery("comments.message", "elephant"), ScoreMode.Avg).innerHit(new InnerHitBuilder("comment"))
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("2"));
                assertThat(response.getHits().getAt(0).getShard(), notNullValue());
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getTotalHits().value, equalTo(3L));
                assertThat(innerHits.getHits().length, equalTo(3));
                assertThat(innerHits.getAt(0).getId(), equalTo("2"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(innerHits.getAt(1).getId(), equalTo("2"));
                assertThat(innerHits.getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(1).getNestedIdentity().getOffset(), equalTo(1));
                assertThat(innerHits.getAt(2).getId(), equalTo("2"));
                assertThat(innerHits.getAt(2).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(2).getNestedIdentity().getOffset(), equalTo(2));
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setHighlightBuilder(new HighlightBuilder().field("comments.message"))
                        .setExplain(true)
                        .addFetchField("comments.mes*")
                        .addScriptField("script", new Script(ScriptType.INLINE, MockScriptEngine.NAME, "5", Collections.emptyMap()))
                        .setSize(1)
                )
            ),
            response -> {
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                assertThat(innerHits.getTotalHits().value, equalTo(2L));
                assertThat(innerHits.getHits().length, equalTo(1));
                HighlightField highlightField = innerHits.getAt(0).getHighlightFields().get("comments.message");
                assertThat(highlightField.fragments()[0].string(), equalTo("<em>fox</em> eat quick"));
                assertThat(innerHits.getAt(0).getExplanation().toString(), containsString("weight(comments.message:fox in"));
                assertThat(
                    innerHits.getAt(0).getFields().get("comments").getValue(),
                    equalTo(Collections.singletonMap("message", Collections.singletonList("fox eat quick")))
                );
                assertThat(innerHits.getAt(0).getFields().get("script").getValue().toString(), equalTo("5"));
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().addDocValueField("comments.mes*").setSize(1)
                )
            ),
            response -> {
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getFields().get("comments.message").getValue().toString(), equalTo("eat"));
            }
        );
    }

    public void testRandomNested() throws Exception {
        assertAcked(prepareCreate("idx").setMapping("field1", "type=nested", "field2", "type=nested"));
        int numDocs = scaledRandomIntBetween(25, 100);
        List<IndexRequestBuilder> requestBuilders = new ArrayList<>();

        int[] field1InnerObjects = new int[numDocs];
        int[] field2InnerObjects = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            int numInnerObjects = field1InnerObjects[i] = scaledRandomIntBetween(1, numDocs);
            XContentBuilder source = jsonBuilder().startObject().field("foo", i).startArray("field1");
            for (int j = 0; j < numInnerObjects; j++) {
                source.startObject().field("x", "y").endObject();
            }
            numInnerObjects = field2InnerObjects[i] = scaledRandomIntBetween(1, numDocs);
            source.endArray().startArray("field2");
            for (int j = 0; j < numInnerObjects; j++) {
                source.startObject().field("x", "y").endObject();
            }
            source.endArray().endObject();
            requestBuilders.add(prepareIndex("idx").setId(Integer.toString(i)).setSource(source));
        }
        indexRandom(true, requestBuilders);

        int size = randomIntBetween(0, numDocs);
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        boolQuery.should(
            nestedQuery("field1", matchAllQuery(), ScoreMode.Avg).innerHit(
                new InnerHitBuilder("a").setSize(size).addSort(new FieldSortBuilder("_doc").order(SortOrder.ASC))
            )
        );
        boolQuery.should(
            nestedQuery("field2", matchAllQuery(), ScoreMode.Avg).innerHit(
                new InnerHitBuilder("b").addSort(new FieldSortBuilder("_doc").order(SortOrder.ASC)).setSize(size)
            )
        );
        assertNoFailuresAndResponse(prepareSearch("idx").setQuery(boolQuery).setSize(numDocs).addSort("foo", SortOrder.ASC), response -> {
            assertHitCount(response, numDocs);
            assertThat(response.getHits().getHits().length, equalTo(numDocs));
            for (int i = 0; i < numDocs; i++) {
                SearchHit searchHit = response.getHits().getAt(i);
                assertThat(searchHit.getShard(), notNullValue());
                SearchHits inner = searchHit.getInnerHits().get("a");
                assertThat(inner.getTotalHits().value, equalTo((long) field1InnerObjects[i]));
                for (int j = 0; j < field1InnerObjects[i] && j < size; j++) {
                    SearchHit innerHit = inner.getAt(j);
                    assertThat(innerHit.getNestedIdentity().getField().string(), equalTo("field1"));
                    assertThat(innerHit.getNestedIdentity().getOffset(), equalTo(j));
                    assertThat(innerHit.getNestedIdentity().getChild(), nullValue());
                }

                inner = searchHit.getInnerHits().get("b");
                assertThat(inner.getTotalHits().value, equalTo((long) field2InnerObjects[i]));
                for (int j = 0; j < field2InnerObjects[i] && j < size; j++) {
                    SearchHit innerHit = inner.getAt(j);
                    assertThat(innerHit.getNestedIdentity().getField().string(), equalTo("field2"));
                    assertThat(innerHit.getNestedIdentity().getOffset(), equalTo(j));
                    assertThat(innerHit.getNestedIdentity().getChild(), nullValue());
                }
            }
        });
    }

    public void testNestedMultipleLayers() throws Exception {
        assertAcked(
            prepareCreate("articles").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("comments")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("message")
                    .field("type", "text")
                    .endObject()
                    .startObject("remarks")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("message")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(
            prepareIndex("articles").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "quick brown fox")
                        .startArray("comments")
                        .startObject()
                        .field("message", "fox eat quick")
                        .startArray("remarks")
                        .startObject()
                        .field("message", "good")
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .field("message", "hippo is hungry")
                        .startArray("remarks")
                        .startObject()
                        .field("message", "neutral")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        requests.add(
            prepareIndex("articles").setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "big gray elephant")
                        .startArray("comments")
                        .startObject()
                        .field("message", "elephant captured")
                        .startArray("remarks")
                        .startObject()
                        .field("message", "bad")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        indexRandom(true, requests);

        // Check we can load the first doubly-nested document.
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery(
                    "comments",
                    nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "good"), ScoreMode.Avg).innerHit(
                        new InnerHitBuilder("remark")
                    ),
                    ScoreMode.Avg
                ).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("1"));
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("1"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("1"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));
            }
        );
        // Check we can load the second doubly-nested document.
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery(
                    "comments",
                    nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "neutral"), ScoreMode.Avg).innerHit(
                        new InnerHitBuilder("remark")
                    ),
                    ScoreMode.Avg
                ).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("1"));
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("1"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(1));
                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("1"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(1));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));
            }
        );
        // Directly refer to the second level:
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "bad"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder()
                )
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("2"));
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments.remarks");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("2"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery(
                    "comments",
                    nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "bad"), ScoreMode.Avg).innerHit(
                        new InnerHitBuilder("remark")
                    ),
                    ScoreMode.Avg
                ).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("2"));
                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("2"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getId(), equalTo("2"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
                assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));
            }
        );
        // Check that inner hits contain _source even when it's disabled on the parent request.
        assertNoFailuresAndResponse(
            prepareSearch("articles").setFetchSource(false)
                .setQuery(
                    nestedQuery(
                        "comments",
                        nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "good"), ScoreMode.Avg).innerHit(
                            new InnerHitBuilder("remark")
                        ),
                        ScoreMode.Avg
                    ).innerHit(new InnerHitBuilder())
                ),
            response -> {
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertNotNull(innerHits.getAt(0).getSourceAsMap());
                assertFalse(innerHits.getAt(0).getSourceAsMap().isEmpty());
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery(
                    "comments",
                    nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "good"), ScoreMode.Avg).innerHit(
                        new InnerHitBuilder("remark")
                    ),
                    ScoreMode.Avg
                ).innerHit(new InnerHitBuilder().setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE))
            ),
            response -> {
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertNotNull(innerHits.getAt(0).getSourceAsMap());
                assertFalse(innerHits.getAt(0).getSourceAsMap().isEmpty());
            }
        );
    }

    // Issue #9723
    public void testNestedDefinedAsObject() throws Exception {
        assertAcked(prepareCreate("articles").setMapping("comments", "type=nested", "title", "type=text"));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(
            prepareIndex("articles").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "quick brown fox")
                        .startObject("comments")
                        .field("message", "fox eat quick")
                        .endObject()
                        .endObject()
                )
        );
        indexRandom(true, requests);

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.Avg).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getId(), equalTo("1"));
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getField().string(),
                    equalTo("comments")
                );
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getChild(), nullValue());
            }
        );
    }

    public void testInnerHitsWithObjectFieldThatHasANestedField() throws Exception {
        assertAcked(
            prepareCreate("articles")
                // number_of_shards = 1, because then we catch the expected exception in the same way.
                // (See expectThrows(...) below)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("comments")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("messages")
                        .field("type", "nested")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(
            prepareIndex("articles").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "quick brown fox")
                        .startArray("comments")
                        .startObject()
                        .startArray("messages")
                        .startObject()
                        .field("message", "fox eat quick")
                        .endObject()
                        .startObject()
                        .field("message", "bear eat quick")
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .startArray("messages")
                        .startObject()
                        .field("message", "no fox")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        indexRandom(true, requests);

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments.messages", matchQuery("comments.messages.message", "fox"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFetchSourceContext(FetchSourceContext.FETCH_SOURCE)
                )
            ),
            response -> {
                assertHitCount(response, 1);
                SearchHit parent = response.getHits().getAt(0);
                assertThat(parent.getId(), equalTo("1"));
                SearchHits inner = parent.getInnerHits().get("comments.messages");
                assertThat(inner.getTotalHits().value, equalTo(2L));
                assertThat(inner.getAt(0).getSourceAsString(), equalTo("{\"message\":\"no fox\"}"));
                assertThat(inner.getAt(1).getSourceAsString(), equalTo("{\"message\":\"fox eat quick\"}"));
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments.messages", matchQuery("comments.messages.message", "fox"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
                )
            ),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().getAt(0);
                assertThat(hit.getId(), equalTo("1"));
                SearchHits messages = hit.getInnerHits().get("comments.messages");
                assertThat(messages.getTotalHits().value, equalTo(2L));
                assertThat(messages.getAt(0).getId(), equalTo("1"));
                assertThat(messages.getAt(0).getNestedIdentity().getField().string(), equalTo("comments.messages"));
                assertThat(messages.getAt(0).getNestedIdentity().getOffset(), equalTo(2));
                assertThat(messages.getAt(0).getNestedIdentity().getChild(), nullValue());
                assertThat(messages.getAt(1).getId(), equalTo("1"));
                assertThat(messages.getAt(1).getNestedIdentity().getField().string(), equalTo("comments.messages"));
                assertThat(messages.getAt(1).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(messages.getAt(1).getNestedIdentity().getChild(), nullValue());
            }
        );
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments.messages", matchQuery("comments.messages.message", "bear"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
                )
            ),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().getAt(0);
                assertThat(hit.getId(), equalTo("1"));
                SearchHits messages = hit.getInnerHits().get("comments.messages");
                assertThat(messages.getTotalHits().value, equalTo(1L));
                assertThat(messages.getAt(0).getId(), equalTo("1"));
                assertThat(messages.getAt(0).getNestedIdentity().getField().string(), equalTo("comments.messages"));
                assertThat(messages.getAt(0).getNestedIdentity().getOffset(), equalTo(1));
                assertThat(messages.getAt(0).getNestedIdentity().getChild(), nullValue());
            }
        );
        // index the message in an object form instead of an array
        requests = new ArrayList<>();
        requests.add(
            prepareIndex("articles").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("title", "quick brown fox")
                        .startObject("comments")
                        .startObject("messages")
                        .field("message", "fox eat quick")
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        indexRandom(true, requests);
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                nestedQuery("comments.messages", matchQuery("comments.messages.message", "fox"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
                )
            ),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().getAt(0);
                assertThat(hit.getId(), equalTo("1"));
                SearchHits messages = hit.getInnerHits().get("comments.messages");
                assertThat(messages.getTotalHits().value, equalTo(1L));
                assertThat(messages.getAt(0).getId(), equalTo("1"));
                assertThat(messages.getAt(0).getNestedIdentity().getField().string(), equalTo("comments.messages"));
                assertThat(messages.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
                assertThat(messages.getAt(0).getNestedIdentity().getChild(), nullValue());
            }
        );
    }

    public void testMatchesQueriesNestedInnerHits() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("nested1")
            .field("type", "nested")
            .startObject("properties")
            .startObject("n_field1")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject("field1")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(builder));
        ensureGreen();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = randomIntBetween(2, 35);
        requests.add(
            prepareIndex("test").setId("0")
                .setSource(
                    jsonBuilder().startObject()
                        .field("field1", 0)
                        .startArray("nested1")
                        .startObject()
                        .field("n_field1", "n_value1_1")
                        .field("n_field2", "n_value2_1")
                        .endObject()
                        .startObject()
                        .field("n_field1", "n_value1_2")
                        .field("n_field2", "n_value2_2")
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        requests.add(
            prepareIndex("test").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("field1", 1)
                        .startArray("nested1")
                        .startObject()
                        .field("n_field1", "n_value1_8")
                        .field("n_field2", "n_value2_5")
                        .endObject()
                        .startObject()
                        .field("n_field1", "n_value1_3")
                        .field("n_field2", "n_value2_1")
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );

        for (int i = 2; i < numDocs; i++) {
            requests.add(
                prepareIndex("test").setId(String.valueOf(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("field1", i)
                            .startArray("nested1")
                            .startObject()
                            .field("n_field1", "n_value1_8")
                            .field("n_field2", "n_value2_5")
                            .endObject()
                            .startObject()
                            .field("n_field1", "n_value1_2")
                            .field("n_field2", "n_value2_2")
                            .endObject()
                            .endArray()
                            .endObject()
                    )
            );
        }

        indexRandom(true, requests);
        waitForRelocation(ClusterHealthStatus.GREEN);

        QueryBuilder query = boolQuery().should(termQuery("nested1.n_field1", "n_value1_1").queryName("test1"))
            .should(termQuery("nested1.n_field1", "n_value1_3").queryName("test2"))
            .should(termQuery("nested1.n_field2", "n_value2_2").queryName("test3"));
        query = nestedQuery("nested1", query, ScoreMode.Avg).innerHit(
            new InnerHitBuilder().addSort(new FieldSortBuilder("nested1.n_field1").order(SortOrder.ASC))
        );
        assertNoFailuresAndResponse(prepareSearch("test").setQuery(query).setSize(numDocs).addSort("field1", SortOrder.ASC), response -> {
            assertAllSuccessful(response);
            assertThat(response.getHits().getTotalHits().value, equalTo((long) numDocs));
            assertThat(response.getHits().getAt(0).getId(), equalTo("0"));
            assertThat(response.getHits().getAt(0).getInnerHits().get("nested1").getTotalHits().value, equalTo(2L));
            assertThat(response.getHits().getAt(0).getInnerHits().get("nested1").getAt(0).getMatchedQueries().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getInnerHits().get("nested1").getAt(0).getMatchedQueries()[0], equalTo("test1"));
            assertThat(response.getHits().getAt(0).getInnerHits().get("nested1").getAt(1).getMatchedQueries().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getInnerHits().get("nested1").getAt(1).getMatchedQueries()[0], equalTo("test3"));

            assertThat(response.getHits().getAt(1).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(1).getInnerHits().get("nested1").getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getAt(1).getInnerHits().get("nested1").getAt(0).getMatchedQueries().length, equalTo(1));
            assertThat(response.getHits().getAt(1).getInnerHits().get("nested1").getAt(0).getMatchedQueries()[0], equalTo("test2"));

            for (int i = 2; i < numDocs; i++) {
                assertThat(response.getHits().getAt(i).getId(), equalTo(String.valueOf(i)));
                assertThat(response.getHits().getAt(i).getInnerHits().get("nested1").getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(i).getInnerHits().get("nested1").getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(i).getInnerHits().get("nested1").getAt(0).getMatchedQueries()[0], equalTo("test3"));
            }
        });
    }

    public void testNestedSource() throws Exception {
        assertAcked(prepareCreate("index1").setMapping("comments", "type=nested"));
        prepareIndex("index1").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("message", "quick brown fox")
                    .startArray("comments")
                    .startObject()
                    .field("message", "fox eat quick")
                    .field("x", "y")
                    .endObject()
                    .startObject()
                    .field("message", "fox ate rabbit x y z")
                    .field("x", "y")
                    .endObject()
                    .startObject()
                    .field("message", "rabbit got away")
                    .field("x", "y")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();

        // the field name (comments.message) used for source filtering should be the same as when using that field for
        // other features (like in the query dsl or aggs) in order for consistency:
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.None).innerHit(
                    new InnerHitBuilder().setFetchSourceContext(FetchSourceContext.of(true, new String[] { "comments.message" }, null))
                )
            ),
            response -> {
                assertHitCount(response, 1);

                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().size(), equalTo(1));
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().get("message"),
                    equalTo("fox eat quick")
                );
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(1).getSourceAsMap().size(), equalTo(1));
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("comments").getAt(1).getSourceAsMap().get("message"),
                    equalTo("fox ate rabbit x y z")
                );
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.None).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);

                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits().value, equalTo(2L));
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().size(), equalTo(2));
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().get("message"),
                    equalTo("fox eat quick")
                );
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().size(), equalTo(2));
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("comments").getAt(1).getSourceAsMap().get("message"),
                    equalTo("fox ate rabbit x y z")
                );
            }
        );

        // Source filter on a field that does not exist inside the nested document and just check that we do not fail and
        // return an empty _source:
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                nestedQuery("comments", matchQuery("comments.message", "away"), ScoreMode.None).innerHit(
                    new InnerHitBuilder().setFetchSourceContext(
                        FetchSourceContext.of(true, new String[] { "comments.missing_field" }, null)
                    )
                )
            ),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().size(), equalTo(0));
            }
        );
        // Check that inner hits contain _source even when it's disabled on the root request.
        assertNoFailuresAndResponse(
            prepareSearch().setFetchSource(false)
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox"), ScoreMode.None).innerHit(new InnerHitBuilder())),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits().value, equalTo(2L));
                assertFalse(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getSourceAsMap().isEmpty());
            }
        );
    }

    public void testInnerHitsWithIgnoreUnmapped() throws Exception {
        assertAcked(prepareCreate("index1").setMapping("nested_type", "type=nested"));
        createIndex("index2");
        prepareIndex("index1").setId("1").setSource("nested_type", Collections.singletonMap("key", "value")).get();
        prepareIndex("index2").setId("3").setSource("key", "value").get();
        refresh();

        assertSearchHitsWithoutFailures(
            prepareSearch("index1", "index2").setQuery(
                boolQuery().should(
                    nestedQuery("nested_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                        .innerHit(new InnerHitBuilder().setIgnoreUnmapped(true))
                ).should(termQuery("key", "value"))
            ),
            "1",
            "3"
        );
    }

    public void testUseMaxDocInsteadOfSize() throws Exception {
        assertAcked(prepareCreate("index2").setMapping("nested", "type=nested"));
        updateIndexSettings(
            Settings.builder().put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), ArrayUtil.MAX_ARRAY_LENGTH),
            "index2"
        );
        prepareIndex("index2").setId("1")
            .setSource(
                jsonBuilder().startObject().startArray("nested").startObject().field("field", "value1").endObject().endArray().endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        QueryBuilder query = nestedQuery("nested", matchQuery("nested.field", "value1"), ScoreMode.Avg).innerHit(
            new InnerHitBuilder().setSize(ArrayUtil.MAX_ARRAY_LENGTH - 1)
        );
        assertHitCountAndNoFailures(prepareSearch("index2").setQuery(query), 1);
    }

    public void testTooHighResultWindow() throws Exception {
        assertAcked(prepareCreate("index2").setMapping("nested", "type=nested"));
        prepareIndex("index2").setId("1")
            .setSource(
                jsonBuilder().startObject().startArray("nested").startObject().field("field", "value1").endObject().endArray().endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        assertHitCountAndNoFailures(
            prepareSearch("index2").setQuery(
                nestedQuery("nested", matchQuery("nested.field", "value1"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFrom(50).setSize(10).setName("_name")
                )
            ),
            1
        );

        Exception e = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch("index2").setQuery(
                nestedQuery("nested", matchQuery("nested.field", "value1"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFrom(100).setSize(10).setName("_name")
                )
            )
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("the inner hit definition's [_name]'s from + size must be less than or equal to: [100] but was [110]")
        );
        e = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch("index2").setQuery(
                nestedQuery("nested", matchQuery("nested.field", "value1"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFrom(10).setSize(100).setName("_name")
                )
            )
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("the inner hit definition's [_name]'s from + size must be less than or equal to: [100] but was [110]")
        );

        updateIndexSettings(Settings.builder().put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 110), "index2");
        assertNoFailures(
            prepareSearch("index2").setQuery(
                nestedQuery("nested", matchQuery("nested.field", "value1"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFrom(100).setSize(10).setName("_name")
                )
            )
        );
        assertNoFailures(
            prepareSearch("index2").setQuery(
                nestedQuery("nested", matchQuery("nested.field", "value1"), ScoreMode.Avg).innerHit(
                    new InnerHitBuilder().setFrom(10).setSize(100).setName("_name")
                )
            )
        );
    }

}
