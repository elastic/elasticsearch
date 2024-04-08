/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.query;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
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
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InnerHitsIT extends ParentChildTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("5", script -> "5");
        }
    }

    public void testSimpleParentChild() throws Exception {
        assertAcked(
            prepareCreate("articles").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("article", "comment")
                    .endObject()
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .endObject()
                    .startObject("message")
                    .field("type", "text")
                    .field("fielddata", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("articles", "article", "p1", null, "title", "quick brown fox"));
        requests.add(createIndexRequest("articles", "comment", "c1", "p1", "message", "fox eat quick"));
        requests.add(createIndexRequest("articles", "comment", "c2", "p1", "message", "fox ate rabbit x y z"));
        requests.add(createIndexRequest("articles", "comment", "c3", "p1", "message", "rabbit got away"));
        requests.add(createIndexRequest("articles", "article", "p2", null, "title", "big gray elephant"));
        requests.add(createIndexRequest("articles", "comment", "c4", "p2", "message", "elephant captured"));
        requests.add(createIndexRequest("articles", "comment", "c5", "p2", "message", "mice squashed by elephant x"));
        requests.add(createIndexRequest("articles", "comment", "c6", "p2", "message", "elephant scared by mice x y"));
        indexRandom(true, requests);

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                hasChildQuery("comment", matchQuery("message", "fox"), ScoreMode.None).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("p1"));
                assertThat(response.getHits().getAt(0).getShard(), notNullValue());

                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getTotalHits().value, equalTo(2L));

                assertThat(innerHits.getAt(0).getId(), equalTo("c1"));
                assertThat(innerHits.getAt(1).getId(), equalTo("c2"));
            }
        );

        final boolean seqNoAndTerm = randomBoolean();
        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                hasChildQuery("comment", matchQuery("message", "elephant"), ScoreMode.None).innerHit(
                    new InnerHitBuilder().setSeqNoAndPrimaryTerm(seqNoAndTerm)
                )
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("p2"));

                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getTotalHits().value, equalTo(3L));

                assertThat(innerHits.getAt(0).getId(), equalTo("c4"));
                assertThat(innerHits.getAt(1).getId(), equalTo("c5"));
                assertThat(innerHits.getAt(2).getId(), equalTo("c6"));

                if (seqNoAndTerm) {
                    assertThat(innerHits.getAt(0).getPrimaryTerm(), equalTo(1L));
                    assertThat(innerHits.getAt(1).getPrimaryTerm(), equalTo(1L));
                    assertThat(innerHits.getAt(2).getPrimaryTerm(), equalTo(1L));
                    assertThat(innerHits.getAt(0).getSeqNo(), greaterThanOrEqualTo(0L));
                    assertThat(innerHits.getAt(1).getSeqNo(), greaterThanOrEqualTo(0L));
                    assertThat(innerHits.getAt(2).getSeqNo(), greaterThanOrEqualTo(0L));
                } else {
                    assertThat(innerHits.getAt(0).getPrimaryTerm(), equalTo(UNASSIGNED_PRIMARY_TERM));
                    assertThat(innerHits.getAt(1).getPrimaryTerm(), equalTo(UNASSIGNED_PRIMARY_TERM));
                    assertThat(innerHits.getAt(2).getPrimaryTerm(), equalTo(UNASSIGNED_PRIMARY_TERM));
                    assertThat(innerHits.getAt(0).getSeqNo(), equalTo(UNASSIGNED_SEQ_NO));
                    assertThat(innerHits.getAt(1).getSeqNo(), equalTo(UNASSIGNED_SEQ_NO));
                    assertThat(innerHits.getAt(2).getSeqNo(), equalTo(UNASSIGNED_SEQ_NO));
                }
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                hasChildQuery("comment", matchQuery("message", "fox"), ScoreMode.None).innerHit(
                    new InnerHitBuilder().addFetchField("message")
                        .setHighlightBuilder(new HighlightBuilder().field("message"))
                        .setExplain(true)
                        .setSize(1)
                        .addScriptField("script", new Script(ScriptType.INLINE, MockScriptEngine.NAME, "5", Collections.emptyMap()))
                )
            ),
            response -> {
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getHits().length, equalTo(1));
                HighlightField highlightField = innerHits.getAt(0).getHighlightFields().get("message");
                assertThat(highlightField.fragments()[0].string(), equalTo("<em>fox</em> eat quick"));
                assertThat(innerHits.getAt(0).getExplanation().toString(), containsString("weight(message:fox"));
                assertThat(innerHits.getAt(0).getFields().get("message").getValue().toString(), equalTo("fox eat quick"));
                assertThat(innerHits.getAt(0).getFields().get("script").getValue().toString(), equalTo("5"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                hasChildQuery("comment", matchQuery("message", "fox"), ScoreMode.None).innerHit(
                    new InnerHitBuilder().addDocValueField("message").setSize(1)
                )
            ),
            response -> {
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getHits().length, equalTo(1));
                assertThat(innerHits.getAt(0).getFields().get("message").getValue().toString(), equalTo("eat"));
            }
        );
    }

    public void testRandomParentChild() throws Exception {
        assertAcked(
            prepareCreate("idx").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("parent", new String[] { "child1", "child2" })
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        int numDocs = scaledRandomIntBetween(5, 50);
        List<IndexRequestBuilder> requestBuilders = new ArrayList<>();

        int child1 = 0;
        int child2 = 0;
        int[] child1InnerObjects = new int[numDocs];
        int[] child2InnerObjects = new int[numDocs];
        for (int parent = 0; parent < numDocs; parent++) {
            String parentId = String.format(Locale.ENGLISH, "p_%03d", parent);
            requestBuilders.add(createIndexRequest("idx", "parent", parentId, null));

            int numChildDocs = child1InnerObjects[parent] = scaledRandomIntBetween(1, numDocs);
            int limit = child1 + numChildDocs;
            for (; child1 < limit; child1++) {
                requestBuilders.add(createIndexRequest("idx", "child1", String.format(Locale.ENGLISH, "c1_%04d", child1), parentId));
            }
            numChildDocs = child2InnerObjects[parent] = scaledRandomIntBetween(1, numDocs);
            limit = child2 + numChildDocs;
            for (; child2 < limit; child2++) {
                requestBuilders.add(createIndexRequest("idx", "child2", String.format(Locale.ENGLISH, "c2_%04d", child2), parentId));
            }
        }
        indexRandom(true, requestBuilders);

        int size = randomIntBetween(0, numDocs);
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        boolQuery.should(
            constantScoreQuery(
                hasChildQuery("child1", matchAllQuery(), ScoreMode.None).innerHit(
                    new InnerHitBuilder().setName("a").addSort(new FieldSortBuilder("id").order(SortOrder.ASC)).setSize(size)
                )
            )
        );
        boolQuery.should(
            constantScoreQuery(
                hasChildQuery("child2", matchAllQuery(), ScoreMode.None).innerHit(
                    new InnerHitBuilder().setName("b").addSort(new FieldSortBuilder("id").order(SortOrder.ASC)).setSize(size)
                )
            )
        );

        assertNoFailuresAndResponse(prepareSearch("idx").setSize(numDocs).addSort("id", SortOrder.ASC).setQuery(boolQuery), response -> {
            assertHitCount(response, numDocs);
            assertThat(response.getHits().getHits().length, equalTo(numDocs));

            int offset1 = 0;
            int offset2 = 0;
            for (int parent = 0; parent < numDocs; parent++) {
                SearchHit searchHit = response.getHits().getAt(parent);
                assertThat(searchHit.getId(), equalTo(String.format(Locale.ENGLISH, "p_%03d", parent)));
                assertThat(searchHit.getShard(), notNullValue());

                SearchHits inner = searchHit.getInnerHits().get("a");
                assertThat(inner.getTotalHits().value, equalTo((long) child1InnerObjects[parent]));
                for (int child = 0; child < child1InnerObjects[parent] && child < size; child++) {
                    SearchHit innerHit = inner.getAt(child);
                    String childId = String.format(Locale.ENGLISH, "c1_%04d", offset1 + child);
                    assertThat(innerHit.getId(), equalTo(childId));
                    assertThat(innerHit.getNestedIdentity(), nullValue());
                }
                offset1 += child1InnerObjects[parent];

                inner = searchHit.getInnerHits().get("b");
                assertThat(inner.getTotalHits().value, equalTo((long) child2InnerObjects[parent]));
                for (int child = 0; child < child2InnerObjects[parent] && child < size; child++) {
                    SearchHit innerHit = inner.getAt(child);
                    String childId = String.format(Locale.ENGLISH, "c2_%04d", offset2 + child);
                    assertThat(innerHit.getId(), equalTo(childId));
                    assertThat(innerHit.getNestedIdentity(), nullValue());
                }
                offset2 += child2InnerObjects[parent];
            }
        });
    }

    public void testInnerHitsOnHasParent() throws Exception {
        assertAcked(
            prepareCreate("stack").setMapping(
                addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "question", "answer"), "body", "text")
            )
        );
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(
            createIndexRequest(
                "stack",
                "question",
                "1",
                null,
                "body",
                "I'm using HTTPS + Basic authentication "
                    + "to protect a resource. How can I throttle authentication attempts to protect against brute force attacks?"
            )
        );
        requests.add(createIndexRequest("stack", "answer", "3", "1", "body", "install fail2ban and enable rules for apache"));
        requests.add(
            createIndexRequest(
                "stack",
                "question",
                "2",
                null,
                "body",
                "I have firewall rules set up and also denyhosts installed.\\ndo I also need to install fail2ban?"
            )
        );
        requests.add(
            createIndexRequest("stack", "answer", "4", "2", "body", "Denyhosts protects only ssh; Fail2Ban protects all daemons.")
        );
        indexRandom(true, requests);

        assertNoFailuresAndResponse(
            prepareSearch("stack").addSort("id", SortOrder.ASC)
                .setQuery(
                    boolQuery().must(matchQuery("body", "fail2ban"))
                        .must(hasParentQuery("question", matchAllQuery(), false).innerHit(new InnerHitBuilder()))
                ),
            response -> {
                assertHitCount(response, 2);

                SearchHit searchHit = response.getHits().getAt(0);
                assertThat(searchHit.getId(), equalTo("3"));
                assertThat(searchHit.getInnerHits().get("question").getTotalHits().value, equalTo(1L));
                assertThat(searchHit.getInnerHits().get("question").getAt(0).getId(), equalTo("1"));

                searchHit = response.getHits().getAt(1);
                assertThat(searchHit.getId(), equalTo("4"));
                assertThat(searchHit.getInnerHits().get("question").getTotalHits().value, equalTo(1L));
                assertThat(searchHit.getInnerHits().get("question").getAt(0).getId(), equalTo("2"));
            }
        );
    }

    public void testParentChildMultipleLayers() throws Exception {
        assertAcked(
            prepareCreate("articles").setMapping(
                addFieldMappings(
                    buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "article", "comment", "comment", "remark"),
                    "title",
                    "text",
                    "message",
                    "text"
                )
            )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("articles", "article", "1", null, "title", "quick brown fox"));
        requests.add(createIndexRequest("articles", "comment", "3", "1", "message", "fox eat quick"));
        requests.add(createIndexRequest("articles", "remark", "5", "3", "message", "good").setRouting("1"));
        requests.add(createIndexRequest("articles", "article", "2", null, "title", "big gray elephant"));
        requests.add(createIndexRequest("articles", "comment", "4", "2", "message", "elephant captured"));
        requests.add(createIndexRequest("articles", "remark", "6", "4", "message", "bad").setRouting("2"));
        indexRandom(true, requests);

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                hasChildQuery(
                    "comment",
                    hasChildQuery("remark", matchQuery("message", "good"), ScoreMode.None).innerHit(new InnerHitBuilder()),
                    ScoreMode.None
                ).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("1"));

                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getAt(0).getId(), equalTo("3"));

                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getAt(0).getId(), equalTo("5"));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch("articles").setQuery(
                hasChildQuery(
                    "comment",
                    hasChildQuery("remark", matchQuery("message", "bad"), ScoreMode.None).innerHit(new InnerHitBuilder()),
                    ScoreMode.None
                ).innerHit(new InnerHitBuilder())
            ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHit(response, 1, hasId("2"));

                assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getAt(0).getId(), equalTo("4"));

                innerHits = innerHits.getAt(0).getInnerHits().get("remark");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getAt(0).getId(), equalTo("6"));
            }
        );
    }

    public void testRoyals() throws Exception {
        assertAcked(
            prepareCreate("royals").setMapping(
                buildParentJoinFieldMappingFromSimplifiedDef(
                    "join_field",
                    true,
                    "king",
                    "prince",
                    "prince",
                    "duke",
                    "duke",
                    "earl",
                    "earl",
                    "baron"
                )
            )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("royals", "king", "king", null));
        requests.add(createIndexRequest("royals", "prince", "prince", "king"));
        requests.add(createIndexRequest("royals", "duke", "duke", "prince").setRouting("king"));
        requests.add(createIndexRequest("royals", "earl", "earl1", "duke").setRouting("king"));
        requests.add(createIndexRequest("royals", "earl", "earl2", "duke").setRouting("king"));
        requests.add(createIndexRequest("royals", "earl", "earl3", "duke").setRouting("king"));
        requests.add(createIndexRequest("royals", "earl", "earl4", "duke").setRouting("king"));
        requests.add(createIndexRequest("royals", "baron", "baron1", "earl1").setRouting("king"));
        requests.add(createIndexRequest("royals", "baron", "baron2", "earl2").setRouting("king"));
        requests.add(createIndexRequest("royals", "baron", "baron3", "earl3").setRouting("king"));
        requests.add(createIndexRequest("royals", "baron", "baron4", "earl4").setRouting("king"));
        indexRandom(true, requests);
        assertResponse(
            prepareSearch("royals").setQuery(
                boolQuery().filter(
                    hasParentQuery(
                        "prince",
                        hasParentQuery("king", matchAllQuery(), false).innerHit(new InnerHitBuilder().setName("kings")),
                        false
                    ).innerHit(new InnerHitBuilder().setName("princes"))
                )
                    .filter(
                        hasChildQuery(
                            "earl",
                            hasChildQuery("baron", matchAllQuery(), ScoreMode.None).innerHit(new InnerHitBuilder().setName("barons")),
                            ScoreMode.None
                        ).innerHit(
                            new InnerHitBuilder().addSort(SortBuilders.fieldSort("id").order(SortOrder.ASC)).setName("earls").setSize(4)
                        )
                    )
            ),
            response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getId(), equalTo("duke"));

                SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("earls");
                assertThat(innerHits.getTotalHits().value, equalTo(4L));
                assertThat(innerHits.getAt(0).getId(), equalTo("earl1"));
                assertThat(innerHits.getAt(1).getId(), equalTo("earl2"));
                assertThat(innerHits.getAt(2).getId(), equalTo("earl3"));
                assertThat(innerHits.getAt(3).getId(), equalTo("earl4"));

                SearchHits innerInnerHits = innerHits.getAt(0).getInnerHits().get("barons");
                assertThat(innerInnerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron1"));

                innerInnerHits = innerHits.getAt(1).getInnerHits().get("barons");
                assertThat(innerInnerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron2"));

                innerInnerHits = innerHits.getAt(2).getInnerHits().get("barons");
                assertThat(innerInnerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron3"));

                innerInnerHits = innerHits.getAt(3).getInnerHits().get("barons");
                assertThat(innerInnerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron4"));

                innerHits = response.getHits().getAt(0).getInnerHits().get("princes");
                assertThat(innerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerHits.getAt(0).getId(), equalTo("prince"));

                innerInnerHits = innerHits.getAt(0).getInnerHits().get("kings");
                assertThat(innerInnerHits.getTotalHits().value, equalTo(1L));
                assertThat(innerInnerHits.getAt(0).getId(), equalTo("king"));
            }
        );
    }

    public void testMatchesQueriesParentChildInnerHits() throws Exception {
        assertAcked(prepareCreate("index").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child")));
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("index", "parent", "1", null));
        requests.add(createIndexRequest("index", "child", "3", "1", "field", "value1"));
        requests.add(createIndexRequest("index", "child", "4", "1", "field", "value2"));
        requests.add(createIndexRequest("index", "parent", "2", null));
        requests.add(createIndexRequest("index", "child", "5", "2", "field", "value1"));
        indexRandom(true, requests);

        assertResponse(
            prepareSearch("index").setQuery(
                hasChildQuery("child", matchQuery("field", "value1").queryName("_name1"), ScoreMode.None).innerHit(new InnerHitBuilder())
            ).addSort("id", SortOrder.ASC),
            response -> {
                assertHitCount(response, 2);
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getInnerHits().get("child").getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries()[0], equalTo("_name1"));

                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(1).getInnerHits().get("child").getTotalHits().value, equalTo(1L));
                assertThat(response.getHits().getAt(1).getInnerHits().get("child").getAt(0).getMatchedQueries().length, equalTo(1));
                assertThat(response.getHits().getAt(1).getInnerHits().get("child").getAt(0).getMatchedQueries()[0], equalTo("_name1"));
            }
        );

        QueryBuilder query = hasChildQuery("child", matchQuery("field", "value2").queryName("_name2"), ScoreMode.None).innerHit(
            new InnerHitBuilder()
        );
        assertResponse(prepareSearch("index").setQuery(query).addSort("id", SortOrder.ASC), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getInnerHits().get("child").getTotalHits().value, equalTo(1L));
            assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries()[0], equalTo("_name2"));
        });
    }

    public void testUseMaxDocInsteadOfSize() throws Exception {
        assertAcked(
            prepareCreate("index1").setMapping(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"))
        );
        updateIndexSettings(
            Settings.builder().put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), ArrayUtil.MAX_ARRAY_LENGTH),
            "index1"
        );
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("index1", "parent", "1", null));
        requests.add(createIndexRequest("index1", "child", "2", "1", "field", "value1"));
        indexRandom(true, requests);

        QueryBuilder query = hasChildQuery("child", matchQuery("field", "value1"), ScoreMode.None).innerHit(
            new InnerHitBuilder().setSize(ArrayUtil.MAX_ARRAY_LENGTH - 1)
        );
        assertHitCountAndNoFailures(prepareSearch("index1").setQuery(query), 1L);
    }

    public void testNestedInnerHitWrappedInParentChildInnerhit() {
        assertAcked(
            prepareCreate("test").setMapping(
                addFieldMappings(
                    buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent_type", "child_type"),
                    "nested_type",
                    "nested"
                )
            )
        );
        createIndexRequest("test", "parent_type", "1", null, "key", "value").get();
        createIndexRequest("test", "child_type", "2", "1", "nested_type", Collections.singletonMap("key", "value")).get();
        refresh();
        assertResponse(
            prepareSearch("test").setQuery(
                boolQuery().must(matchQuery("key", "value"))
                    .should(
                        hasChildQuery(
                            "child_type",
                            nestedQuery("nested_type", matchAllQuery(), ScoreMode.None).innerHit(new InnerHitBuilder()),
                            ScoreMode.None
                        ).innerHit(new InnerHitBuilder())
                    )
            ),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().getAt(0);
                String parentId = (String) extractValue(
                    "join_field.parent",
                    hit.getInnerHits().get("child_type").getAt(0).getSourceAsMap()
                );
                assertThat(parentId, equalTo("1"));
                assertThat(
                    hit.getInnerHits().get("child_type").getAt(0).getInnerHits().get("nested_type").getAt(0).field("_parent"),
                    nullValue()
                );
            }
        );
    }

    public void testInnerHitsWithIgnoreUnmapped() {
        assertAcked(
            prepareCreate("index1").setMapping(
                addFieldMappings(
                    buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent_type", "child_type"),
                    "nested_type",
                    "nested"
                )
            )
        );
        assertAcked(prepareCreate("index2"));
        createIndexRequest("index1", "parent_type", "1", null, "nested_type", Collections.singletonMap("key", "value")).get();
        createIndexRequest("index1", "child_type", "2", "1").get();
        prepareIndex("index2").setId("3").setSource("key", "value").get();
        refresh();
        assertSearchHitsWithoutFailures(
            prepareSearch("index1", "index2").setQuery(
                boolQuery().should(
                    hasChildQuery("child_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                        .innerHit(new InnerHitBuilder().setIgnoreUnmapped(true))
                ).should(termQuery("key", "value"))
            ),
            "1",
            "3"
        );
    }

    public void testTooHighResultWindow() {
        assertAcked(
            prepareCreate("index1").setMapping(
                addFieldMappings(
                    buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent_type", "child_type"),
                    "nested_type",
                    "nested"
                )
            )
        );
        createIndexRequest("index1", "parent_type", "1", null, "nested_type", Collections.singletonMap("key", "value")).get();
        createIndexRequest("index1", "child_type", "2", "1").get();
        refresh();
        assertHitCountAndNoFailures(
            prepareSearch("index1").setQuery(
                hasChildQuery("child_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                    .innerHit(new InnerHitBuilder().setFrom(50).setSize(10).setName("_name"))
            ),
            1
        );

        Exception e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> prepareSearch("index1").setQuery(
                hasChildQuery("child_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                    .innerHit(new InnerHitBuilder().setFrom(100).setSize(10).setName("_name"))
            ).get()
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("the inner hit definition's [_name]'s from + size must be less than or equal to: [100] but was [110]")
        );
        e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> prepareSearch("index1").setQuery(
                hasChildQuery("child_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                    .innerHit(new InnerHitBuilder().setFrom(10).setSize(100).setName("_name"))
            ).get()
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("the inner hit definition's [_name]'s from + size must be less than or equal to: [100] but was [110]")
        );
        updateIndexSettings(Settings.builder().put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 110), "index1");
        assertNoFailures(
            prepareSearch("index1").setQuery(
                hasChildQuery("child_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                    .innerHit(new InnerHitBuilder().setFrom(100).setSize(10).setName("_name"))
            )
        );
        assertNoFailures(
            prepareSearch("index1").setQuery(
                hasChildQuery("child_type", matchAllQuery(), ScoreMode.None).ignoreUnmapped(true)
                    .innerHit(new InnerHitBuilder().setFrom(10).setSize(100).setName("_name"))
            )
        );
    }
}
