/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.innerhits;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 */
public class InnerHitsIT extends ESIntegTestCase {

    @Test
    public void testSimpleNested() throws Exception {
        assertAcked(prepareCreate("articles").addMapping("article", jsonBuilder().startObject().startObject("article").startObject("properties")
                .startObject("comments")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("message")
                            .field("type", "string")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("title")
                    .field("type", "string")
                .endObject()
                .endObject().endObject().endObject()));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startArray("comments")
                .startObject().field("message", "fox eat quick").endObject()
                .startObject().field("message", "fox ate rabbit x y z").endObject()
                .startObject().field("message", "rabbit got away").endObject()
                .endArray()
                .endObject()));
        requests.add(client().prepareIndex("articles", "article", "2").setSource(jsonBuilder().startObject()
                .field("title", "big gray elephant")
                .startArray("comments")
                    .startObject().field("message", "elephant captured").endObject()
                    .startObject().field("message", "mice squashed by elephant x").endObject()
                    .startObject().field("message", "elephant scared by mice x y").endObject()
                .endArray()
                .endObject()));
        indexRandom(true, requests);

        // Inner hits can be defined in two ways: 1) with the query 2) as seperate inner_hit definition
        SearchRequest[] searchRequests = new SearchRequest[]{
                client().prepareSearch("articles").setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder().setName("comment"))).request(),
                client().prepareSearch("articles").setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")))
                        .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setPath("comments").setQuery(matchQuery("comments.message", "fox"))).request()
        };
        for (SearchRequest searchRequest : searchRequests) {
            SearchResponse response = client().search(searchRequest).actionGet();
            assertNoFailures(response);
            assertHitCount(response, 1);
            assertSearchHit(response, 1, hasId("1"));
            assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
            SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
            assertThat(innerHits.totalHits(), equalTo(2l));
            assertThat(innerHits.getHits().length, equalTo(2));
            assertThat(innerHits.getAt(0).getId(), equalTo("1"));
            assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
            assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
            assertThat(innerHits.getAt(1).getId(), equalTo("1"));
            assertThat(innerHits.getAt(1).getNestedIdentity().getField().string(), equalTo("comments"));
            assertThat(innerHits.getAt(1).getNestedIdentity().getOffset(), equalTo(1));
        }

        searchRequests = new SearchRequest[] {
                client().prepareSearch("articles")
                        .setQuery(nestedQuery("comments", matchQuery("comments.message", "elephant")))
                        .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setPath("comments").setQuery(matchQuery("comments.message", "elephant"))).request(),
                client().prepareSearch("articles")
                        .setQuery(nestedQuery("comments", matchQuery("comments.message", "elephant")).innerHit(new QueryInnerHitBuilder().setName("comment"))).request(),
                client().prepareSearch("articles")
                        .setQuery(nestedQuery("comments", matchQuery("comments.message", "elephant")).innerHit(new QueryInnerHitBuilder().setName("comment").addSort("_doc", SortOrder.DESC))).request()
        };
        for (SearchRequest searchRequest : searchRequests) {
            SearchResponse response = client().search(searchRequest).actionGet();
            assertNoFailures(response);
            assertHitCount(response, 1);
            assertSearchHit(response, 1, hasId("2"));
            assertThat(response.getHits().getAt(0).getShard(), notNullValue());
            assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
            SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
            assertThat(innerHits.totalHits(), equalTo(3l));
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

        searchRequests = new SearchRequest[] {
                client().prepareSearch("articles")
                        .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")))
                        .addInnerHit("comments", new InnerHitsBuilder.InnerHit().setPath("comments")
                                .setQuery(matchQuery("comments.message", "fox"))
                                .addHighlightedField("comments.message")
                                .setExplain(true)
                                .addFieldDataField("comments.message")
                                        .addScriptField("script", new Script("doc['comments.message'].value"))
                                .setSize(1)).request(),
                client().prepareSearch("articles")
                        .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder()
                                .addHighlightedField("comments.message")
                                .setExplain(true)
                                .addFieldDataField("comments.message")
                                                .addScriptField("script", new Script("doc['comments.message'].value"))
                                .setSize(1))).request()
        };

        for (SearchRequest searchRequest : searchRequests) {
            SearchResponse response = client().search(searchRequest).actionGet();
            assertNoFailures(response);
            SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comments");
            assertThat(innerHits.getTotalHits(), equalTo(2l));
            assertThat(innerHits.getHits().length, equalTo(1));
            assertThat(innerHits.getAt(0).getHighlightFields().get("comments.message").getFragments()[0].string(), equalTo("<em>fox</em> eat quick"));
            assertThat(innerHits.getAt(0).explanation().toString(), containsString("weight(comments.message:fox in"));
            assertThat(innerHits.getAt(0).getFields().get("comments.message").getValue().toString(), equalTo("eat"));
            assertThat(innerHits.getAt(0).getFields().get("script").getValue().toString(), equalTo("eat"));
        }
    }

    @Test
    public void testRandomNested() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", "field1", "type=nested", "field2", "type=nested"));
        int numDocs = scaledRandomIntBetween(25, 100);
        List<IndexRequestBuilder> requestBuilders = new ArrayList<>();

        int[] field1InnerObjects = new int[numDocs];
        int[] field2InnerObjects = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            int numInnerObjects = field1InnerObjects[i] = scaledRandomIntBetween(1, numDocs);
            XContentBuilder source = jsonBuilder().startObject().startArray("field1");
            for (int j = 0; j < numInnerObjects; j++) {
                source.startObject().field("x", "y").endObject();
            }
            numInnerObjects = field2InnerObjects[i] = scaledRandomIntBetween(1, numDocs);
            source.endArray().startArray("field2");
            for (int j = 0; j < numInnerObjects; j++) {
                source.startObject().field("x", "y").endObject();
            }
            source.endArray().endObject();
            requestBuilders.add(client().prepareIndex("idx", "type", String.format(Locale.ENGLISH, "%03d", i)).setSource(source));
        }
        indexRandom(true, requestBuilders);

        int size = randomIntBetween(0, numDocs);
        SearchResponse searchResponse;
        if (randomBoolean()) {
            searchResponse = client().prepareSearch("idx")
                    .setSize(numDocs)
                    .addSort("_uid", SortOrder.ASC)
                    .addInnerHit("a", new InnerHitsBuilder.InnerHit().setPath("field1").addSort("_doc", SortOrder.DESC).setSize(size)) // Sort order is DESC, because we reverse the inner objects during indexing!
                    .addInnerHit("b", new InnerHitsBuilder.InnerHit().setPath("field2").addSort("_doc", SortOrder.DESC).setSize(size))
                    .get();
        } else {
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            if (randomBoolean()) {
                boolQuery.should(nestedQuery("field1", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("a").addSort("_doc", SortOrder.DESC).setSize(size)));
                boolQuery.should(nestedQuery("field2", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("b").addSort("_doc", SortOrder.DESC).setSize(size)));
            } else {
                boolQuery.should(constantScoreQuery(nestedQuery("field1", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("a").addSort("_doc", SortOrder.DESC).setSize(size))));
                boolQuery.should(constantScoreQuery(nestedQuery("field2", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("b").addSort("_doc", SortOrder.DESC).setSize(size))));
            }
            searchResponse = client().prepareSearch("idx")
                    .setQuery(boolQuery)
                    .setSize(numDocs)
                    .addSort("_uid", SortOrder.ASC)
                    .get();
        }

        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        assertThat(searchResponse.getHits().getHits().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat(searchHit.getShard(), notNullValue());
            SearchHits inner = searchHit.getInnerHits().get("a");
            assertThat(inner.totalHits(), equalTo((long) field1InnerObjects[i]));
            for (int j = 0; j < field1InnerObjects[i] && j < size; j++) {
                SearchHit innerHit =  inner.getAt(j);
                assertThat(innerHit.getNestedIdentity().getField().string(), equalTo("field1"));
                assertThat(innerHit.getNestedIdentity().getOffset(), equalTo(j));
                assertThat(innerHit.getNestedIdentity().getChild(), nullValue());
            }

            inner = searchHit.getInnerHits().get("b");
            assertThat(inner.totalHits(), equalTo((long) field2InnerObjects[i]));
            for (int j = 0; j < field2InnerObjects[i] && j < size; j++) {
                SearchHit innerHit =  inner.getAt(j);
                assertThat(innerHit.getNestedIdentity().getField().string(), equalTo("field2"));
                assertThat(innerHit.getNestedIdentity().getOffset(), equalTo(j));
                assertThat(innerHit.getNestedIdentity().getChild(), nullValue());
            }
        }
    }

    @Test
    public void testSimpleParentChild() throws Exception {
        assertAcked(prepareCreate("articles")
                .addMapping("article", "title", "type=string")
                .addMapping("comment", "_parent", "type=article", "message", "type=string")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource("title", "quick brown fox"));
        requests.add(client().prepareIndex("articles", "comment", "1").setParent("1").setSource("message", "fox eat quick"));
        requests.add(client().prepareIndex("articles", "comment", "2").setParent("1").setSource("message", "fox ate rabbit x y z"));
        requests.add(client().prepareIndex("articles", "comment", "3").setParent("1").setSource("message", "rabbit got away"));
        requests.add(client().prepareIndex("articles", "article", "2").setSource("title", "big gray elephant"));
        requests.add(client().prepareIndex("articles", "comment", "4").setParent("2").setSource("message", "elephant captured"));
        requests.add(client().prepareIndex("articles", "comment", "5").setParent("2").setSource("message", "mice squashed by elephant x"));
        requests.add(client().prepareIndex("articles", "comment", "6").setParent("2").setSource("message", "elephant scared by mice x y"));
        indexRandom(true, requests);

        SearchRequest[] searchRequests = new SearchRequest[]{
                client().prepareSearch("articles")
                        .setQuery(hasChildQuery("comment", matchQuery("message", "fox")))
                        .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setType("comment").setQuery(matchQuery("message", "fox")))
                        .request(),
                client().prepareSearch("articles")
                        .setQuery(hasChildQuery("comment", matchQuery("message", "fox")).innerHit(new QueryInnerHitBuilder().setName("comment")))
                        .request()
        };
        for (SearchRequest searchRequest : searchRequests) {
            SearchResponse response = client().search(searchRequest).actionGet();
            assertNoFailures(response);
            assertHitCount(response, 1);
            assertSearchHit(response, 1, hasId("1"));
            assertThat(response.getHits().getAt(0).getShard(), notNullValue());

            assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
            SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
            assertThat(innerHits.totalHits(), equalTo(2l));

            assertThat(innerHits.getAt(0).getId(), equalTo("1"));
            assertThat(innerHits.getAt(0).type(), equalTo("comment"));
            assertThat(innerHits.getAt(1).getId(), equalTo("2"));
            assertThat(innerHits.getAt(1).type(), equalTo("comment"));
        }

        searchRequests = new SearchRequest[] {
                client().prepareSearch("articles")
                        .setQuery(hasChildQuery("comment", matchQuery("message", "elephant")))
                        .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setType("comment").setQuery(matchQuery("message", "elephant")))
                        .request(),
                client().prepareSearch("articles")
                        .setQuery(hasChildQuery("comment", matchQuery("message", "elephant")).innerHit(new QueryInnerHitBuilder()))
                        .request()
        };
        for (SearchRequest searchRequest : searchRequests) {
            SearchResponse response = client().search(searchRequest).actionGet();
            assertNoFailures(response);
            assertHitCount(response, 1);
            assertSearchHit(response, 1, hasId("2"));

            assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
            SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
            assertThat(innerHits.totalHits(), equalTo(3l));

            assertThat(innerHits.getAt(0).getId(), equalTo("4"));
            assertThat(innerHits.getAt(0).type(), equalTo("comment"));
            assertThat(innerHits.getAt(1).getId(), equalTo("5"));
            assertThat(innerHits.getAt(1).type(), equalTo("comment"));
            assertThat(innerHits.getAt(2).getId(), equalTo("6"));
            assertThat(innerHits.getAt(2).type(), equalTo("comment"));
        }

        searchRequests = new SearchRequest[] {
                client().prepareSearch("articles")
                        .setQuery(hasChildQuery("comment", matchQuery("message", "fox")))
                        .addInnerHit("comment", new InnerHitsBuilder.InnerHit().setType("comment")
                                        .setQuery(matchQuery("message", "fox"))
                                        .addHighlightedField("message")
                                        .setExplain(true)
                                        .addFieldDataField("message")
                                        .addScriptField("script", new Script("doc['message'].value"))
                                        .setSize(1)
                        ).request(),
                client().prepareSearch("articles")
                        .setQuery(
                                hasChildQuery("comment", matchQuery("message", "fox")).innerHit(
                                        new QueryInnerHitBuilder().addHighlightedField("message").setExplain(true)
                                                .addFieldDataField("message").addScriptField("script", new Script("doc['message'].value"))
                                                .setSize(1))).request() };

        for (SearchRequest searchRequest : searchRequests) {
            SearchResponse response = client().search(searchRequest).actionGet();
            assertNoFailures(response);
            SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
            assertThat(innerHits.getHits().length, equalTo(1));
            assertThat(innerHits.getAt(0).getHighlightFields().get("message").getFragments()[0].string(), equalTo("<em>fox</em> eat quick"));
            assertThat(innerHits.getAt(0).explanation().toString(), containsString("weight(message:fox"));
            assertThat(innerHits.getAt(0).getFields().get("message").getValue().toString(), equalTo("eat"));
            assertThat(innerHits.getAt(0).getFields().get("script").getValue().toString(), equalTo("eat"));
        }
    }

    @Test
    public void testRandomParentChild() throws Exception {
        assertAcked(prepareCreate("idx")
                        .addMapping("parent")
                        .addMapping("child1", "_parent", "type=parent")
                        .addMapping("child2", "_parent", "type=parent")
        );
        int numDocs = scaledRandomIntBetween(5, 50);
        List<IndexRequestBuilder> requestBuilders = new ArrayList<>();

        int child1 = 0;
        int child2 = 0;
        int[] child1InnerObjects = new int[numDocs];
        int[] child2InnerObjects = new int[numDocs];
        for (int parent = 0; parent < numDocs; parent++) {
            String parentId = String.format(Locale.ENGLISH, "%03d", parent);
            requestBuilders.add(client().prepareIndex("idx", "parent", parentId).setSource("{}"));

            int numChildDocs = child1InnerObjects[parent] = scaledRandomIntBetween(1, numDocs);
            int limit = child1 + numChildDocs;
            for (; child1 < limit; child1++) {
                requestBuilders.add(client().prepareIndex("idx", "child1", String.format(Locale.ENGLISH, "%04d", child1)).setParent(parentId).setSource("{}"));
            }
            numChildDocs = child2InnerObjects[parent] = scaledRandomIntBetween(1, numDocs);
            limit = child2 + numChildDocs;
            for (; child2 < limit; child2++) {
                requestBuilders.add(client().prepareIndex("idx", "child2", String.format(Locale.ENGLISH, "%04d", child2)).setParent(parentId).setSource("{}"));
            }
        }
        indexRandom(true, requestBuilders);

        int size = randomIntBetween(0, numDocs);
        SearchResponse searchResponse;
        if (randomBoolean()) {
            searchResponse = client().prepareSearch("idx")
                    .setSize(numDocs)
                    .setTypes("parent")
                    .addSort("_uid", SortOrder.ASC)
                    .addInnerHit("a", new InnerHitsBuilder.InnerHit().setType("child1").addSort("_uid", SortOrder.ASC).setSize(size))
                    .addInnerHit("b", new InnerHitsBuilder.InnerHit().setType("child2").addSort("_uid", SortOrder.ASC).setSize(size))
                    .get();
        } else {
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            if (randomBoolean()) {
                boolQuery.should(hasChildQuery("child1", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("a").addSort("_uid", SortOrder.ASC).setSize(size)));
                boolQuery.should(hasChildQuery("child2", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("b").addSort("_uid", SortOrder.ASC).setSize(size)));
            } else {
                boolQuery.should(constantScoreQuery(hasChildQuery("child1", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("a").addSort("_uid", SortOrder.ASC).setSize(size))));
                boolQuery.should(constantScoreQuery(hasChildQuery("child2", matchAllQuery()).innerHit(new QueryInnerHitBuilder().setName("b").addSort("_uid", SortOrder.ASC).setSize(size))));
            }
            searchResponse = client().prepareSearch("idx")
                    .setSize(numDocs)
                    .setTypes("parent")
                    .addSort("_uid", SortOrder.ASC)
                    .setQuery(boolQuery)
                    .get();
        }

        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, numDocs);
        assertThat(searchResponse.getHits().getHits().length, equalTo(numDocs));

        int offset1 = 0;
        int offset2 = 0;
        for (int parent = 0; parent < numDocs; parent++) {
            SearchHit searchHit = searchResponse.getHits().getAt(parent);
            assertThat(searchHit.getType(), equalTo("parent"));
            assertThat(searchHit.getId(), equalTo(String.format(Locale.ENGLISH, "%03d", parent)));
            assertThat(searchHit.getShard(), notNullValue());

            SearchHits inner = searchHit.getInnerHits().get("a");
            assertThat(inner.totalHits(), equalTo((long) child1InnerObjects[parent]));
            for (int child = 0; child < child1InnerObjects[parent] && child < size; child++) {
                SearchHit innerHit =  inner.getAt(child);
                assertThat(innerHit.getType(), equalTo("child1"));
                String childId = String.format(Locale.ENGLISH, "%04d", offset1 + child);
                assertThat(innerHit.getId(), equalTo(childId));
                assertThat(innerHit.getNestedIdentity(), nullValue());
            }
            offset1 += child1InnerObjects[parent];

            inner = searchHit.getInnerHits().get("b");
            assertThat(inner.totalHits(), equalTo((long) child2InnerObjects[parent]));
            for (int child = 0; child < child2InnerObjects[parent] && child < size; child++) {
                SearchHit innerHit = inner.getAt(child);
                assertThat(innerHit.getType(), equalTo("child2"));
                String childId = String.format(Locale.ENGLISH, "%04d", offset2 + child);
                assertThat(innerHit.getId(), equalTo(childId));
                assertThat(innerHit.getNestedIdentity(), nullValue());
            }
            offset2 += child2InnerObjects[parent];
        }
    }

    @Test
    public void testPathOrTypeMustBeDefined() {
        createIndex("articles");
        ensureGreen("articles");
        try {
            client().prepareSearch("articles")
                    .addInnerHit("comment", new InnerHitsBuilder.InnerHit())
                    .get();
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Failed to build"));
        }

    }

    @Test
    public void testInnerHitsOnHasParent() throws Exception {
        assertAcked(prepareCreate("stack")
                        .addMapping("question", "body", "type=string")
                        .addMapping("answer", "_parent", "type=question", "body", "type=string")
        );
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("stack", "question", "1").setSource("body", "I'm using HTTPS + Basic authentication to protect a resource. How can I throttle authentication attempts to protect against brute force attacks?"));
        requests.add(client().prepareIndex("stack", "answer", "1").setParent("1").setSource("body", "install fail2ban and enable rules for apache"));
        requests.add(client().prepareIndex("stack", "question", "2").setSource("body", "I have firewall rules set up and also denyhosts installed.\\ndo I also need to install fail2ban?"));
        requests.add(client().prepareIndex("stack", "answer", "2").setParent("2").setSource("body", "Denyhosts protects only ssh; Fail2Ban protects all daemons."));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("stack")
                .setTypes("answer")
                .addSort("_uid", SortOrder.ASC)
                .setQuery(
                        boolQuery()
                                .must(matchQuery("body", "fail2ban"))
                                .must(hasParentQuery("question", matchAllQuery()).innerHit(new QueryInnerHitBuilder()))
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        SearchHit searchHit = response.getHits().getAt(0);
        assertThat(searchHit.getId(), equalTo("1"));
        assertThat(searchHit.getType(), equalTo("answer"));
        assertThat(searchHit.getInnerHits().get("question").getTotalHits(), equalTo(1l));
        assertThat(searchHit.getInnerHits().get("question").getAt(0).getType(), equalTo("question"));
        assertThat(searchHit.getInnerHits().get("question").getAt(0).id(), equalTo("1"));

        searchHit = response.getHits().getAt(1);
        assertThat(searchHit.getId(), equalTo("2"));
        assertThat(searchHit.getType(), equalTo("answer"));
        assertThat(searchHit.getInnerHits().get("question").getTotalHits(), equalTo(1l));
        assertThat(searchHit.getInnerHits().get("question").getAt(0).getType(), equalTo("question"));
        assertThat(searchHit.getInnerHits().get("question").getAt(0).id(), equalTo("2"));
    }

    @Test
    public void testParentChildMultipleLayers() throws Exception {
        assertAcked(prepareCreate("articles")
                        .addMapping("article", "title", "type=string")
                        .addMapping("comment", "_parent", "type=article", "message", "type=string")
                        .addMapping("remark", "_parent", "type=comment", "message", "type=string")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource("title", "quick brown fox"));
        requests.add(client().prepareIndex("articles", "comment", "1").setParent("1").setSource("message", "fox eat quick"));
        requests.add(client().prepareIndex("articles", "remark", "1").setParent("1").setRouting("1").setSource("message", "good"));
        requests.add(client().prepareIndex("articles", "article", "2").setSource("title", "big gray elephant"));
        requests.add(client().prepareIndex("articles", "comment", "2").setParent("2").setSource("message", "elephant captured"));
        requests.add(client().prepareIndex("articles", "remark", "2").setParent("2").setRouting("2").setSource("message", "bad"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(hasChildQuery("comment", hasChildQuery("remark", matchQuery("message", "good"))))
                .addInnerHit("comment",
                        new InnerHitsBuilder.InnerHit().setType("comment")
                                .setQuery(hasChildQuery("remark", matchQuery("message", "good")))
                                .addInnerHit("remark", new InnerHitsBuilder.InnerHit().setType("remark").setQuery(matchQuery("message", "good")))
                )
                .get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("1"));

        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertThat(innerHits.getAt(0).type(), equalTo("comment"));

        innerHits = innerHits.getAt(0).getInnerHits().get("remark");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertThat(innerHits.getAt(0).type(), equalTo("remark"));

        response = client().prepareSearch("articles")
                .setQuery(hasChildQuery("comment", hasChildQuery("remark", matchQuery("message", "bad"))))
                .addInnerHit("comment",
                        new InnerHitsBuilder.InnerHit().setType("comment")
                                .setQuery(hasChildQuery("remark", matchQuery("message", "bad")))
                                .addInnerHit("remark", new InnerHitsBuilder.InnerHit().setType("remark").setQuery(matchQuery("message", "bad")))
                )
                .get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("2"));

        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getAt(0).getId(), equalTo("2"));
        assertThat(innerHits.getAt(0).type(), equalTo("comment"));

        innerHits = innerHits.getAt(0).getInnerHits().get("remark");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getAt(0).getId(), equalTo("2"));
        assertThat(innerHits.getAt(0).type(), equalTo("remark"));
    }

    @Test
    public void testNestedMultipleLayers() throws Exception {
        assertAcked(prepareCreate("articles").addMapping("article", jsonBuilder().startObject().startObject("article").startObject("properties")
                .startObject("comments")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("message")
                            .field("type", "string")
                        .endObject()
                        .startObject("remarks")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("message").field("type", "string").endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("title")
                    .field("type", "string")
                .endObject()
                .endObject().endObject().endObject()));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startArray("comments")
                .startObject()
                .field("message", "fox eat quick")
                .startArray("remarks").startObject().field("message", "good").endObject().endArray()
                .endObject()
                .endArray()
                .endObject()));
        requests.add(client().prepareIndex("articles", "article", "2").setSource(jsonBuilder().startObject()
                .field("title", "big gray elephant")
                .startArray("comments")
                    .startObject()
                        .field("message", "elephant captured")
                        .startArray("remarks").startObject().field("message", "bad").endObject().endArray()
                    .endObject()
                .endArray()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "good"))))
                .addInnerHit("comment", new InnerHitsBuilder.InnerHit()
                                .setPath("comments")
                                .setQuery(nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "good")))
                                .addInnerHit("remark", new InnerHitsBuilder.InnerHit().setPath("comments.remarks").setQuery(matchQuery("comments.remarks.message", "good")))
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getHits().length, equalTo(1));
        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        innerHits = innerHits.getAt(0).getInnerHits().get("remark");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getHits().length, equalTo(1));
        assertThat(innerHits.getAt(0).getId(), equalTo("1"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));

        // Directly refer to the second level:
        response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "bad")).innerHit(new QueryInnerHitBuilder()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("2"));
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        innerHits = response.getHits().getAt(0).getInnerHits().get("comments.remarks");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getHits().length, equalTo(1));
        assertThat(innerHits.getAt(0).getId(), equalTo("2"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));

        response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "bad"))))
                .addInnerHit("comment", new InnerHitsBuilder.InnerHit()
                                .setPath("comments")
                                .setQuery(nestedQuery("comments.remarks", matchQuery("comments.remarks.message", "bad")))
                                .addInnerHit("remark", new InnerHitsBuilder.InnerHit().setPath("comments.remarks").setQuery(matchQuery("comments.remarks.message", "bad"))))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertSearchHit(response, 1, hasId("2"));
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        innerHits = response.getHits().getAt(0).getInnerHits().get("comment");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getHits().length, equalTo(1));
        assertThat(innerHits.getAt(0).getId(), equalTo("2"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        innerHits = innerHits.getAt(0).getInnerHits().get("remark");
        assertThat(innerHits.totalHits(), equalTo(1l));
        assertThat(innerHits.getHits().length, equalTo(1));
        assertThat(innerHits.getAt(0).getId(), equalTo("2"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getField().string(), equalTo("remarks"));
        assertThat(innerHits.getAt(0).getNestedIdentity().getChild().getOffset(), equalTo(0));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/9723
    public void testNestedDefinedAsObject() throws Exception {
        assertAcked(prepareCreate("articles").addMapping("article", "comments", "type=nested", "title", "type=string"));

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments").field("message", "fox eat quick").endObject()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getChild(), nullValue());
    }

    @Test
    public void testNestedInnerHitsWithStoredFieldsAndNoSourceBackcompat() throws Exception {
        assertAcked(prepareCreate("articles")
                .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id)
                .addMapping("article", jsonBuilder().startObject()
                                .startObject("_source").field("enabled", false).endObject()
                                .startObject("properties")
                                    .startObject("comments")
                                        .field("type", "nested")
                                        .startObject("properties")
                                            .startObject("message").field("type", "string").field("store", "yes").endObject()
                                        .endObject()
                                    .endObject()
                                    .endObject()
                                .endObject()
                )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments").field("message", "fox eat quick").endObject()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder().field("comments.message")))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getChild(), nullValue());
        assertThat(String.valueOf((Object)response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).fields().get("comments.message").getValue()), equalTo("fox eat quick"));
    }

    @Test
    public void testNestedInnerHitsWithHighlightOnStoredFieldBackcompat() throws Exception {
        assertAcked(prepareCreate("articles")
                .setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id)
                        .addMapping("article", jsonBuilder().startObject()
                                        .startObject("_source").field("enabled", false).endObject()
                                            .startObject("properties")
                                                .startObject("comments")
                                                    .field("type", "nested")
                                                    .startObject("properties")
                                                        .startObject("message").field("type", "string").field("store", "yes").endObject()
                                                    .endObject()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                        )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments").field("message", "fox eat quick").endObject()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder().addHighlightedField("comments.message")))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getChild(), nullValue());
        assertThat(String.valueOf(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).highlightFields().get("comments.message").getFragments()[0]), equalTo("<em>fox</em> eat quick"));
    }

    @Test
    public void testNestedInnerHitsWithExcludeSourceBackcompat() throws Exception {
        assertAcked(prepareCreate("articles").setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id)
                        .addMapping("article", jsonBuilder().startObject()
                                        .startObject("_source").field("excludes", new String[]{"comments"}).endObject()
                                        .startObject("properties")
                                            .startObject("comments")
                                                .field("type", "nested")
                                                .startObject("properties")
                                                    .startObject("message").field("type", "string").field("store", "yes").endObject()
                                                .endObject()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                        )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments").field("message", "fox eat quick").endObject()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder().field("comments.message").setFetchSource(true)))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getChild(), nullValue());
        assertThat(String.valueOf((Object)response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).fields().get("comments.message").getValue()), equalTo("fox eat quick"));
    }

    @Test
    public void testNestedInnerHitsHiglightWithExcludeSourceBackcompat() throws Exception {
        assertAcked(prepareCreate("articles").setSettings(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id)
                        .addMapping("article", jsonBuilder().startObject()
                                        .startObject("_source").field("excludes", new String[]{"comments"}).endObject()
                                        .startObject("properties")
                                        .startObject("comments")
                                        .field("type", "nested")
                                        .startObject("properties")
                                        .startObject("message").field("type", "string").field("store", "yes").endObject()
                                        .endObject()
                                        .endObject()
                                        .endObject()
                                        .endObject()
                        )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments").field("message", "fox eat quick").endObject()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments", matchQuery("comments.message", "fox")).innerHit(new QueryInnerHitBuilder().addHighlightedField("comments.message")))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getField().string(), equalTo("comments"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).getNestedIdentity().getChild(), nullValue());
        assertThat(String.valueOf(response.getHits().getAt(0).getInnerHits().get("comments").getAt(0).highlightFields().get("comments.message").getFragments()[0]), equalTo("<em>fox</em> eat quick"));
    }

    @Test
    public void testInnerHitsWithObjectFieldThatHasANestedField() throws Exception {
        assertAcked(prepareCreate("articles")
                        .addMapping("article", jsonBuilder().startObject()
                                        .startObject("properties")
                                            .startObject("comments")
                                                .field("type", "object")
                                                .startObject("properties")
                                                    .startObject("messages").field("type", "nested").endObject()
                                                .endObject()
                                                .endObject()
                                            .endObject()
                                        .endObject()
                        )
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments")
                .startArray("messages")
                    .startObject().field("message", "fox eat quick").endObject()
                    .startObject().field("message", "bear eat quick").endObject()
                .endArray()
                .endObject()
                .endObject()));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments.messages", matchQuery("comments.messages.message", "fox")).innerHit(new QueryInnerHitBuilder()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getField().string(), equalTo("comments.messages"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getChild(), nullValue());

        response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments.messages", matchQuery("comments.messages.message", "bear")).innerHit(new QueryInnerHitBuilder()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getField().string(), equalTo("comments.messages"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getChild(), nullValue());

        // index the message in an object form instead of an array
        requests = new ArrayList<>();
        requests.add(client().prepareIndex("articles", "article", "1").setSource(jsonBuilder().startObject()
                .field("title", "quick brown fox")
                .startObject("comments").startObject("messages").field("message", "fox eat quick").endObject().endObject()
                .endObject()));
        indexRandom(true, requests);
        response = client().prepareSearch("articles")
                .setQuery(nestedQuery("comments.messages", matchQuery("comments.messages.message", "fox")).innerHit(new QueryInnerHitBuilder()))
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getField().string(), equalTo("comments.messages"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("comments.messages").getAt(0).getNestedIdentity().getChild(), nullValue());
    }

    @Test
    public void testRoyals() throws Exception {
        assertAcked(
                prepareCreate("royals")
                .addMapping("king")
                .addMapping("prince", "_parent", "type=king")
                .addMapping("duke", "_parent", "type=prince")
                .addMapping("earl", "_parent", "type=duke")
                .addMapping("baron", "_parent", "type=earl")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("royals", "king", "king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "prince", "prince").setParent("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "duke", "duke").setParent("prince").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "earl", "earl1").setParent("duke").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "earl", "earl2").setParent("duke").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "earl", "earl3").setParent("duke").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "earl", "earl4").setParent("duke").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "baron", "baron1").setParent("earl1").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "baron", "baron2").setParent("earl2").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "baron", "baron3").setParent("earl3").setRouting("king").setSource("{}"));
        requests.add(client().prepareIndex("royals", "baron", "baron4").setParent("earl4").setRouting("king").setSource("{}"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("royals")
                .setTypes("duke")
                .addInnerHit("earls", new InnerHitsBuilder.InnerHit()
                                .setType("earl")
                                .addSort(SortBuilders.fieldSort("_uid").order(SortOrder.ASC))
                                .setSize(4)
                                .addInnerHit("barons", new InnerHitsBuilder.InnerHit().setType("baron"))
                )
                .addInnerHit("princes",
                        new InnerHitsBuilder.InnerHit().setType("prince")
                        .addInnerHit("kings", new InnerHitsBuilder.InnerHit().setType("king"))
                )
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("duke"));

        SearchHits innerHits = response.getHits().getAt(0).getInnerHits().get("earls");
        assertThat(innerHits.getTotalHits(), equalTo(4l));
        assertThat(innerHits.getAt(0).getId(), equalTo("earl1"));
        assertThat(innerHits.getAt(1).getId(), equalTo("earl2"));
        assertThat(innerHits.getAt(2).getId(), equalTo("earl3"));
        assertThat(innerHits.getAt(3).getId(), equalTo("earl4"));

        SearchHits innerInnerHits = innerHits.getAt(0).getInnerHits().get("barons");
        assertThat(innerInnerHits.totalHits(), equalTo(1l));
        assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron1"));

        innerInnerHits = innerHits.getAt(1).getInnerHits().get("barons");
        assertThat(innerInnerHits.totalHits(), equalTo(1l));
        assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron2"));

        innerInnerHits = innerHits.getAt(2).getInnerHits().get("barons");
        assertThat(innerInnerHits.totalHits(), equalTo(1l));
        assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron3"));

        innerInnerHits = innerHits.getAt(3).getInnerHits().get("barons");
        assertThat(innerInnerHits.totalHits(), equalTo(1l));
        assertThat(innerInnerHits.getAt(0).getId(), equalTo("baron4"));

        innerHits = response.getHits().getAt(0).getInnerHits().get("princes");
        assertThat(innerHits.getTotalHits(), equalTo(1l));
        assertThat(innerHits.getAt(0).getId(), equalTo("prince"));

        innerInnerHits = innerHits.getAt(0).getInnerHits().get("kings");
        assertThat(innerInnerHits.totalHits(), equalTo(1l));
        assertThat(innerInnerHits.getAt(0).getId(), equalTo("king"));
    }

    @Test
    public void matchesQueries_nestedInnerHits() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("nested1")
                .field("type", "nested")
                .endObject()
                .startObject("field1")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        assertAcked(prepareCreate("test").addMapping("type1", builder));
        ensureGreen();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = randomIntBetween(2, 35);
        requests.add(client().prepareIndex("test", "type1", "0").setSource(jsonBuilder().startObject()
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
                .endObject()));
        requests.add(client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
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
                .endObject()));

        for (int i = 2; i < numDocs; i++) {
            requests.add(client().prepareIndex("test", "type1", String.valueOf(i)).setSource(jsonBuilder().startObject()
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
                    .endObject()));
        }

        indexRandom(true, requests);
        waitForRelocation(ClusterHealthStatus.GREEN);

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(nestedQuery("nested1", boolQuery()
                                .should(termQuery("nested1.n_field1", "n_value1_1").queryName("test1"))
                                .should(termQuery("nested1.n_field1", "n_value1_3").queryName("test2"))
                                .should(termQuery("nested1.n_field2", "n_value2_2").queryName("test3"))
                ).innerHit(new QueryInnerHitBuilder().addSort("nested1.n_field1", SortOrder.ASC)))
                .setSize(numDocs)
                .addSort("field1", SortOrder.ASC)
                .get();
        assertNoFailures(searchResponse);
        assertAllSuccessful(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) numDocs));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("0"));
        assertThat(searchResponse.getHits().getAt(0).getInnerHits().get("nested1").getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).getInnerHits().get("nested1").getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getInnerHits().get("nested1").getAt(0).getMatchedQueries()[0], equalTo("test1"));
        assertThat(searchResponse.getHits().getAt(0).getInnerHits().get("nested1").getAt(1).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getInnerHits().get("nested1").getAt(1).getMatchedQueries()[0], equalTo("test3"));


        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getInnerHits().get("nested1").getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(1).getInnerHits().get("nested1").getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(1).getInnerHits().get("nested1").getAt(0).getMatchedQueries()[0], equalTo("test2"));

        for (int i = 2; i < numDocs; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(String.valueOf(i)));
            assertThat(searchResponse.getHits().getAt(i).getInnerHits().get("nested1").getTotalHits(), equalTo(1l));
            assertThat(searchResponse.getHits().getAt(i).getInnerHits().get("nested1").getAt(0).getMatchedQueries().length, equalTo(1));
            assertThat(searchResponse.getHits().getAt(i).getInnerHits().get("nested1").getAt(0).getMatchedQueries()[0], equalTo("test3"));
        }
    }

    @Test
    public void matchesQueries_parentChildInnerHits() throws Exception {
        assertAcked(prepareCreate("index").addMapping("child", "_parent", "type=parent"));
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("index", "parent", "1").setSource("{}"));
        requests.add(client().prepareIndex("index", "child", "1").setParent("1").setSource("field", "value1"));
        requests.add(client().prepareIndex("index", "child", "2").setParent("1").setSource("field", "value2"));
        requests.add(client().prepareIndex("index", "parent", "2").setSource("{}"));
        requests.add(client().prepareIndex("index", "child", "3").setParent("2").setSource("field", "value1"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("index")
                .setQuery(hasChildQuery("child", matchQuery("field", "value1").queryName("_name1")).innerHit(new QueryInnerHitBuilder()))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("child").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries()[0], equalTo("_name1"));

        assertThat(response.getHits().getAt(1).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getInnerHits().get("child").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(1).getInnerHits().get("child").getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(response.getHits().getAt(1).getInnerHits().get("child").getAt(0).getMatchedQueries()[0], equalTo("_name1"));

        response = client().prepareSearch("index")
                .setQuery(hasChildQuery("child", matchQuery("field", "value2").queryName("_name2")).innerHit(new QueryInnerHitBuilder()))
                .addSort("_id", SortOrder.ASC)
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("child").getTotalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(response.getHits().getAt(0).getInnerHits().get("child").getAt(0).getMatchedQueries()[0], equalTo("_name2"));
    }

    @Test
    public void testDontExplode() throws Exception {
        assertAcked(prepareCreate("index1").addMapping("child", "_parent", "type=parent"));
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex("index1", "parent", "1").setSource("{}"));
        requests.add(client().prepareIndex("index1", "child", "1").setParent("1").setSource("field", "value1"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("index1")
                .setQuery(hasChildQuery("child", matchQuery("field", "value1")).innerHit(new QueryInnerHitBuilder().setSize(ArrayUtil.MAX_ARRAY_LENGTH - 1)))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        assertAcked(prepareCreate("index2").addMapping("type", "nested", "type=nested"));
        client().prepareIndex("index2", "type", "1").setSource(jsonBuilder().startObject()
                .startArray("nested")
                .startObject()
                .field("field", "value1")
                .endObject()
                .endArray()
                .endObject())
        .setRefresh(true)
        .get();

        response = client().prepareSearch("index2")
                .setQuery(nestedQuery("nested", matchQuery("nested.field", "value1")).innerHit(new QueryInnerHitBuilder().setSize(ArrayUtil.MAX_ARRAY_LENGTH - 1)))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);
    }

}
