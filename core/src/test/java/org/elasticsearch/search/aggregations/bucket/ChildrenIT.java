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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 */
@ESIntegTestCase.SuiteScopeTestCase
public class ChildrenIT extends ESIntegTestCase {

    private final static Map<String, Control> categoryToControl = new HashMap<>();

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
                prepareCreate("test")
                    .addMapping("article")
                    .addMapping("comment", "_parent", "type=article")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        String[] uniqueCategories = new String[randomIntBetween(1, 25)];
        for (int i = 0; i < uniqueCategories.length; i++) {
            uniqueCategories[i] = Integer.toString(i);
        }
        int catIndex = 0;

        int numParentDocs = randomIntBetween(uniqueCategories.length, uniqueCategories.length * 5);
        for (int i = 0; i < numParentDocs; i++) {
            String id = Integer.toString(i);

            // TODO: this array is always of length 1, and testChildrenAggs fails if this is changed
            String[] categories = new String[randomIntBetween(1,1)];
            for (int j = 0; j < categories.length; j++) {
                String category = categories[j] = uniqueCategories[catIndex++ % uniqueCategories.length];
                Control control = categoryToControl.get(category);
                if (control == null) {
                    categoryToControl.put(category, control = new Control(category));
                }
                control.articleIds.add(id);
            }

            requests.add(client().prepareIndex("test", "article", id).setCreate(true).setSource("category", categories, "randomized", true));
        }

        String[] commenters = new String[randomIntBetween(5, 50)];
        for (int i = 0; i < commenters.length; i++) {
            commenters[i] = Integer.toString(i);
        }

        int id = 0;
        for (Control control : categoryToControl.values()) {
            for (String articleId : control.articleIds) {
                int numChildDocsPerParent = randomIntBetween(0, 5);
                for (int i = 0; i < numChildDocsPerParent; i++) {
                    String commenter = commenters[id % commenters.length];
                    String idValue = Integer.toString(id++);
                    control.commentIds.add(idValue);
                    Set<String> ids = control.commenterToCommentId.get(commenter);
                    if (ids == null) {
                        control.commenterToCommentId.put(commenter, ids = new HashSet<>());
                    }
                    ids.add(idValue);
                    requests.add(client().prepareIndex("test", "comment", idValue).setCreate(true).setParent(articleId).setSource("commenter", commenter));
                }
            }
        }

        requests.add(client().prepareIndex("test", "article", "a").setSource("category", new String[]{"a"}, "randomized", false));
        requests.add(client().prepareIndex("test", "article", "b").setSource("category", new String[]{"a", "b"}, "randomized", false));
        requests.add(client().prepareIndex("test", "article", "c").setSource("category", new String[]{"a", "b", "c"}, "randomized", false));
        requests.add(client().prepareIndex("test", "article", "d").setSource("category", new String[]{"c"}, "randomized", false));
        requests.add(client().prepareIndex("test", "comment", "a").setParent("a").setSource("{}"));
        requests.add(client().prepareIndex("test", "comment", "c").setParent("c").setSource("{}"));

        indexRandom(true, requests);
        ensureSearchable("test");
    }

    @Test
    public void testChildrenAggs() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("randomized", true))
                .addAggregation(
                        terms("category").field("category").size(0).subAggregation(
                                children("to_comment").childType("comment").subAggregation(
                                        terms("commenters").field("commenter").size(0).subAggregation(
                                                topHits("top_comments")
                                        ))
                        )
                ).get();
        assertSearchResponse(searchResponse);

        Terms categoryTerms = searchResponse.getAggregations().get("category");
        assertThat(categoryTerms.getBuckets().size(), equalTo(categoryToControl.size()));
        for (Map.Entry<String, Control> entry1 : categoryToControl.entrySet()) {
            Terms.Bucket categoryBucket = categoryTerms.getBucketByKey(entry1.getKey());
            assertThat(categoryBucket.getKeyAsString(), equalTo(entry1.getKey()));
            assertThat(categoryBucket.getDocCount(), equalTo((long) entry1.getValue().articleIds.size()));

            Children childrenBucket = categoryBucket.getAggregations().get("to_comment");
            assertThat(childrenBucket.getName(), equalTo("to_comment"));
            assertThat(childrenBucket.getDocCount(), equalTo((long) entry1.getValue().commentIds.size()));
            assertThat((long) childrenBucket.getProperty("_count"), equalTo((long) entry1.getValue().commentIds.size()));

            Terms commentersTerms = childrenBucket.getAggregations().get("commenters");
            assertThat((Terms) childrenBucket.getProperty("commenters"), sameInstance(commentersTerms));
            assertThat(commentersTerms.getBuckets().size(), equalTo(entry1.getValue().commenterToCommentId.size()));
            for (Map.Entry<String, Set<String>> entry2 : entry1.getValue().commenterToCommentId.entrySet()) {
                Terms.Bucket commentBucket = commentersTerms.getBucketByKey(entry2.getKey());
                assertThat(commentBucket.getKeyAsString(), equalTo(entry2.getKey()));
                assertThat(commentBucket.getDocCount(), equalTo((long) entry2.getValue().size()));

                TopHits topHits = commentBucket.getAggregations().get("top_comments");
                for (SearchHit searchHit : topHits.getHits().getHits()) {
                    assertThat(entry2.getValue().contains(searchHit.getId()), is(true));
                }
            }
        }
    }

    @Test
    public void testParentWithMultipleBuckets() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("randomized", false))
                .addAggregation(
                        terms("category").field("category").size(0).subAggregation(
                                children("to_comment").childType("comment").subAggregation(topHits("top_comments").addSort("_uid", SortOrder.ASC))
                        )
                ).get();
        assertSearchResponse(searchResponse);

        Terms categoryTerms = searchResponse.getAggregations().get("category");
        assertThat(categoryTerms.getBuckets().size(), equalTo(3));

        for (Terms.Bucket bucket : categoryTerms.getBuckets()) {
            logger.info("bucket=" + bucket.getKey());
            Children childrenBucket = bucket.getAggregations().get("to_comment");
            TopHits topHits = childrenBucket.getAggregations().get("top_comments");
            logger.info("total_hits={}", topHits.getHits().getTotalHits());
            for (SearchHit searchHit : topHits.getHits()) {
                logger.info("hit= {} {} {}", searchHit.sortValues()[0], searchHit.getType(), searchHit.getId());
            }
        }

        Terms.Bucket categoryBucket = categoryTerms.getBucketByKey("a");
        assertThat(categoryBucket.getKeyAsString(), equalTo("a"));
        assertThat(categoryBucket.getDocCount(), equalTo(3l));

        Children childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(2l));
        TopHits topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().totalHits(), equalTo(2l));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("a"));
        assertThat(topHits.getHits().getAt(0).getType(), equalTo("comment"));
        assertThat(topHits.getHits().getAt(1).getId(), equalTo("c"));
        assertThat(topHits.getHits().getAt(1).getType(), equalTo("comment"));

        categoryBucket = categoryTerms.getBucketByKey("b");
        assertThat(categoryBucket.getKeyAsString(), equalTo("b"));
        assertThat(categoryBucket.getDocCount(), equalTo(2l));

        childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(1l));
        topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().totalHits(), equalTo(1l));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("c"));
        assertThat(topHits.getHits().getAt(0).getType(), equalTo("comment"));

        categoryBucket = categoryTerms.getBucketByKey("c");
        assertThat(categoryBucket.getKeyAsString(), equalTo("c"));
        assertThat(categoryBucket.getDocCount(), equalTo(2l));

        childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(1l));
        topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().totalHits(), equalTo(1l));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("c"));
        assertThat(topHits.getHits().getAt(0).getType(), equalTo("comment"));
    }

    @Test
    public void testWithDeletes() throws Exception {
        String indexName = "xyz";
        assertAcked(
                prepareCreate(indexName)
                        .addMapping("parent")
                        .addMapping("child", "_parent", "type=parent", "count", "type=long")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex(indexName, "parent", "1").setSource("{}"));
        requests.add(client().prepareIndex(indexName, "child", "0").setParent("1").setSource("count", 1));
        requests.add(client().prepareIndex(indexName, "child", "1").setParent("1").setSource("count", 1));
        requests.add(client().prepareIndex(indexName, "child", "2").setParent("1").setSource("count", 1));
        requests.add(client().prepareIndex(indexName, "child", "3").setParent("1").setSource("count", 1));
        indexRandom(true, requests);

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch(indexName)
                    .addAggregation(children("children").childType("child").subAggregation(sum("counts").field("count")))
                    .get();

            assertNoFailures(searchResponse);
            Children children = searchResponse.getAggregations().get("children");
            assertThat(children.getDocCount(), equalTo(4l));

            Sum count = children.getAggregations().get("counts");
            assertThat(count.getValue(), equalTo(4.));

            String idToUpdate = Integer.toString(randomInt(3));
            /*
             * The whole point of this test is to test these things with deleted
             * docs in the index so we turn off detect_noop to make sure that
             * the updates cause that.
             */
            UpdateResponse updateResponse = client().prepareUpdate(indexName, "child", idToUpdate)
                    .setParent("1")
                    .setDoc("count", 1)
                    .setDetectNoop(false)
                    .get();
            assertThat(updateResponse.getVersion(), greaterThan(1l));
            refresh();
        }
    }

    @Test
    public void testNonExistingChildType() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .addAggregation(
                        children("non-existing").childType("xyz")
                ).get();
        assertSearchResponse(searchResponse);

        Children children = searchResponse.getAggregations().get("non-existing");
        assertThat(children.getName(), equalTo("non-existing"));
        assertThat(children.getDocCount(), equalTo(0l));
    }

    @Test
    public void testPostCollection() throws Exception {
        String indexName = "prodcatalog";
        String masterType = "masterprod";
        String childType = "variantsku";
        assertAcked(
                prepareCreate(indexName)
                        .addMapping(masterType, "brand", "type=string", "name", "type=string", "material", "type=string")
                        .addMapping(childType, "_parent", "type=masterprod", "color", "type=string", "size", "type=string")
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex(indexName, masterType, "1").setSource("brand", "Levis", "name", "Style 501", "material", "Denim"));
        requests.add(client().prepareIndex(indexName, childType, "0").setParent("1").setSource("color", "blue", "size", "32"));
        requests.add(client().prepareIndex(indexName, childType, "1").setParent("1").setSource("color", "blue", "size", "34"));
        requests.add(client().prepareIndex(indexName, childType, "2").setParent("1").setSource("color", "blue", "size", "36"));
        requests.add(client().prepareIndex(indexName, childType, "3").setParent("1").setSource("color", "black", "size", "38"));
        requests.add(client().prepareIndex(indexName, childType, "4").setParent("1").setSource("color", "black", "size", "40"));
        requests.add(client().prepareIndex(indexName, childType, "5").setParent("1").setSource("color", "gray", "size", "36"));

        requests.add(client().prepareIndex(indexName, masterType, "2").setSource("brand", "Wrangler", "name", "Regular Cut", "material", "Leather"));
        requests.add(client().prepareIndex(indexName, childType, "6").setParent("2").setSource("color", "blue", "size", "32"));
        requests.add(client().prepareIndex(indexName, childType, "7").setParent("2").setSource("color", "blue", "size", "34"));
        requests.add(client().prepareIndex(indexName, childType, "8").setParent("2").setSource("color", "black", "size", "36"));
        requests.add(client().prepareIndex(indexName, childType, "9").setParent("2").setSource("color", "black", "size", "38"));
        requests.add(client().prepareIndex(indexName, childType, "10").setParent("2").setSource("color", "black", "size", "40"));
        requests.add(client().prepareIndex(indexName, childType, "11").setParent("2").setSource("color", "orange", "size", "36"));
        requests.add(client().prepareIndex(indexName, childType, "12").setParent("2").setSource("color", "green", "size", "44"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch(indexName).setTypes(masterType)
                .setQuery(hasChildQuery(childType, termQuery("color", "orange")))
                .addAggregation(children("my-refinements")
                                .childType(childType)
                                .subAggregation(terms("my-colors").field("color"))
                                .subAggregation(terms("my-sizes").field("size"))
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        Children childrenAgg = response.getAggregations().get("my-refinements");
        assertThat(childrenAgg.getDocCount(), equalTo(7l));

        Terms termsAgg = childrenAgg.getAggregations().get("my-colors");
        assertThat(termsAgg.getBuckets().size(), equalTo(4));
        assertThat(termsAgg.getBucketByKey("black").getDocCount(), equalTo(3l));
        assertThat(termsAgg.getBucketByKey("blue").getDocCount(), equalTo(2l));
        assertThat(termsAgg.getBucketByKey("green").getDocCount(), equalTo(1l));
        assertThat(termsAgg.getBucketByKey("orange").getDocCount(), equalTo(1l));

        termsAgg = childrenAgg.getAggregations().get("my-sizes");
        assertThat(termsAgg.getBuckets().size(), equalTo(6));
        assertThat(termsAgg.getBucketByKey("36").getDocCount(), equalTo(2l));
        assertThat(termsAgg.getBucketByKey("32").getDocCount(), equalTo(1l));
        assertThat(termsAgg.getBucketByKey("34").getDocCount(), equalTo(1l));
        assertThat(termsAgg.getBucketByKey("38").getDocCount(), equalTo(1l));
        assertThat(termsAgg.getBucketByKey("40").getDocCount(), equalTo(1l));
        assertThat(termsAgg.getBucketByKey("44").getDocCount(), equalTo(1l));
    }

    @Test
    public void testHierarchicalChildrenAggs() {
        String indexName = "geo";
        String grandParentType = "continent";
        String parentType = "country";
        String childType = "city";
        assertAcked(
                prepareCreate(indexName)
                        .setSettings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        )
                        .addMapping(grandParentType)
                        .addMapping(parentType, "_parent", "type=" + grandParentType)
                        .addMapping(childType, "_parent", "type=" + parentType)
        );

        client().prepareIndex(indexName, grandParentType, "1").setSource("name", "europe").get();
        client().prepareIndex(indexName, parentType, "2").setParent("1").setSource("name", "belgium").get();
        client().prepareIndex(indexName, childType, "3").setParent("2").setRouting("1").setSource("name", "brussels").get();
        refresh();

        SearchResponse response = client().prepareSearch(indexName)
                .setQuery(matchQuery("name", "europe"))
                .addAggregation(
                        children(parentType).childType(parentType).subAggregation(
                                children(childType).childType(childType).subAggregation(
                                        terms("name").field("name")
                                )
                        )
                )
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        Children children = response.getAggregations().get(parentType);
        assertThat(children.getName(), equalTo(parentType));
        assertThat(children.getDocCount(), equalTo(1l));
        children = children.getAggregations().get(childType);
        assertThat(children.getName(), equalTo(childType));
        assertThat(children.getDocCount(), equalTo(1l));
        Terms terms = children.getAggregations().get("name");
        assertThat(terms.getBuckets().size(), equalTo(1));
        assertThat(terms.getBuckets().get(0).getKey().toString(), equalTo("brussels"));
        assertThat(terms.getBuckets().get(0).getDocCount(), equalTo(1l));
    }

    private static final class Control {

        final String category;
        final Set<String> articleIds = new HashSet<>();
        final Set<String> commentIds = new HashSet<>();
        final Map<String, Set<String>> commenterToCommentId = new HashMap<>();

        private Control(String category) {
            this.category = category;
        }
    }

}
