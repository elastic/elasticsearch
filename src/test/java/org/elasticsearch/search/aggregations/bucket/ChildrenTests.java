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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class ChildrenTests extends ElasticsearchIntegrationTest {

    private final static Map<String, Control> categoryToControl = new HashMap<>();

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
                prepareCreate("test")
                    .addMapping("article", "_id", "index=not_analyzed")
                    .addMapping("comment", "_parent", "type=article", "_id", "index=not_analyzed")
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
            assertThat(categoryBucket.getKey(), equalTo(entry1.getKey()));
            assertThat(categoryBucket.getDocCount(), equalTo((long) entry1.getValue().articleIds.size()));

            Children childrenBucket = categoryBucket.getAggregations().get("to_comment");
            assertThat(childrenBucket.getName(), equalTo("to_comment"));
            assertThat(childrenBucket.getDocCount(), equalTo((long) entry1.getValue().commentIds.size()));

            Terms commentersTerms = childrenBucket.getAggregations().get("commenters");
            assertThat(commentersTerms.getBuckets().size(), equalTo(entry1.getValue().commenterToCommentId.size()));
            for (Map.Entry<String, Set<String>> entry2 : entry1.getValue().commenterToCommentId.entrySet()) {
                Terms.Bucket commentBucket = commentersTerms.getBucketByKey(entry2.getKey());
                assertThat(commentBucket.getKey(), equalTo(entry2.getKey()));
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
                                children("to_comment").childType("comment").subAggregation(topHits("top_comments").addSort("_id", SortOrder.ASC))
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
        assertThat(categoryBucket.getKey(), equalTo("a"));
        assertThat(categoryBucket.getDocCount(), equalTo(3l));

        Children childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(2l));
        TopHits topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().totalHits(), equalTo(2l));
        assertThat(topHits.getHits().getAt(0).sortValues()[0].toString(), equalTo("a"));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("a"));
        assertThat(topHits.getHits().getAt(0).getType(), equalTo("comment"));
        assertThat(topHits.getHits().getAt(1).sortValues()[0].toString(), equalTo("c"));
        assertThat(topHits.getHits().getAt(1).getId(), equalTo("c"));
        assertThat(topHits.getHits().getAt(1).getType(), equalTo("comment"));

        categoryBucket = categoryTerms.getBucketByKey("b");
        assertThat(categoryBucket.getKey(), equalTo("b"));
        assertThat(categoryBucket.getDocCount(), equalTo(2l));

        childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(1l));
        topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().totalHits(), equalTo(1l));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("c"));
        assertThat(topHits.getHits().getAt(0).getType(), equalTo("comment"));

        categoryBucket = categoryTerms.getBucketByKey("c");
        assertThat(categoryBucket.getKey(), equalTo("c"));
        assertThat(categoryBucket.getDocCount(), equalTo(2l));

        childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(1l));
        topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().totalHits(), equalTo(1l));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("c"));
        assertThat(topHits.getHits().getAt(0).getType(), equalTo("comment"));
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
