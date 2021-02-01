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
package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.join.aggregations.JoinAggregationBuilders.children;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ChildrenIT extends AbstractParentChildTestCase {

    public void testChildrenAggs() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("randomized", true))
                .addAggregation(
                        terms("category").field("category").size(10000).subAggregation(children("to_comment", "comment")
                                .subAggregation(
                                        terms("commenters").field("commenter").size(10000).subAggregation(
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
            assertThat(((InternalAggregation)childrenBucket).getProperty("_count"),
                equalTo((long) entry1.getValue().commentIds.size()));

            Terms commentersTerms = childrenBucket.getAggregations().get("commenters");
            assertThat(((InternalAggregation)childrenBucket).getProperty("commenters"), sameInstance(commentersTerms));
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

    public void testParentWithMultipleBuckets() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("randomized", false))
                .addAggregation(
                        terms("category").field("category").size(10000).subAggregation(
                        children("to_comment", "comment").subAggregation(topHits("top_comments").sort("id", SortOrder.ASC))
                        )
                ).get();
        assertSearchResponse(searchResponse);

        Terms categoryTerms = searchResponse.getAggregations().get("category");
        assertThat(categoryTerms.getBuckets().size(), equalTo(3));

        for (Terms.Bucket bucket : categoryTerms.getBuckets()) {
            logger.info("bucket={}", bucket.getKey());
            Children childrenBucket = bucket.getAggregations().get("to_comment");
            TopHits topHits = childrenBucket.getAggregations().get("top_comments");
            logger.info("total_hits={}", topHits.getHits().getTotalHits().value);
            for (SearchHit searchHit : topHits.getHits()) {
                logger.info("hit= {} {}", searchHit.getSortValues()[0], searchHit.getId());
            }
        }

        Terms.Bucket categoryBucket = categoryTerms.getBucketByKey("a");
        assertThat(categoryBucket.getKeyAsString(), equalTo("a"));
        assertThat(categoryBucket.getDocCount(), equalTo(3L));

        Children childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(2L));
        TopHits topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().getTotalHits().value, equalTo(2L));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("e"));
        assertThat(topHits.getHits().getAt(1).getId(), equalTo("f"));

        categoryBucket = categoryTerms.getBucketByKey("b");
        assertThat(categoryBucket.getKeyAsString(), equalTo("b"));
        assertThat(categoryBucket.getDocCount(), equalTo(2L));

        childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(1L));
        topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().getTotalHits().value, equalTo(1L));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("f"));

        categoryBucket = categoryTerms.getBucketByKey("c");
        assertThat(categoryBucket.getKeyAsString(), equalTo("c"));
        assertThat(categoryBucket.getDocCount(), equalTo(2L));

        childrenBucket = categoryBucket.getAggregations().get("to_comment");
        assertThat(childrenBucket.getName(), equalTo("to_comment"));
        assertThat(childrenBucket.getDocCount(), equalTo(1L));
        topHits = childrenBucket.getAggregations().get("top_comments");
        assertThat(topHits.getHits().getTotalHits().value, equalTo(1L));
        assertThat(topHits.getHits().getAt(0).getId(), equalTo("f"));
    }

    public void testWithDeletes() throws Exception {
        String indexName = "xyz";
        assertAcked(
                prepareCreate(indexName)
                    .setMapping(
                        addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true, "parent", "child"),
                            "name", "keyword"))
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest(indexName, "parent", "1", null));
        requests.add(createIndexRequest(indexName, "child", "2", "1", "count", 1));
        requests.add(createIndexRequest(indexName, "child", "3", "1", "count", 1));
        requests.add(createIndexRequest(indexName, "child", "4", "1", "count", 1));
        requests.add(createIndexRequest(indexName, "child", "5", "1", "count", 1));
        indexRandom(true, requests);

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch(indexName)
                    .addAggregation(children("children", "child").subAggregation(sum("counts").field("count")))
                    .get();

            assertNoFailures(searchResponse);
            Children children = searchResponse.getAggregations().get("children");
            assertThat(children.getDocCount(), equalTo(4L));

            Sum count = children.getAggregations().get("counts");
            assertThat(count.getValue(), equalTo(4.));

            String idToUpdate = Integer.toString(2 + randomInt(3));
            /*
             * The whole point of this test is to test these things with deleted
             * docs in the index so we turn off detect_noop to make sure that
             * the updates cause that.
             */
            UpdateResponse updateResponse;
            updateResponse = client().prepareUpdate(indexName, idToUpdate)
                    .setRouting("1")
                    .setDoc(Requests.INDEX_CONTENT_TYPE, "count", 1)
                    .setDetectNoop(false)
                    .get();
            assertThat(updateResponse.getVersion(), greaterThan(1L));
            refresh();
        }
    }

    public void testNonExistingChildType() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .addAggregation(
                    children("non-existing", "xyz")
                ).get();
        assertSearchResponse(searchResponse);

        Children children = searchResponse.getAggregations().get("non-existing");
        assertThat(children.getName(), equalTo("non-existing"));
        assertThat(children.getDocCount(), equalTo(0L));
    }

    public void testPostCollection() throws Exception {
        String indexName = "prodcatalog";
        String masterType = "masterprod";
        String childType = "variantsku";
        assertAcked(
                prepareCreate(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                    .setMapping(
                        addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true,
                            masterType, childType),
                            "brand", "text", "name", "keyword", "material", "text", "color", "keyword", "size", "keyword"))
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest(indexName, masterType, "1", null, "brand", "Levis", "name",
                "Style 501", "material", "Denim"));
        requests.add(createIndexRequest(indexName, childType, "3", "1", "color", "blue", "size", "32"));
        requests.add(createIndexRequest(indexName, childType, "4", "1", "color", "blue", "size", "34"));
        requests.add(createIndexRequest(indexName, childType, "5", "1", "color", "blue", "size", "36"));
        requests.add(createIndexRequest(indexName, childType, "6", "1", "color", "black", "size", "38"));
        requests.add(createIndexRequest(indexName, childType, "7", "1", "color", "black", "size", "40"));
        requests.add(createIndexRequest(indexName, childType, "8", "1", "color", "gray", "size", "36"));

        requests.add(createIndexRequest(indexName, masterType, "2", null, "brand", "Wrangler", "name",
                "Regular Cut", "material", "Leather"));
        requests.add(createIndexRequest(indexName, childType, "9", "2", "color", "blue", "size", "32"));
        requests.add(createIndexRequest(indexName, childType, "10", "2", "color", "blue", "size", "34"));
        requests.add(createIndexRequest(indexName, childType, "12", "2", "color", "black", "size", "36"));
        requests.add(createIndexRequest(indexName, childType, "13", "2", "color", "black", "size", "38"));
        requests.add(createIndexRequest(indexName, childType, "14", "2", "color", "black", "size", "40"));
        requests.add(createIndexRequest(indexName, childType, "15", "2", "color", "orange", "size", "36"));
        requests.add(createIndexRequest(indexName, childType, "16", "2", "color", "green", "size", "44"));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch(indexName)
                .setQuery(hasChildQuery(childType, termQuery("color", "orange"), ScoreMode.None))
                        .addAggregation(children("my-refinements", childType)
                                .subAggregation(terms("my-colors").field("color"))
                                .subAggregation(terms("my-sizes").field("size"))
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        Children childrenAgg = response.getAggregations().get("my-refinements");
        assertThat(childrenAgg.getDocCount(), equalTo(7L));

        Terms termsAgg = childrenAgg.getAggregations().get("my-colors");
        assertThat(termsAgg.getBuckets().size(), equalTo(4));
        assertThat(termsAgg.getBucketByKey("black").getDocCount(), equalTo(3L));
        assertThat(termsAgg.getBucketByKey("blue").getDocCount(), equalTo(2L));
        assertThat(termsAgg.getBucketByKey("green").getDocCount(), equalTo(1L));
        assertThat(termsAgg.getBucketByKey("orange").getDocCount(), equalTo(1L));

        termsAgg = childrenAgg.getAggregations().get("my-sizes");
        assertThat(termsAgg.getBuckets().size(), equalTo(6));
        assertThat(termsAgg.getBucketByKey("36").getDocCount(), equalTo(2L));
        assertThat(termsAgg.getBucketByKey("32").getDocCount(), equalTo(1L));
        assertThat(termsAgg.getBucketByKey("34").getDocCount(), equalTo(1L));
        assertThat(termsAgg.getBucketByKey("38").getDocCount(), equalTo(1L));
        assertThat(termsAgg.getBucketByKey("40").getDocCount(), equalTo(1L));
        assertThat(termsAgg.getBucketByKey("44").getDocCount(), equalTo(1L));
    }

    public void testHierarchicalChildrenAggs() {
        String indexName = "geo";
        String grandParentType = "continent";
        String parentType = "country";
        String childType = "city";
        assertAcked(
                prepareCreate(indexName)
                    .setMapping(
                        addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true,
                            grandParentType, parentType, parentType, childType),
                            "name", "keyword"))
        );

        createIndexRequest(indexName, grandParentType, "1", null, "name", "europe").get();
        createIndexRequest(indexName, parentType, "2", "1", "name", "belgium").get();
        createIndexRequest(indexName, childType, "3", "2", "name", "brussels").setRouting("1").get();
        refresh();

        SearchResponse response = client().prepareSearch(indexName)
                .setQuery(matchQuery("name", "europe"))
                .addAggregation(
                children(parentType, parentType).subAggregation(children(childType, childType).subAggregation(
                                        terms("name").field("name")
                                )
                        )
                )
                .get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        Children children = response.getAggregations().get(parentType);
        assertThat(children.getName(), equalTo(parentType));
        assertThat(children.getDocCount(), equalTo(1L));
        children = children.getAggregations().get(childType);
        assertThat(children.getName(), equalTo(childType));
        assertThat(children.getDocCount(), equalTo(1L));
        Terms terms = children.getAggregations().get("name");
        assertThat(terms.getBuckets().size(), equalTo(1));
        assertThat(terms.getBuckets().get(0).getKey().toString(), equalTo("brussels"));
        assertThat(terms.getBuckets().get(0).getDocCount(), equalTo(1L));
    }

    public void testPostCollectAllLeafReaders() throws Exception {
        // The 'towns' and 'parent_names' aggs operate on parent docs and if child docs are in different segments we need
        // to ensure those segments which child docs are also evaluated to in the post collect phase.

        // Before we only evaluated segments that yielded matches in 'towns' and 'parent_names' aggs, which caused
        // us to miss to evaluate child docs in segments we didn't have parent matches for.
        assertAcked(
                prepareCreate("index")
                    .setMapping(
                        addFieldMappings(buildParentJoinFieldMappingFromSimplifiedDef("join_field", true,
                            "parentType", "childType"),
                            "name", "keyword", "town", "keyword", "age", "integer"))
        );
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(createIndexRequest("index", "parentType", "1", null, "name", "Bob", "town", "Memphis"));
        requests.add(createIndexRequest("index", "parentType", "2", null, "name", "Alice", "town", "Chicago"));
        requests.add(createIndexRequest("index", "parentType", "3", null, "name", "Bill", "town", "Chicago"));
        requests.add(createIndexRequest("index", "childType", "4", "1", "name", "Jill", "age", 5));
        requests.add(createIndexRequest("index", "childType", "5", "1", "name", "Joey", "age", 3));
        requests.add(createIndexRequest("index", "childType", "6", "2", "name", "John", "age", 2));
        requests.add(createIndexRequest("index", "childType", "7", "3", "name", "Betty", "age", 6));
        requests.add(createIndexRequest("index", "childType", "8", "3", "name", "Dan", "age", 1));
        indexRandom(true, requests);

        SearchResponse response = client().prepareSearch("index")
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("towns").field("town")
                .subAggregation(AggregationBuilders.terms("parent_names").field("name")
                    .subAggregation(children("child_docs", "childType"))
                )
            ).get();

        Terms towns = response.getAggregations().get("towns");
        assertThat(towns.getBuckets().size(), equalTo(2));
        assertThat(towns.getBuckets().get(0).getKeyAsString(), equalTo("Chicago"));
        assertThat(towns.getBuckets().get(0).getDocCount(), equalTo(2L));

        Terms parents = towns.getBuckets().get(0).getAggregations().get("parent_names");
        assertThat(parents.getBuckets().size(), equalTo(2));
        assertThat(parents.getBuckets().get(0).getKeyAsString(), equalTo("Alice"));
        assertThat(parents.getBuckets().get(0).getDocCount(), equalTo(1L));
        Children children = parents.getBuckets().get(0).getAggregations().get("child_docs");
        assertThat(children.getDocCount(), equalTo(1L));

        assertThat(parents.getBuckets().get(1).getKeyAsString(), equalTo("Bill"));
        assertThat(parents.getBuckets().get(1).getDocCount(), equalTo(1L));
        children = parents.getBuckets().get(1).getAggregations().get("child_docs");
        assertThat(children.getDocCount(), equalTo(2L));

        assertThat(towns.getBuckets().get(1).getKeyAsString(), equalTo("Memphis"));
        assertThat(towns.getBuckets().get(1).getDocCount(), equalTo(1L));
        parents = towns.getBuckets().get(1).getAggregations().get("parent_names");
        assertThat(parents.getBuckets().size(), equalTo(1));
        assertThat(parents.getBuckets().get(0).getKeyAsString(), equalTo("Bob"));
        assertThat(parents.getBuckets().get(0).getDocCount(), equalTo(1L));
        children = parents.getBuckets().get(0).getAggregations().get("child_docs");
        assertThat(children.getDocCount(), equalTo(2L));
    }
}
