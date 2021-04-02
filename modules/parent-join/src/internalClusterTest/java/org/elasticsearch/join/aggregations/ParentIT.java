/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.aggregations;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.join.aggregations.JoinAggregationBuilders.parent;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class ParentIT extends AbstractParentChildTestCase {

    public void testSimpleParentAgg() throws Exception {
        final SearchRequestBuilder searchRequest = client().prepareSearch("test")
            .setSize(10000)
            .setQuery(matchQuery("randomized", true))
            .addAggregation(
                parent("to_article", "comment")
                    .subAggregation(
                        terms("category").field("category").size(10000)));
        SearchResponse searchResponse = searchRequest.get();
        assertSearchResponse(searchResponse);

        long articlesWithComment = articleToControl.values().stream().filter(
            parentControl -> parentControl.commentIds.isEmpty() == false
        ).count();

        Parent parentAgg = searchResponse.getAggregations().get("to_article");
        assertThat("Request: " + searchRequest + "\nResponse: " + searchResponse + "\n",
            parentAgg.getDocCount(), equalTo(articlesWithComment));
        Terms categoryTerms = parentAgg.getAggregations().get("category");
        long categoriesWithComments = categoryToControl.values().stream().filter(
            control -> control.commentIds.isEmpty() == false).count();
        assertThat("Buckets: " + categoryTerms.getBuckets().stream().map(
            (Function<Terms.Bucket, String>) MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList()) +
                "\nCategories: " + categoryToControl.keySet(),
            (long)categoryTerms.getBuckets().size(), equalTo(categoriesWithComments));
        for (Map.Entry<String, Control> entry : categoryToControl.entrySet()) {
            // no children for this category -> no entry in the child to parent-aggregation
            if(entry.getValue().commentIds.isEmpty()) {
                assertNull(categoryTerms.getBucketByKey(entry.getKey()));
                continue;
            }

            final Terms.Bucket categoryBucket = categoryTerms.getBucketByKey(entry.getKey());
            assertNotNull("Failed for category " + entry.getKey(),
                categoryBucket);
            assertThat("Failed for category " + entry.getKey(),
                categoryBucket.getKeyAsString(), equalTo(entry.getKey()));

            // count all articles in this category which have at least one comment
            long articlesForCategory = articleToControl.values().stream()
                // only articles with this category
                .filter(parentControl -> parentControl.category.equals(entry.getKey()))
                // only articles which have comments
                .filter(parentControl -> parentControl.commentIds.isEmpty() == false)
                .count();
            assertThat("Failed for category " + entry.getKey(),
                categoryBucket.getDocCount(), equalTo(articlesForCategory));
        }
    }

    public void testParentAggs() throws Exception {
        final SearchRequestBuilder searchRequest = client().prepareSearch("test")
            .setSize(10000)
            .setQuery(matchQuery("randomized", true))
            .addAggregation(
                terms("to_commenter").field("commenter").size(10000).subAggregation(
                    parent("to_article", "comment").subAggregation(
                        terms("to_category").field("category").size(10000).subAggregation(
                            topHits("top_category")
                        ))
                )
            );
        SearchResponse searchResponse = searchRequest.get();
        assertSearchResponse(searchResponse);

        final Set<String> commenters = getCommenters();
        final Map<String, Set<String>> commenterToComments = getCommenterToComments();

        Terms categoryTerms = searchResponse.getAggregations().get("to_commenter");
        assertThat("Request: " + searchRequest + "\nResponse: " + searchResponse + "\n",
            categoryTerms.getBuckets().size(), equalTo(commenters.size()));
        for (Terms.Bucket commenterBucket : categoryTerms.getBuckets()) {
            Set<String> comments = commenterToComments.get(commenterBucket.getKeyAsString());
            assertNotNull(comments);
            assertThat("Failed for commenter " + commenterBucket.getKeyAsString(),
                commenterBucket.getDocCount(), equalTo((long)comments.size()));

            Parent articleAgg = commenterBucket.getAggregations().get("to_article");
            assertThat(articleAgg.getName(), equalTo("to_article"));
            // find all articles for the comments for the current commenter
            Set<String> articles = articleToControl.values().stream().flatMap(
                (Function<ParentControl, Stream<String>>) parentControl -> parentControl.commentIds.stream().
                    filter(comments::contains)
            ).collect(Collectors.toSet());

            assertThat(articleAgg.getDocCount(), equalTo((long)articles.size()));

            Terms categoryAgg = articleAgg.getAggregations().get("to_category");
            assertNotNull(categoryAgg);

            List<String> categories = categoryToControl.entrySet().
                stream().
                filter(entry -> entry.getValue().commenterToCommentId.containsKey(commenterBucket.getKeyAsString())).
                map(Map.Entry::getKey).
                collect(Collectors.toList());

            for (String category : categories) {
                Terms.Bucket categoryBucket = categoryAgg.getBucketByKey(category);
                assertNotNull(categoryBucket);

                Aggregation topCategory = categoryBucket.getAggregations().get("top_category");
                assertNotNull(topCategory);
            }
        }

        for (String commenter : commenters) {
            Terms.Bucket categoryBucket = categoryTerms.getBucketByKey(commenter);
            assertThat(categoryBucket.getKeyAsString(), equalTo(commenter));
            assertThat(categoryBucket.getDocCount(), equalTo((long) commenterToComments.get(commenter).size()));

            Parent childrenBucket = categoryBucket.getAggregations().get("to_article");
            assertThat(childrenBucket.getName(), equalTo("to_article"));
        }
    }

    private Set<String> getCommenters() {
        return categoryToControl.values().stream().flatMap(
                (Function<Control, Stream<String>>) control -> control.commenterToCommentId.keySet().stream()).
                collect(Collectors.toSet());
    }

    private Map<String, Set<String>> getCommenterToComments() {
        final Map<String, Set<String>> commenterToComments = new HashMap<>();
        for (Control control : categoryToControl.values()) {
            for (Map.Entry<String, Set<String>> entry : control.commenterToCommentId.entrySet()) {
                final Set<String> comments = commenterToComments.computeIfAbsent(entry.getKey(), s -> new HashSet<>());
                comments.addAll(entry.getValue());
            }
        }
        return commenterToComments;
    }

    public void testNonExistingParentType() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
            .addAggregation(
                parent("non-existing", "xyz")
            ).get();
        assertSearchResponse(searchResponse);

        Parent parent = searchResponse.getAggregations().get("non-existing");
        assertThat(parent.getName(), equalTo("non-existing"));
        assertThat(parent.getDocCount(), equalTo(0L));
    }

    public void testTermsParentAggTerms() throws Exception {
        final SearchRequestBuilder searchRequest = client().prepareSearch("test")
            .setSize(10000)
            .setQuery(matchQuery("randomized", true))
            .addAggregation(
                terms("to_commenter").field("commenter").size(10000).subAggregation(
                    parent("to_article", "comment").subAggregation(
                        terms("to_category").field("category").size(10000))));
        SearchResponse searchResponse = searchRequest.get();
        assertSearchResponse(searchResponse);

        final Set<String> commenters = getCommenters();
        final Map<String, Set<String>> commenterToComments = getCommenterToComments();

        Terms commentersAgg = searchResponse.getAggregations().get("to_commenter");
        assertThat("Request: " + searchRequest + "\nResponse: " + searchResponse + "\n",
            commentersAgg.getBuckets().size(), equalTo(commenters.size()));
        for (Terms.Bucket commenterBucket : commentersAgg.getBuckets()) {
            Set<String> comments = commenterToComments.get(commenterBucket.getKeyAsString());
            assertNotNull(comments);
            assertThat("Failed for commenter " + commenterBucket.getKeyAsString(),
                commenterBucket.getDocCount(), equalTo((long)comments.size()));

            Parent articleAgg = commenterBucket.getAggregations().get("to_article");
            assertThat(articleAgg.getName(), equalTo("to_article"));
            // find all articles for the comments for the current commenter
            Set<String> articles = articleToControl.values().stream().flatMap(
                (Function<ParentControl, Stream<String>>) parentControl -> parentControl.commentIds.stream().
                    filter(comments::contains)
            ).collect(Collectors.toSet());

            assertThat(articleAgg.getDocCount(), equalTo((long)articles.size()));

            Terms categoryAgg = articleAgg.getAggregations().get("to_category");
            assertNotNull(categoryAgg);

            List<String> categories = categoryToControl.entrySet().
                stream().
                    filter(entry -> entry.getValue().commenterToCommentId.containsKey(commenterBucket.getKeyAsString())).
                    map(Map.Entry::getKey).
                collect(Collectors.toList());

            for (String category : categories) {
                Terms.Bucket categoryBucket = categoryAgg.getBucketByKey(category);
                assertNotNull(categoryBucket);
            }
        }
    }
}
