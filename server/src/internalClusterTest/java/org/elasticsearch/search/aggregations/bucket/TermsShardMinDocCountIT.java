/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class TermsShardMinDocCountIT extends ESIntegTestCase {
    private static final String index = "someindex";

    private static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }

    @Override
    protected boolean enableConcurrentSearch() {
        return false;
    }

    // see https://github.com/elastic/elasticsearch/issues/5998
    public void testShardMinDocCountSignificantTermsTest() throws Exception {
        String textMappings;
        if (randomBoolean()) {
            textMappings = "type=long";
        } else {
            textMappings = "type=text,fielddata=true";
        }
        assertAcked(prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping("text", textMappings));
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        addTermsDocs("1", 1, 0, indexBuilders);// high score but low doc freq
        addTermsDocs("2", 1, 0, indexBuilders);
        addTermsDocs("3", 1, 0, indexBuilders);
        addTermsDocs("4", 1, 0, indexBuilders);
        addTermsDocs("5", 3, 1, indexBuilders);// low score but high doc freq
        addTermsDocs("6", 3, 1, indexBuilders);
        addTermsDocs("7", 0, 3, indexBuilders);// make sure the terms all get score > 0 except for this one
        indexRandom(true, false, indexBuilders);

        // first, check that indeed when not setting the shardMinDocCount parameter 0 terms are returned
        assertNoFailuresAndResponse(
            prepareSearch(index).addAggregation(
                (filter("inclass", QueryBuilders.termQuery("class", true))).subAggregation(
                    significantTerms("mySignificantTerms").field("text")
                        .minDocCount(2)
                        .size(2)
                        .shardSize(2)
                        .executionHint(randomExecutionHint())
                )
            ),
            response -> {
                InternalFilter filteredBucket = response.getAggregations().get("inclass");
                SignificantTerms sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
                assertThat(sigterms.getBuckets().size(), equalTo(0));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch(index).addAggregation(
                (filter("inclass", QueryBuilders.termQuery("class", true))).subAggregation(
                    significantTerms("mySignificantTerms").field("text")
                        .minDocCount(2)
                        .shardSize(2)
                        .shardMinDocCount(2)
                        .size(2)
                        .executionHint(randomExecutionHint())
                )
            ),
            response -> {
                assertNoFailures(response);
                InternalFilter filteredBucket = response.getAggregations().get("inclass");
                SignificantTerms sigterms = filteredBucket.getAggregations().get("mySignificantTerms");
                assertThat(sigterms.getBuckets().size(), equalTo(2));
            }
        );
    }

    private void addTermsDocs(String term, int numInClass, int numNotInClass, List<IndexRequestBuilder> builders) {
        String sourceClass = "{\"text\": \"" + term + "\", \"class\":" + "true" + "}";
        String sourceNotClass = "{\"text\": \"" + term + "\", \"class\":" + "false" + "}";
        for (int i = 0; i < numInClass; i++) {
            builders.add(prepareIndex(index).setSource(sourceClass, XContentType.JSON));
        }
        for (int i = 0; i < numNotInClass; i++) {
            builders.add(prepareIndex(index).setSource(sourceNotClass, XContentType.JSON));
        }
    }

    // see https://github.com/elastic/elasticsearch/issues/5998
    public void testShardMinDocCountTermsTest() throws Exception {
        final String[] termTypes = { "text", "long", "integer", "float", "double" };
        String termtype = termTypes[randomInt(termTypes.length - 1)];
        String termMappings = "type=" + termtype;
        if (termtype.equals("text")) {
            termMappings += ",fielddata=true";
        }
        assertAcked(prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping("text", termMappings));
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        addTermsDocs("1", 1, indexBuilders);// low doc freq but high score
        addTermsDocs("2", 1, indexBuilders);
        addTermsDocs("3", 1, indexBuilders);
        addTermsDocs("4", 1, indexBuilders);
        addTermsDocs("5", 3, indexBuilders);// low score but high doc freq
        addTermsDocs("6", 3, indexBuilders);
        indexRandom(true, false, indexBuilders);

        // first, check that indeed when not setting the shardMinDocCount parameter 0 terms are returned
        assertNoFailuresAndResponse(
            prepareSearch(index).addAggregation(
                terms("myTerms").field("text")
                    .minDocCount(2)
                    .size(2)
                    .shardSize(2)
                    .executionHint(randomExecutionHint())
                    .order(BucketOrder.key(true))
            ),
            response -> {
                Terms sigterms = response.getAggregations().get("myTerms");
                assertThat(sigterms.getBuckets().size(), equalTo(0));
            }
        );

        assertNoFailuresAndResponse(
            prepareSearch(index).addAggregation(
                terms("myTerms").field("text")
                    .minDocCount(2)
                    .shardMinDocCount(2)
                    .size(2)
                    .shardSize(2)
                    .executionHint(randomExecutionHint())
                    .order(BucketOrder.key(true))
            ),
            response -> {
                Terms sigterms = response.getAggregations().get("myTerms");
                assertThat(sigterms.getBuckets().size(), equalTo(2));
            }
        );
    }

    private static void addTermsDocs(String term, int numDocs, List<IndexRequestBuilder> builders) {
        String sourceClass = "{\"text\": \"" + term + "\"}";
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(index).setSource(sourceClass, XContentType.JSON));
        }
    }
}
