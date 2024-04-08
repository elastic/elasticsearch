/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public abstract class AbstractTermsTestCase extends ESIntegTestCase {

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    private static long sumOfDocCounts(Terms terms) {
        long sumOfDocCounts = terms.getSumOfOtherDocCounts();
        for (Terms.Bucket b : terms.getBuckets()) {
            sumOfDocCounts += b.getDocCount();
        }
        return sumOfDocCounts;
    }

    public void testOtherDocCount(String... fieldNames) {
        for (String fieldName : fieldNames) {
            assertResponse(
                prepareSearch("idx").addAggregation(
                    new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                        .field(fieldName)
                        .size(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                ),
                allTerms -> {
                    assertNoFailures(allTerms);

                    Terms terms = allTerms.getAggregations().get("terms");
                    assertEquals(0, terms.getSumOfOtherDocCounts()); // size is 0
                    final long sumOfDocCounts = sumOfDocCounts(terms);
                    final int totalNumTerms = terms.getBuckets().size();

                    for (int size = 1; size < totalNumTerms + 2; size += randomIntBetween(1, 5)) {
                        for (int shardSize = size; shardSize <= totalNumTerms + 2; shardSize += randomIntBetween(1, 5)) {
                            final int finalSize = size;
                            assertResponse(
                                prepareSearch("idx").addAggregation(
                                    new TermsAggregationBuilder("terms").executionHint(randomExecutionHint())
                                        .field(fieldName)
                                        .size(size)
                                        .shardSize(shardSize)
                                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                                ),
                                response -> {
                                    assertNoFailures(response);
                                    Terms innerTerms = response.getAggregations().get("terms");
                                    assertEquals(Math.min(finalSize, totalNumTerms), innerTerms.getBuckets().size());
                                    assertEquals(sumOfDocCounts, sumOfDocCounts(innerTerms));
                                }
                            );
                        }
                    }
                }
            );
        }
    }

}
