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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

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
            SearchResponse allTerms = client().prepareSearch("idx")
                    .addAggregation(terms("terms")
                            .executionHint(randomExecutionHint())
                            .field(fieldName)
                            .size(10000)
                            .collectMode(randomFrom(SubAggCollectionMode.values())))
                    .get();
            assertSearchResponse(allTerms);

            Terms terms = allTerms.getAggregations().get("terms");
            assertEquals(0, terms.getSumOfOtherDocCounts()); // size is 0
            final long sumOfDocCounts = sumOfDocCounts(terms);
            final int totalNumTerms = terms.getBuckets().size();

            for (int size = 1; size < totalNumTerms + 2; size += randomIntBetween(1, 5)) {
                for (int shardSize = size; shardSize <= totalNumTerms + 2; shardSize += randomIntBetween(1, 5)) {
                    SearchResponse resp = client().prepareSearch("idx")
                            .addAggregation(terms("terms")
                                    .executionHint(randomExecutionHint())
                                    .field(fieldName)
                                    .size(size)
                                    .shardSize(shardSize)
                                    .collectMode(randomFrom(SubAggCollectionMode.values())))
                            .get();
                    assertSearchResponse(resp);
                    terms = resp.getAggregations().get("terms");
                    assertEquals(Math.min(size, totalNumTerms), terms.getBuckets().size());
                    assertEquals(sumOfDocCounts, sumOfDocCounts(terms));
                }
            }
        }
    }

}
