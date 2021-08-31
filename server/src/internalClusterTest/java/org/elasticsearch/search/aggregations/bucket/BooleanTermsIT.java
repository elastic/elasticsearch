/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BooleanTermsIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "b_value";
    private static final String MULTI_VALUED_FIELD_NAME = "b_values";

    static int numSingleTrues, numSingleFalses, numMultiTrues, numMultiFalses;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        ensureSearchable();
        final int numDocs = randomInt(5);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            final boolean singleValue = randomBoolean();
            if (singleValue) {
                numSingleTrues++;
            } else {
                numSingleFalses++;
            }
            final boolean[] multiValue;
            switch (randomInt(3)) {
                case 0:
                    multiValue = new boolean[0];
                    break;
                case 1:
                    numMultiFalses++;
                    multiValue = new boolean[] { false };
                    break;
                case 2:
                    numMultiTrues++;
                    multiValue = new boolean[] { true };
                    break;
                case 3:
                    numMultiFalses++;
                    numMultiTrues++;
                    multiValue = new boolean[] { false, true };
                    break;
                default:
                    throw new AssertionError();
            }
            builders[i] = client().prepareIndex("idx", "type")
                .setSource(
                    jsonBuilder().startObject()
                        .field(SINGLE_VALUED_FIELD_NAME, singleValue)
                        .array(MULTI_VALUED_FIELD_NAME, multiValue)
                        .endObject()
                );
        }
        indexRandom(true, builders);
    }

    public void testSingleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setTypes("type")
            .addAggregation(terms("terms").field(SINGLE_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values())))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        final int bucketCount = numSingleFalses > 0 && numSingleTrues > 0 ? 2 : numSingleFalses + numSingleTrues > 0 ? 1 : 0;
        assertThat(terms.getBuckets().size(), equalTo(bucketCount));

        Terms.Bucket bucket = terms.getBucketByKey("false");
        if (numSingleFalses == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numSingleFalses, bucket.getDocCount());
            assertEquals("false", bucket.getKeyAsString());
        }

        bucket = terms.getBucketByKey("true");
        if (numSingleTrues == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numSingleTrues, bucket.getDocCount());
            assertEquals("true", bucket.getKeyAsString());
        }
    }

    public void testMultiValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setTypes("type")
            .addAggregation(terms("terms").field(MULTI_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values())))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        final int bucketCount = numMultiFalses > 0 && numMultiTrues > 0 ? 2 : numMultiFalses + numMultiTrues > 0 ? 1 : 0;
        assertThat(terms.getBuckets().size(), equalTo(bucketCount));

        Terms.Bucket bucket = terms.getBucketByKey("false");
        if (numMultiFalses == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numMultiFalses, bucket.getDocCount());
            assertEquals("false", bucket.getKeyAsString());
        }

        bucket = terms.getBucketByKey("true");
        if (numMultiTrues == 0) {
            assertNull(bucket);
        } else {
            assertNotNull(bucket);
            assertEquals(numMultiTrues, bucket.getDocCount());
            assertEquals("true", bucket.getKeyAsString());
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .setTypes("type")
            .addAggregation(
                terms("terms").field(SINGLE_VALUED_FIELD_NAME).size(between(1, 5)).collectMode(randomFrom(SubAggCollectionMode.values()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(0));
    }
}
