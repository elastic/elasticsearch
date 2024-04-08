/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class FlattenedFieldSearchTests extends ESSingleNodeTestCase {

    @Before
    public void setUpIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("flattened")
            .field("type", "flattened")
            .field("split_queries_on_whitespace", true)
            .endObject()
            .startObject("headers")
            .field("type", "flattened")
            .field("split_queries_on_whitespace", true)
            .endObject()
            .startObject("labels")
            .field("type", "flattened")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("test", Settings.EMPTY, mapping);
    }

    public void testMatchQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("headers")
                    .field("content-type", "application/json")
                    .field("origin", "https://www.elastic.co")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch().setQuery(matchQuery("headers", "application/json")), 1L);

        // Check that queries are split on whitespace.
        assertHitCount(client().prepareSearch().setQuery(matchQuery("headers.content-type", "application/json text/plain")), 1L);
        assertHitCount(client().prepareSearch().setQuery(matchQuery("headers.origin", "application/json")), 0L);
    }

    public void testMultiMatchQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("headers")
                    .field("content-type", "application/json")
                    .field("origin", "https://www.elastic.co")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch().setQuery(multiMatchQuery("application/json", "headers")), 1L);
        assertHitCount(client().prepareSearch().setQuery(multiMatchQuery("application/json text/plain", "headers.content-type")), 1L);
        assertHitCount(client().prepareSearch().setQuery(multiMatchQuery("application/json", "headers.origin")), 0L);
        assertHitCount(client().prepareSearch().setQuery(multiMatchQuery("application/json", "headers.origin", "headers.contentType")), 0L);
    }

    public void testQueryStringQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("flattened")
                    .field("field1", "value")
                    .field("field2", "2.718")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(queryStringQuery("flattened.field1:value")), 1);
        assertHitCountAndNoFailures(
            client().prepareSearch("test").setQuery(queryStringQuery("flattened.field1:value AND flattened:2.718")),
            1
        );
        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(queryStringQuery("2.718").field("flattened.field2")), 1);
    }

    public void testSimpleQueryStringQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("flattened")
                    .field("field1", "value")
                    .field("field2", "2.718")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(simpleQueryStringQuery("value").field("flattened.field1")), 1);
        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(simpleQueryStringQuery("+value +2.718").field("flattened")), 1);
        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(simpleQueryStringQuery("+value +3.141").field("flattened")), 0);
    }

    public void testExists() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("headers")
                    .field("content-type", "application/json")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch().setQuery(existsQuery("headers")), 1L);
        assertHitCount(client().prepareSearch().setQuery(existsQuery("headers.content-type")), 1L);
        assertHitCount(client().prepareSearch().setQuery(existsQuery("headers.nonexistent")), 0L);
    }

    public void testCardinalityAggregation() throws IOException {
        int numDocs = randomIntBetween(2, 100);
        int precisionThreshold = randomIntBetween(0, 1 << randomInt(20));

        BulkRequestBuilder bulkRequest = client().prepareBulk("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);

        // Add a random number of documents containing a flattened field, plus
        // a small number of dummy documents.
        for (int i = 0; i < numDocs; ++i) {
            bulkRequest.add(
                client().prepareIndex()
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("flattened")
                            .field("first", i)
                            .field("second", i / 2)
                            .endObject()
                            .endObject()
                    )
            );
        }

        for (int i = 0; i < 10; i++) {
            bulkRequest.add(prepareIndex("test").setSource("other_field", "1"));
        }

        BulkResponse bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        // Test the root flattened field.
        assertNoFailuresAndResponse(
            client().prepareSearch("test")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("flattened")),
            response -> {
                Cardinality count = response.getAggregations().get("cardinality");
                assertCardinality(count, numDocs, precisionThreshold);
            }
        );

        // Test two keyed flattened fields.
        assertNoFailuresAndResponse(
            client().prepareSearch("test")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("flattened.first")),
            firstResponse -> {

                Cardinality firstCount = firstResponse.getAggregations().get("cardinality");
                assertCardinality(firstCount, numDocs, precisionThreshold);
            }
        );

        assertNoFailuresAndResponse(
            client().prepareSearch("test")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("flattened.second")),
            secondResponse -> {
                Cardinality secondCount = secondResponse.getAggregations().get("cardinality");
                assertCardinality(secondCount, (numDocs + 1) / 2, precisionThreshold);
            }
        );
    }

    private void assertCardinality(Cardinality count, long value, int precisionThreshold) {
        if (value <= precisionThreshold) {
            // linear counting should be picked, and should be accurate
            assertEquals(value, count.getValue());
        } else {
            // error is not bound, so let's just make sure it is > 0
            assertThat(count.getValue(), greaterThan(0L));
        }
    }

    public void testTermsAggregation() throws IOException {
        BulkRequestBuilder bulkRequest = client().prepareBulk("test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 5; i++) {
            bulkRequest.add(
                client().prepareIndex()
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("labels")
                            .field("priority", "urgent")
                            .field("release", "v1.2." + i)
                            .endObject()
                            .endObject()
                    )
            );
        }

        BulkResponse bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        // Aggregate on the root 'labels' field.
        TermsAggregationBuilder builder = createTermsAgg("labels");
        assertNoFailuresAndResponse(client().prepareSearch("test").addAggregation(builder), response -> {
            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("terms"));
            assertThat(terms.getBuckets().size(), equalTo(6));

            Terms.Bucket bucket1 = terms.getBuckets().get(0);
            assertEquals("urgent", bucket1.getKey());
            assertEquals(5, bucket1.getDocCount());

            Terms.Bucket bucket2 = terms.getBuckets().get(1);
            assertThat(bucket2.getKeyAsString(), startsWith("v1.2."));
            assertEquals(1, bucket2.getDocCount());
        });

        // Aggregate on the 'priority' subfield.
        TermsAggregationBuilder priorityAgg = createTermsAgg("labels.priority");
        assertNoFailuresAndResponse(client().prepareSearch("test").addAggregation(priorityAgg), priorityResponse -> {
            Terms priorityTerms = priorityResponse.getAggregations().get("terms");
            assertThat(priorityTerms, notNullValue());
            assertThat(priorityTerms.getName(), equalTo("terms"));
            assertThat(priorityTerms.getBuckets().size(), equalTo(1));

            Terms.Bucket priorityBucket = priorityTerms.getBuckets().get(0);
            assertEquals("urgent", priorityBucket.getKey());
            assertEquals(5, priorityBucket.getDocCount());
        });

        // Aggregate on the 'release' subfield.
        TermsAggregationBuilder releaseAgg = createTermsAgg("labels.release");
        assertNoFailuresAndResponse(client().prepareSearch("test").addAggregation(releaseAgg), releaseResponse -> {
            Terms releaseTerms = releaseResponse.getAggregations().get("terms");
            assertThat(releaseTerms, notNullValue());
            assertThat(releaseTerms.getName(), equalTo("terms"));
            assertThat(releaseTerms.getBuckets().size(), equalTo(5));

            for (Terms.Bucket bucket : releaseTerms.getBuckets()) {
                assertThat(bucket.getKeyAsString(), startsWith("v1.2."));
                assertEquals(1, bucket.getDocCount());
            }
        });

        // Aggregate on the 'priority' subfield with a min_doc_count of 0.
        TermsAggregationBuilder minDocCountAgg = createTermsAgg("labels.priority").minDocCount(0);
        assertNoFailuresAndResponse(client().prepareSearch("test").addAggregation(minDocCountAgg), minDocCountResponse -> {
            Terms minDocCountTerms = minDocCountResponse.getAggregations().get("terms");
            assertThat(minDocCountTerms, notNullValue());
            assertThat(minDocCountTerms.getName(), equalTo("terms"));
            assertThat(minDocCountTerms.getBuckets().size(), equalTo(1));
        });
    }

    private TermsAggregationBuilder createTermsAgg(String field) {
        TermsAggregatorFactory.ExecutionMode executionMode = randomFrom(TermsAggregatorFactory.ExecutionMode.values());
        Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());

        return terms("terms").field(field).collectMode(collectionMode).executionHint(executionMode.toString());
    }

    public void testLoadDocValuesFields() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("flattened")
                    .field("key", "value")
                    .field("other_key", "other_value")
                    .endObject()
                    .endObject()
            )
            .get();

        assertNoFailuresAndResponse(
            client().prepareSearch("test").addDocValueField("flattened").addDocValueField("flattened.key"),
            response -> {
                assertHitCount(response, 1);

                Map<String, DocumentField> fields = response.getHits().getAt(0).getFields();

                DocumentField field = fields.get("flattened");
                assertEquals("flattened", field.getName());
                assertEquals(Arrays.asList("other_value", "value"), field.getValues());

                DocumentField keyedField = fields.get("flattened.key");
                assertEquals("flattened.key", keyedField.getName());
                assertEquals("value", keyedField.getValue());
            }
        );
    }

    public void testFieldSort() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("flattened")
                    .field("key", "A")
                    .field("other_key", "D")
                    .endObject()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("2")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("flattened")
                    .field("key", "B")
                    .field("other_key", "C")
                    .endObject()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("3")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder().startObject().startObject("flattened").field("other_key", "E").endObject().endObject())
            .get();

        assertNoFailuresAndResponse(client().prepareSearch("test").addSort("flattened", SortOrder.DESC), response -> {
            assertHitCount(response, 3);
            assertOrderedSearchHits(response, "3", "1", "2");
        });
        assertNoFailuresAndResponse(client().prepareSearch("test").addSort("flattened.key", SortOrder.DESC), response -> {
            assertHitCount(response, 3);
            assertOrderedSearchHits(response, "2", "1", "3");
        });

        assertNoFailuresAndResponse(
            client().prepareSearch("test").addSort(new FieldSortBuilder("flattened.key").order(SortOrder.DESC).missing("Z")),
            response -> {
                assertHitCount(response, 3);
                assertOrderedSearchHits(response, "3", "2", "1");
            }
        );
    }

    public void testSourceFiltering() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("content-type", "application/json");
        headers.put("origin", "https://www.elastic.co");
        Map<String, Object> source = Collections.singletonMap("headers", headers);

        prepareIndex("test").setId("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource(source).get();

        assertResponse(
            client().prepareSearch("test").setFetchSource(true),
            response -> assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(source))
        );

        // Check 'include' filtering.
        assertResponse(
            client().prepareSearch("test").setFetchSource("headers", null),
            response -> assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(source))
        );

        assertResponse(client().prepareSearch("test").setFetchSource("headers.content-type", null), response -> {
            Map<String, Object> filteredSource = Collections.singletonMap(
                "headers",
                Collections.singletonMap("content-type", "application/json")
            );
            assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(filteredSource));
        });

        // Check 'exclude' filtering.
        assertResponse(
            client().prepareSearch("test").setFetchSource(null, "headers.content-type"),
            response -> assertThat(
                response.getHits().getAt(0).getSourceAsMap(),
                equalTo(Collections.singletonMap("headers", Collections.singletonMap("origin", "https://www.elastic.co")))
            )
        );
    }
}
