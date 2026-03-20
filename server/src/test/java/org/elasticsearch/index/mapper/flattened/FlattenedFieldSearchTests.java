/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
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
import org.elasticsearch.xcontent.XContentType;
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
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
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

    public void testTermsAggregationWithDuplicateValues() throws IOException {
        String indexName = "test-noindex-terms-agg";
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("labels")
            .field("type", "flattened")
            .field("index", false)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex(indexName, Settings.EMPTY, mapping);

        prepareIndex(indexName).setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("labels")
                    .field("key1", "foo")
                    .field("key2", "foo")
                    .field("key3", "bar")
                    .endObject()
                    .endObject()
            )
            .get();

        TermsAggregationBuilder builder = terms("terms").field("labels");
        assertNoFailuresAndResponse(client().prepareSearch(indexName).addAggregation(builder), response -> {
            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getBuckets().size(), equalTo(2));

            Terms.Bucket barBucket = terms.getBuckets().get(0);
            assertEquals("bar", barBucket.getKey());
            assertEquals(1, barBucket.getDocCount());

            Terms.Bucket fooBucket = terms.getBuckets().get(1);
            assertEquals("foo", fooBucket.getKey());
            assertEquals(1, fooBucket.getDocCount());
        });
    }

    public void testNoRootDocValuesFieldWhenIndexFalse() throws Exception {
        String indexName = "test-noindex-no-root-dv";
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("labels")
            .field("type", "flattened")
            .field("index", false)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex(indexName, Settings.EMPTY, mapping);

        prepareIndex(indexName).setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("labels")
                    .field("key1", "value1")
                    .field("key2", "value2")
                    .endObject()
                    .endObject()
            )
            .get();

        var indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(indexName));
        var shard = indexService.getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            for (var leaf : searcher.getDirectoryReader().leaves()) {
                for (FieldInfo fieldInfo : leaf.reader().getFieldInfos()) {
                    if (fieldInfo.name.equals("labels")) {
                        assertEquals(
                            "root field 'labels' should not have doc values when index=false",
                            DocValuesType.NONE,
                            fieldInfo.getDocValuesType()
                        );
                    }
                    if (fieldInfo.name.equals("labels._keyed")) {
                        assertNotEquals(
                            "keyed field 'labels._keyed' should have doc values",
                            DocValuesType.NONE,
                            fieldInfo.getDocValuesType()
                        );
                    }
                }
            }
        }
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

    public void testMappedPropertyTermQuery() throws Exception {
        createIndex("props_test", indicesAdmin().prepareCreate("props_test").setMapping("""
            {
              "properties": {
                "event": {
                  "type": "flattened",
                  "properties": {
                    "host.name": { "type": "keyword" }
                  }
                }
              }
            }"""));

        prepareIndex("props_test").setId("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"host.name": "server-a", "region": "us-east"}}""", XContentType.JSON).get();

        prepareIndex("props_test").setId("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"host.name": "server-b", "region": "eu-west"}}""", XContentType.JSON).get();

        // Mapped property: term query on the typed keyword sub-field
        assertHitCount(client().prepareSearch("props_test").setQuery(termQuery("event.host.name", "server-a")), 1L);
        assertHitCount(client().prepareSearch("props_test").setQuery(termQuery("event.host.name", "server-b")), 1L);
        assertHitCount(client().prepareSearch("props_test").setQuery(termQuery("event.host.name", "nonexistent")), 0L);

        // Unmapped key: still queryable through the flattened keyed mechanism
        assertHitCount(client().prepareSearch("props_test").setQuery(termQuery("event.region", "us-east")), 1L);
        assertHitCount(client().prepareSearch("props_test").setQuery(termQuery("event.region", "eu-west")), 1L);
    }

    public void testMappedPropertySort() throws Exception {
        createIndex("sort_test", indicesAdmin().prepareCreate("sort_test").setMapping("""
            {
              "properties": {
                "event": {
                  "type": "flattened",
                  "properties": {
                    "priority": { "type": "keyword" }
                  }
                }
              }
            }"""));

        prepareIndex("sort_test").setId("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"priority": "B", "other": "x"}}""", XContentType.JSON).get();

        prepareIndex("sort_test").setId("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"priority": "A", "other": "y"}}""", XContentType.JSON).get();

        prepareIndex("sort_test").setId("3").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"priority": "C"}}""", XContentType.JSON).get();

        // Sort ascending on the mapped keyword property
        assertNoFailuresAndResponse(client().prepareSearch("sort_test").addSort("event.priority", SortOrder.ASC), response -> {
            assertHitCount(response, 3);
            assertOrderedSearchHits(response, "2", "1", "3");
        });

        // Sort descending
        assertNoFailuresAndResponse(client().prepareSearch("sort_test").addSort("event.priority", SortOrder.DESC), response -> {
            assertHitCount(response, 3);
            assertOrderedSearchHits(response, "3", "1", "2");
        });
    }

    public void testMappedPropertyTermsAggregation() throws Exception {
        createIndex("agg_test", indicesAdmin().prepareCreate("agg_test").setMapping("""
            {
              "properties": {
                "event": {
                  "type": "flattened",
                  "properties": {
                    "status": { "type": "keyword" }
                  }
                }
              }
            }"""));

        BulkRequestBuilder bulkRequest = client().prepareBulk("agg_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 5; i++) {
            bulkRequest.add(client().prepareIndex().setSource("""
                {"event": {"status": "ok", "region": "us-east"}}""", XContentType.JSON));
        }
        for (int i = 0; i < 3; i++) {
            bulkRequest.add(client().prepareIndex().setSource("""
                {"event": {"status": "error", "region": "eu-west"}}""", XContentType.JSON));
        }
        BulkResponse bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        // Terms aggregation on the mapped keyword property
        TermsAggregationBuilder statusAgg = createTermsAgg("event.status");
        assertNoFailuresAndResponse(client().prepareSearch("agg_test").addAggregation(statusAgg), response -> {
            Terms statusTerms = response.getAggregations().get("terms");
            assertThat(statusTerms, notNullValue());
            assertThat(statusTerms.getBuckets().size(), equalTo(2));

            Terms.Bucket okBucket = statusTerms.getBucketByKey("ok");
            assertEquals(5, okBucket.getDocCount());

            Terms.Bucket errorBucket = statusTerms.getBucketByKey("error");
            assertEquals(3, errorBucket.getDocCount());
        });

        // Terms aggregation on unmapped key still works through flattened
        TermsAggregationBuilder regionAgg = createTermsAgg("event.region");
        assertNoFailuresAndResponse(client().prepareSearch("agg_test").addAggregation(regionAgg), response -> {
            Terms regionTerms = response.getAggregations().get("terms");
            assertThat(regionTerms.getBuckets().size(), equalTo(2));
        });
    }

    public void testMappedPropertyLongRangeQuery() throws Exception {
        createIndex("range_test", indicesAdmin().prepareCreate("range_test").setMapping("""
            {
              "properties": {
                "metrics": {
                  "type": "flattened",
                  "properties": {
                    "response_time": { "type": "long" }
                  }
                }
              }
            }"""));

        BulkRequestBuilder bulkRequest = client().prepareBulk("range_test").setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        int[] responseTimes = { 50, 120, 200, 350, 500 };
        for (int i = 0; i < responseTimes.length; i++) {
            bulkRequest.add(client().prepareIndex().setId(Integer.toString(i)).setSource(Strings.format("""
                {"metrics": {"response_time": %d, "label": "req-%d"}}""", responseTimes[i], i), XContentType.JSON));
        }
        BulkResponse bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        // Numeric range query on the mapped long property
        assertHitCount(client().prepareSearch("range_test").setQuery(rangeQuery("metrics.response_time").gte(100).lte(300)), 2L);
        assertHitCount(client().prepareSearch("range_test").setQuery(rangeQuery("metrics.response_time").gt(200)), 2L);
        assertHitCount(client().prepareSearch("range_test").setQuery(rangeQuery("metrics.response_time").lt(100)), 1L);

        // Sort on the mapped long property
        assertNoFailuresAndResponse(client().prepareSearch("range_test").addSort("metrics.response_time", SortOrder.DESC), response -> {
            assertHitCount(response, 5);
            assertOrderedSearchHits(response, "4", "3", "2", "1", "0");
        });

        // Unmapped key still works as flattened
        assertHitCount(client().prepareSearch("range_test").setQuery(termQuery("metrics.label", "req-0")), 1L);
    }

    public void testMappedPropertyExistsQuery() throws Exception {
        createIndex("exists_test", indicesAdmin().prepareCreate("exists_test").setMapping("""
            {
              "properties": {
                "event": {
                  "type": "flattened",
                  "properties": {
                    "host.name": { "type": "keyword" }
                  }
                }
              }
            }"""));

        prepareIndex("exists_test").setId("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"host.name": "server-a", "region": "us-east"}}""", XContentType.JSON).get();

        prepareIndex("exists_test").setId("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"region": "eu-west"}}""", XContentType.JSON).get();

        assertHitCount(client().prepareSearch("exists_test").setQuery(existsQuery("event.host.name")), 1L);
        assertHitCount(client().prepareSearch("exists_test").setQuery(existsQuery("event")), 2L);
    }

    public void testCrossIndexQueryWithMappedProperty() throws Exception {
        createIndex("cross_idx_a", indicesAdmin().prepareCreate("cross_idx_a").setMapping("""
            {
              "properties": {
                "event": {
                  "type": "flattened",
                  "properties": {
                    "status": { "type": "keyword" }
                  }
                }
              }
            }"""));

        createIndex("cross_idx_b", indicesAdmin().prepareCreate("cross_idx_b").setMapping("""
            {
              "properties": {
                "event": { "type": "flattened" }
              }
            }"""));

        prepareIndex("cross_idx_a").setId("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"status": "ok"}}""", XContentType.JSON).get();

        prepareIndex("cross_idx_b").setId("2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"status": "ok"}}""", XContentType.JSON).get();

        assertHitCount(client().prepareSearch("cross_idx_a", "cross_idx_b").setQuery(termQuery("event.status", "ok")), 2L);
    }

    public void testMappedPropertyDocValueFields() throws Exception {
        createIndex("dv_test", indicesAdmin().prepareCreate("dv_test").setMapping("""
            {
              "properties": {
                "event": {
                  "type": "flattened",
                  "properties": {
                    "host.name": { "type": "keyword" }
                  }
                }
              }
            }"""));

        prepareIndex("dv_test").setId("1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource("""
            {"event": {"host.name": "server-a", "region": "us-east"}}""", XContentType.JSON).get();

        assertNoFailuresAndResponse(
            client().prepareSearch("dv_test").addDocValueField("event.host.name").addDocValueField("event.region"),
            response -> {
                assertHitCount(response, 1);
                Map<String, DocumentField> fields = response.getHits().getAt(0).getFields();

                DocumentField hostField = fields.get("event.host.name");
                assertEquals("event.host.name", hostField.getName());
                assertEquals("server-a", hostField.getValue());

                DocumentField regionField = fields.get("event.region");
                assertEquals("event.region", regionField.getName());
                assertEquals("us-east", regionField.getValue());
            }
        );
    }
}
