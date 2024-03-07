/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IgnoredMetaFieldAggregationIT extends ParameterizedRollingUpgradeTestCase {

    private static String aggFirstIndex;
    private static boolean aggFirstCreated;
    private static String aggSecondIndex;
    private static String runtimeFieldFirstIndex;
    private static boolean runtimeFieldFirstCreated;
    private static String runtimeFieldSecondIndex;

    public IgnoredMetaFieldAggregationIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Before
    public void setup() {
        aggFirstCreated = false;
        runtimeFieldFirstCreated = false;
        aggFirstIndex = "aaa_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        aggSecondIndex = "bbb_" + randomAlphaOfLength(9).toLowerCase(Locale.ROOT);
        runtimeFieldFirstIndex = "ccc_" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        runtimeFieldSecondIndex = "ddd_" + randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
    }

    public void testAggregation() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex(aggFirstIndex)), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument(aggFirstIndex, "foofoo", "1024.12.321.777")), RestStatus.CREATED);
            assertAggregateIgnoredMetadataFieldException(
                aggFirstIndex,
                "Fielddata is not supported on field [_ignored] of type [_ignored]"
            );
            aggFirstCreated = true;
        } else if (isUpgradedCluster()) {
            final Request waitForGreen = new Request("GET", "/_cluster/health");
            waitForGreen.addParameter("wait_for_nodes", "3");
            waitForGreen.addParameter("wait_for_status", "green");
            waitForGreen.addParameter("timeout", "90s");
            waitForGreen.addParameter("level", "shards");
            final Response response = client().performRequest(waitForGreen);
            assertRestStatus(response, RestStatus.OK);

            assertRestStatus(client().performRequest(createNewIndex(aggSecondIndex)), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument(aggSecondIndex, "barbar", "555.222.111.000")), RestStatus.CREATED);

            final String indexPattern = aggFirstCreated ? (aggFirstIndex + "," + aggSecondIndex) : aggSecondIndex;
            assertAggregateIgnoredMetadataField(indexPattern);
            if (aggFirstCreated) {
                assertAggregateIgnoredMetadataFieldException(
                    aggFirstIndex,
                    "unexpected docvalues type NONE for field '_ignored' (expected one of [SORTED, SORTED_SET])"
                );
            }
            assertAggregateIgnoredMetadataField(aggSecondIndex);
        }
    }

    public void testExistsUsingRuntimeField() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex(runtimeFieldFirstIndex)), RestStatus.OK);
            assertRestStatus(
                client().performRequest(indexDocument(runtimeFieldFirstIndex, "foofoo", "1024.12.321.777")),
                RestStatus.CREATED
            );
            runtimeFieldFirstCreated = true;
        } else if (isUpgradedCluster()) {
            final Request waitForGreen = new Request("GET", "/_cluster/health");
            waitForGreen.addParameter("wait_for_nodes", "3");
            waitForGreen.addParameter("wait_for_status", "green");
            waitForGreen.addParameter("timeout", "90s");
            waitForGreen.addParameter("level", "shards");
            final Response response = client().performRequest(waitForGreen);
            assertRestStatus(response, RestStatus.OK);

            assertRestStatus(client().performRequest(createNewIndex(runtimeFieldSecondIndex)), RestStatus.OK);
            assertRestStatus(
                client().performRequest(indexDocument(runtimeFieldSecondIndex, "barbar", "555.222.111.000")),
                RestStatus.CREATED
            );
            final String indexPattern = runtimeFieldFirstCreated
                ? (runtimeFieldFirstIndex + "," + runtimeFieldSecondIndex)
                : runtimeFieldSecondIndex;
            assertExistsUsingRuntimeField(indexPattern, runtimeFieldFirstIndex, runtimeFieldSecondIndex);
        }
    }

    private static void assertRestStatus(final Response indexDocumentResponse, final RestStatus restStatus) {
        assertThat(indexDocumentResponse.getStatusLine().getStatusCode(), Matchers.equalTo(restStatus.getStatus()));
    }

    private static Request createNewIndex(final String indexName) throws IOException {
        final Request createIndex = new Request("PUT", "/" + indexName);
        final XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("keyword")
            .field("type", "keyword")
            .field("ignore_above", 3)
            .endObject()
            .startObject("ip_address")
            .field("type", "ip")
            .field("ignore_malformed", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex.setJsonEntity(Strings.toString(mappings));
        return createIndex;
    }

    private static Request indexDocument(final String indexName, final String keyword, final String ipAddress) throws IOException {
        final Request indexRequest = new Request("POST", "/" + indexName + "/_doc/");
        final XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("keyword", keyword)
            .field("ip_address", ipAddress)
            .endObject();
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity(Strings.toString(doc));
        return indexRequest;
    }

    @SuppressWarnings("unchecked")
    private static void assertAggregateIgnoredMetadataField(final String indexPattern) throws IOException {
        final Request aggRequest = new Request("POST", "/" + indexPattern + "/_search");
        aggRequest.addParameter("size", "0");
        aggRequest.setJsonEntity(Strings.format("""
            {
              "aggs": {
                "ignored_terms": {
                  "terms": {
                    "field": "_ignored"
                  }
                }
              }
            }"""));
        final Response aggResponse = client().performRequest(aggRequest);
        final Map<String, Object> aggResponseEntityAsMap = entityAsMap(aggResponse);
        final Map<String, Object> aggregations = (Map<String, Object>) aggResponseEntityAsMap.get("aggregations");
        final Map<String, Object> ignoredTerms = (Map<String, Object>) aggregations.get("ignored_terms");
        final List<Map<String, Object>> buckets = (List<Map<String, Object>>) ignoredTerms.get("buckets");
        assertThat(buckets.stream().map(bucket -> bucket.get("key")).toList(), Matchers.containsInAnyOrder("ip_address", "keyword"));
        assertThat(buckets.stream().map(bucket -> bucket.get("doc_count")).toList(), Matchers.contains(1, 1));
    }

    @SuppressWarnings("unchecked")
    private static void assertExistsUsingRuntimeField(final String indexPattern, final String firstIndex, final String secondIndex)
        throws IOException {
        final Request request = new Request("POST", "/" + indexPattern + "/_search");
        request.addParameter("size", "2");
        request.setJsonEntity(Strings.format("""
            {
               "runtime_mappings": {
                 "has_ignored_fields": {
                   "type": "boolean",
                   "script": {
                     "source": "if (doc['_ignored'].size() > 0) { emit(true) }"
                   }
                 }
               },
               "query": {
                 "exists": {
                   "field": "has_ignored_fields"
                 }
               }
             }"""));
        final Response response = client().performRequest(request);
        final Map<String, Object> aggResponseEntityAsMap = entityAsMap(response);

        final Map<String, Object> shards = (Map<String, Object>) aggResponseEntityAsMap.get("_shards");
        final List<Object> failures = (List<Object>) shards.get("failures");
        assertThat(failures.size(), Matchers.equalTo(1));
        final Map<String, Object> failure = (Map<String, Object>) failures.get(0);
        assertThat((String) failure.get("index"), Matchers.equalTo(firstIndex));

        final Map<String, Object> hits = (Map<String, Object>) aggResponseEntityAsMap.get("hits");
        final List<Object> hitsList = (List<Object>) hits.get("hits");
        assertThat(hitsList.size(), Matchers.equalTo(1));
        final Map<String, Object> ignoredHit = (Map<String, Object>) hitsList.get(0);
        assertThat(secondIndex, Matchers.equalTo(ignoredHit.get("_index")));
        assertThat((List<String>) ignoredHit.get("_ignored"), Matchers.containsInAnyOrder("keyword", "ip_address"));
    }

    private static void assertAggregateIgnoredMetadataFieldException(final String indexPattern, final String exceptionMessage)
        throws IOException {
        final Request aggRequest = new Request("POST", "/" + indexPattern + "/_search");
        aggRequest.addParameter("size", "0");
        aggRequest.setJsonEntity(Strings.format("""
            {
              "aggs": {
                "ignored_terms": {
                  "terms": {
                    "field": "_ignored"
                  }
                }
              }
            }"""));
        final Exception responseException = assertThrows(ResponseException.class, () -> client().performRequest(aggRequest));
        assertThat(responseException.getMessage(), Matchers.containsString(exceptionMessage));
    }

}
