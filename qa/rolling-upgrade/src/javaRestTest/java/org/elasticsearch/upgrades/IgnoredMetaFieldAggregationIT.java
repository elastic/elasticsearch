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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IgnoredMetaFieldAggregationIT extends ParameterizedRollingUpgradeTestCase {
    public IgnoredMetaFieldAggregationIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testAggregation() throws IOException {
        if (isOldCluster()) {
            // NOTE: index 'test1' is created on a version that is missing doc values for the _ignored field which results in an aggregation
            // failure
            final Response createIndexResponse = client().performRequest(createNewIndex("test1", 3, true));
            assertRestStatus(createIndexResponse, RestStatus.OK);
            final Response indexDocumentResponse = client().performRequest(indexDocument("test1", "foofoo", "1024.12.321.777"));
            assertRestStatus(indexDocumentResponse, RestStatus.CREATED);
            assertIndexDocuments("test1", 1);
            assertAggregateIgnoredMetadataFieldException("test1", "Fielddata is not supported on field [_ignored] of type [_ignored]");
        } else if (isUpgradedCluster()) {
            // NOTE: index 'test2' is created on a version that has doc values for the _ignored field
            final Request waitForGreen = new Request("GET", "/_cluster/health/test1");
            waitForGreen.addParameter("wait_for_nodes", "3");
            waitForGreen.addParameter("wait_for_status", "green");
            waitForGreen.addParameter("timeout", "30s");
            waitForGreen.addParameter("level", "shards");
            final Response response = client().performRequest(waitForGreen);
            assertRestStatus(response, RestStatus.OK);

            final Response createIndexResponse = client().performRequest(createNewIndex("test2", 3, true));
            assertRestStatus(createIndexResponse, RestStatus.OK);
            final Response indexDocumentResponse = client().performRequest(indexDocument("test2", "barbar", "555.222.111.000"));
            assertRestStatus(indexDocumentResponse, RestStatus.CREATED);
            assertIndexDocuments("test*", 1);

            assertAggregateIgnoredMetadataField("test*");
            assertAggregateIgnoredMetadataFieldException(
                "test1",
                "unexpected docvalues type NONE for field '_ignored' (expected one of [SORTED, SORTED_SET]). Re-index with correct docvalues type."
            );
            assertAggregateIgnoredMetadataField("test2");
        }
    }

    private static void assertRestStatus(final Response indexDocumentResponse, final RestStatus restStatus) {
        assertThat(indexDocumentResponse.getStatusLine().getStatusCode(), Matchers.equalTo(restStatus.getStatus()));
    }

    private static Request createNewIndex(final String indexName, int ignoreABove, boolean ignoreMalformed) throws IOException {
        final Request createIndex = new Request("PUT", "/" + indexName);
        final XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("keyword")
            .field("type", "keyword")
            .field("ignore_above", ignoreABove)
            .endObject()
            .startObject("ip_address")
            .field("type", "ip")
            .field("ignore_malformed", ignoreMalformed)
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
    private static void assertIndexDocuments(final String indexPattern, int expectedDocs) throws IOException {
        final Response response = client().performRequest(new Request("POST", "/" + indexPattern + "/_search"));
        final Map<String, Object> responseEntityAsMap = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) responseEntityAsMap.get("hits");
        final List<Object> hitsList = (List<Object>) hits.get("hits");
        assertThat(hitsList.size(), Matchers.equalTo(expectedDocs));
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
