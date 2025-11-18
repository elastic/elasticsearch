/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IgnoredMetaFieldRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String TERMS_AGG_QUERY = Strings.format("""
        {
          "aggs": {
            "ignored_terms": {
              "terms": {
                "field": "_ignored"
              }
            }
          }
        }""");

    public IgnoredMetaFieldRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testAggregation() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex("index-old-agg")), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument("index-old-agg", "foofoo", "1024.12.321.777", "1")), RestStatus.CREATED);
            if (getOldClusterIndexVersion().before(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD)) {
                assertTermsAggIgnoredMetadataFieldException(
                    "index-old-agg",
                    "Fielddata is not supported on field [_ignored] of type [_ignored]"
                );
            } else {
                assertTermsAggIgnoredMetadataField("index-old-agg");
            }
        } else if (isUpgradedCluster()) {
            assertRestStatus(client().performRequest(waitForClusterStatus("green", "90s")), RestStatus.OK);
            assertRestStatus(client().performRequest(createNewIndex("index-new-agg")), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument("index-new-agg", "barbar", "555.222.111.000", "2")), RestStatus.CREATED);

            assertTermsAggIgnoredMetadataField("index-*");
            if (getOldClusterIndexVersion().before(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD)) {
                assertTermsAggIgnoredMetadataFieldException(
                    "index-old-agg",
                    "Fielddata is not supported on field [_ignored] of type [_ignored]"
                );
            } else {
                assertTermsAggIgnoredMetadataField("index-old-agg");
            }
            assertTermsAggIgnoredMetadataField("index-new-agg");
        }
    }

    public void testIgnoredMetaFieldGetWithIgnoredQuery() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex("old-get-ignored-index")), RestStatus.OK);
            assertRestStatus(
                client().performRequest(indexDocument("old-get-ignored-index", "foofoo", "192.168.10.1234", "1")),
                RestStatus.CREATED
            );
            final Map<String, Object> doc = entityAsMap(getWithIgnored("old-get-ignored-index", "1"));
            assertThat(((List<?>) doc.get(IgnoredFieldMapper.NAME)), Matchers.containsInAnyOrder("ip_address", "keyword"));
        } else if (isUpgradedCluster()) {
            assertRestStatus(client().performRequest(waitForClusterStatus("green", "90s")), RestStatus.OK);
            assertRestStatus(
                client().performRequest(indexDocument("old-get-ignored-index", "barbar", "192.168.256.256", "2")),
                RestStatus.CREATED
            );
            final Map<String, Object> doc = entityAsMap(getWithIgnored("old-get-ignored-index", "2"));
            // NOTE: here we are reading documents from an index created by an older version of Elasticsearch where the _ignored
            // field could be stored depending on the version of Elasticsearch which created the index. The mapper for the _ignored field
            // will keep the stored field if necessary to avoid mixing documents where the _ignored field is stored and documents where it
            // is not, in the same index.
            assertThat(((List<?>) doc.get(IgnoredFieldMapper.NAME)), Matchers.containsInAnyOrder("ip_address", "keyword"));

            // NOTE: The stored field is dropped only once a new index is created by a new version of Elasticsearch.
            final String newVersionIndexName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            assertRestStatus(client().performRequest(createNewIndex(newVersionIndexName)), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument(newVersionIndexName, "foobar", "192.168.777", "3")), RestStatus.CREATED);
            final Map<String, Object> docFromNewIndex = entityAsMap(getWithIgnored(newVersionIndexName, "3"));
            assertThat(((List<?>) docFromNewIndex.get(IgnoredFieldMapper.NAME)), Matchers.containsInAnyOrder("ip_address", "keyword"));
        }
    }

    public void testIgnoredMetaFieldGetWithoutIgnoredQuery() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex("old-get-index")), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument("old-get-index", "foofoo", "192.168.169.300", "1")), RestStatus.CREATED);
            final Map<String, Object> doc = entityAsMap(get("old-get-index", "1"));
            if (getOldClusterIndexVersion().onOrAfter(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD)) {
                assertNull(doc.get(IgnoredFieldMapper.NAME));
            }
        } else if (isUpgradedCluster()) {
            assertRestStatus(client().performRequest(waitForClusterStatus("green", "90s")), RestStatus.OK);
            final Map<String, Object> doc1 = entityAsMap(get("old-get-index", "1"));
            assertNull(doc1.get(IgnoredFieldMapper.NAME));
            assertRestStatus(client().performRequest(indexDocument("old-get-index", "barbar", "192.168.0.1234", "2")), RestStatus.CREATED);
            final Map<String, Object> doc2 = entityAsMap(get("old-get-index", "2"));
            assertNull(doc2.get(IgnoredFieldMapper.NAME));

            final String newVersionIndexName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            assertRestStatus(client().performRequest(createNewIndex(newVersionIndexName)), RestStatus.OK);
            // NOTE: new Elasticsearch version does not used stored field for _ignored due to writing an index created by the new version
            assertRestStatus(
                client().performRequest(indexDocument(newVersionIndexName, "foobar", "263.192.168.12", "3")),
                RestStatus.CREATED
            );
            final Map<String, Object> docFromNewIndex = entityAsMap(get(newVersionIndexName, "3"));
            assertNull(docFromNewIndex.get(IgnoredFieldMapper.NAME));
        }
    }

    private static Response getWithIgnored(final String index, final String docId) throws IOException {
        return client().performRequest(new Request("GET", "/" + index + "/_doc/" + docId + "?stored_fields=_ignored"));
    }

    private static Response get(final String index, final String docId) throws IOException {
        return client().performRequest(new Request("GET", "/" + index + "/_doc/" + docId));
    }

    private static Request waitForClusterStatus(final String statusColor, final String timeoutSeconds) {
        final Request waitForGreen = new Request("GET", "/_cluster/health");
        waitForGreen.addParameter("wait_for_status", statusColor);
        waitForGreen.addParameter("timeout", timeoutSeconds);
        waitForGreen.addParameter("level", "shards");
        return waitForGreen;
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

    private static Request indexDocument(final String indexName, final String keywordValue, final String ipAddressValue, final String docId)
        throws IOException {
        final Request indexRequest = new Request("POST", "/" + indexName + "/_doc/" + docId);
        final XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("keyword", keywordValue)
            .field("ip_address", ipAddressValue)
            .endObject();
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity(Strings.toString(doc));
        return indexRequest;
    }

    @SuppressWarnings("unchecked")
    private static void assertTermsAggIgnoredMetadataField(final String indexPattern) throws IOException {
        final Request aggRequest = new Request("POST", "/" + indexPattern + "/_search");
        aggRequest.addParameter("size", "0");
        aggRequest.setJsonEntity(TERMS_AGG_QUERY);
        final Response aggResponse = client().performRequest(aggRequest);
        final Map<String, Object> aggResponseEntityAsMap = entityAsMap(aggResponse);
        final Map<String, Object> aggregations = (Map<String, Object>) aggResponseEntityAsMap.get("aggregations");
        final Map<String, Object> ignoredTerms = (Map<String, Object>) aggregations.get("ignored_terms");
        final List<Map<String, Object>> buckets = (List<Map<String, Object>>) ignoredTerms.get("buckets");
        assertThat(buckets.stream().map(bucket -> bucket.get("key")).toList(), Matchers.containsInAnyOrder("ip_address", "keyword"));
    }

    private static void assertTermsAggIgnoredMetadataFieldException(final String indexPattern, final String exceptionMessage) {
        final Request aggRequest = new Request("POST", "/" + indexPattern + "/_search");
        aggRequest.addParameter("size", "0");
        aggRequest.setJsonEntity(TERMS_AGG_QUERY);
        final Exception responseException = assertThrows(ResponseException.class, () -> client().performRequest(aggRequest));
        assertThat(responseException.getMessage(), Matchers.containsString(exceptionMessage));
    }

}
