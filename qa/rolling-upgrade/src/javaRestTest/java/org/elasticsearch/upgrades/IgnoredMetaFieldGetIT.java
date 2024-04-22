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
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IgnoredMetaFieldGetIT extends ParameterizedRollingUpgradeTestCase {

    public IgnoredMetaFieldGetIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @SuppressWarnings("unchecked")
    public void testIgnoredMetaFieldGetWithIgnoredQuery() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex("old-get-ignored-index")), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument("old-get-ignored-index", "1", "foofoo")), RestStatus.CREATED);
            final Map<String, Object> doc = entityAsMap(getWithIgnored("old-get-ignored-index", "1"));
            assertThat(((List<?>) doc.get(IgnoredFieldMapper.NAME)).get(0), Matchers.equalTo("keyword"));
        } else if (isUpgradedCluster()) {
            assertRestStatus(client().performRequest(indexDocument("old-get-ignored-index", "2", "foobar")), RestStatus.CREATED);
            final Map<String, Object> doc = entityAsMap(getWithIgnored("old-get-ignored-index", "2"));
            // NOTE: from version 8.15 _ignored is not stored anymore and is not returned when requested via `stored_fields`.
            // Anyway, here we are reading documents from an index created by an older version of Elasticsearch where the _ignored
            // field was stored. The mapper will keep the stored field to avoid mixing documents where the _ignored field
            // is stored and documents where it is not, in the same index.
            assertThat(((List<?>) doc.get(IgnoredFieldMapper.NAME)).get(0), Matchers.equalTo("keyword"));

            // NOTE: The stored field is dropped only once a new index is created by a new version of Elasticsearch.
            final String newVersionIndexName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            assertRestStatus(client().performRequest(createNewIndex(newVersionIndexName)), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument(newVersionIndexName, "3", "barbar")), RestStatus.CREATED);
            final Map<String, Object> docFromNewIndex = entityAsMap(getWithIgnored(newVersionIndexName, "3"));
            assertNull(docFromNewIndex.get(IgnoredFieldMapper.NAME));
        }
    }

    @SuppressWarnings("unchecked")
    public void testIgnoredMetaFieldGetWithoutIgnoredQuery() throws IOException {
        if (isOldCluster()) {
            // NOTE: old Elasticsearch version uses stored field for _ignored
            assertRestStatus(client().performRequest(createNewIndex("old-get-index")), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument("old-get-index", "1", "foofoo")), RestStatus.CREATED);
            final Map<String, Object> doc = entityAsMap(get("old-get-index", "1"));
            assertThat(((List<?>) doc.get(IgnoredFieldMapper.NAME)).get(0), Matchers.equalTo("keyword"));
        } else if (isUpgradedCluster()) {
            final Map<String, Object> doc1 = entityAsMap(get("old-get-index", "1"));
            assertNull(doc1.get(IgnoredFieldMapper.NAME));
            // NOTE: new Elasticsearch version uses stored field for _ignored due to writing an index created by an older version
            assertRestStatus(client().performRequest(indexDocument("old-get-index", "2", "foobar")), RestStatus.CREATED);
            final Map<String, Object> doc2 = entityAsMap(get("old-get-index", "2"));
            assertNull(doc2.get(IgnoredFieldMapper.NAME));

            final String newVersionIndexName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            assertRestStatus(client().performRequest(createNewIndex(newVersionIndexName)), RestStatus.OK);
            // NOTE: new Elasticsearch version does not used stored field for _ignored due to writing an index created by the new version
            assertRestStatus(client().performRequest(indexDocument(newVersionIndexName, "3", "barbar")), RestStatus.CREATED);
            final Map<String, Object> docFromNewIndex = entityAsMap(get(newVersionIndexName, "3"));
            assertNull(docFromNewIndex.get(IgnoredFieldMapper.NAME));
        }
    }

    private static Response matchAll(final String index) throws IOException {
        return client().performRequest(new Request("POST", "/" + index + "/_search"));
    }

    private static Response getWithIgnored(final String index, final String docId) throws IOException {
        return client().performRequest(new Request("GET", "/" + index + "/_doc/" + docId + "?stored_fields=_ignored"));
    }

    private static Response get(final String index, final String docId) throws IOException {
        return client().performRequest(new Request("GET", "/" + index + "/_doc/" + docId));
    }

    private static Response existsQuery(final String index) throws IOException {
        final Request request = new Request("POST", "/" + index + "/_search");
        final String format = Strings.format("""
            {
              "query": {
                "exists": {
                  "field": "_ignored"
                }
              }
            }""");
        request.setJsonEntity(format);
        return client().performRequest(request);
    }

    private static Request createNewIndex(final String index) throws IOException {
        final Request createIndex = new Request("PUT", "/" + index);
        final XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("keyword")
            .field("type", "keyword")
            .field("ignore_above", 3)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex.setJsonEntity(Strings.toString(mappings));
        return createIndex;
    }

    private static Request indexDocument(final String index, final String id, final String keywordValue) throws IOException {
        final Request indexRequest = new Request("POST", "/" + index + "/_doc/" + id);
        final XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("keyword", keywordValue)
            .endObject();
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity(Strings.toString(doc));
        return indexRequest;
    }

    private static void assertRestStatus(final Response indexDocumentResponse, final RestStatus restStatus) {
        assertThat(indexDocumentResponse.getStatusLine().getStatusCode(), Matchers.equalTo(restStatus.getStatus()));
    }
}
