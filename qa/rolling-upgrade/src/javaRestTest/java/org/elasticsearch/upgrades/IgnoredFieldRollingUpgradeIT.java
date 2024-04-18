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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IgnoredFieldRollingUpgradeIT extends ParameterizedRollingUpgradeTestCase {
    private static List<Map<String, Object>> oldHits;
    private static final String INDEX_NAME = "exists-index";

    public IgnoredFieldRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @SuppressWarnings("unchecked")
    public void testIgnoredMetaFieldExistsQuery() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex(INDEX_NAME)), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "1", "foofoo")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "2", "barbar")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "3", "fooooo")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "4", "barbaz")), RestStatus.CREATED);
            final List<Map<String, Object>> allDocs = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "hits.hits",
                entityAsMap(matchAll(INDEX_NAME))
            );
            assertThat(allDocs.size(), Matchers.equalTo(4));
            allDocs.forEach(doc -> assertThat((List<String>) doc.get("_ignored"), Matchers.contains("keyword")));
            oldHits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", entityAsMap(existsQuery(INDEX_NAME)));
            assertThat(oldHits.size(), Matchers.equalTo(4));
        } else if (isUpgradedCluster()) {
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "5", "foobar")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "6", "bazfoo")), RestStatus.CREATED);
            final List<Map<String, Object>> allDocs = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "hits.hits",
                entityAsMap(matchAll(INDEX_NAME))
            );
            assertThat(allDocs.size(), Matchers.equalTo(6));
            allDocs.forEach(doc -> assertThat((List<String>) doc.get("_ignored"), Matchers.contains("keyword")));
            final List<Map<String, Object>> hits = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "hits.hits",
                entityAsMap(existsQuery(INDEX_NAME))
            );
            final List<String> oldIds = oldHits.stream().map(oldHit -> oldHit.get(IdFieldMapper.NAME).toString()).toList();
            final List<String> ids = hits.stream().map(hit -> hit.get(IdFieldMapper.NAME).toString()).toList();
            assertTrue(ids.containsAll(oldIds));
        }
    }

    @SuppressWarnings("unchecked")
    public void testIgnoredMetaFieldGetQuery() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex(INDEX_NAME)), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "1", "foofoo")), RestStatus.CREATED);
            final Map<String, Object> doc = entityAsMap(get(INDEX_NAME, "1"));
            assertThat(((List<?>) doc.get(IgnoredFieldMapper.NAME)).get(0), Matchers.equalTo("keyword"));
        } else if (isUpgradedCluster()) {
            assertRestStatus(client().performRequest(indexDocument(INDEX_NAME, "2", "foobar")), RestStatus.CREATED);
            final Map<String, Object> doc = entityAsMap(get(INDEX_NAME, "2"));
            // NOTE: from version 8.15 _ignored is not returned anymore by default when using the get api since
            // `get` only allows fetching stored fields via `stored_fields` and _ignored is not stored anymore.
            assertNull(doc.get(IgnoredFieldMapper.NAME));
        }
    }

    private static Response matchAll(final String index) throws IOException {
        return client().performRequest(new Request("POST", "/" + index + "/_search"));
    }

    private static Response get(final String index, final String docId) throws IOException {
        return client().performRequest(new Request("GET", "/" + index + "/_doc/" + docId + "?stored_fields=_ignored"));
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
