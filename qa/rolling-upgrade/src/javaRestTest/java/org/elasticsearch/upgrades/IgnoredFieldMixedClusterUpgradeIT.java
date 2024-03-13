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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IgnoredFieldMixedClusterUpgradeIT extends ParameterizedRollingUpgradeTestCase {
    private static List<?> oldHits;
    private static final String INDEX_NAME = "exists-index";

    public IgnoredFieldMixedClusterUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
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
            oldHits = (List<?>) XContentMapValues.extractValue("hits.hits", entityAsMap(existsQuery(INDEX_NAME)));
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
            final List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", entityAsMap(existsQuery(INDEX_NAME)));
            assertThat(hits.size(), Matchers.equalTo(6));
            assertThat(hits.containsAll(oldHits), Matchers.equalTo(true));
        }
    }

    private static Response matchAll(final String index) throws IOException {
        return client().performRequest(new Request("POST", "/" + index + "/_search"));
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
        final Request indexRequest = new Request("POST", "/" + index + "/_doc/");
        final XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("id", id)
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
