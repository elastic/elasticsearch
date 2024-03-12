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
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IgnoreMalformedMixedClusterUpgradeIT extends ParameterizedRollingUpgradeTestCase {
    private static Map<?, ?> oldHits;

    public IgnoreMalformedMixedClusterUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testTermQueryIgnoredMetaField() throws IOException {
        if (isOldCluster()) {
            assertRestStatus(client().performRequest(createNewIndex("test")), RestStatus.OK);
            assertRestStatus(client().performRequest(indexDocument("1", "foofoo")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument("2", "barbar")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument("3", "foo")), RestStatus.CREATED);
            assertRestStatus(client().performRequest(indexDocument("4", "bar")), RestStatus.CREATED);
            oldHits = (Map<?, ?>) ((List<?>) (XContentMapValues.extractValue("hits.hits", entityAsMap(termQuery())))).get(0);
        } else if (isUpgradedCluster()) {
            final Map<?, ?> hits = (Map<?, ?>) ((List<?>) (XContentMapValues.extractValue("hits.hits", entityAsMap(termQuery())))).get(0);
            assertThat(oldHits, MapMatcher.matchesMap(hits));
        }
    }

    private static Response termQuery() throws IOException {
        final Request request = new Request("POST", "/test/_search");
        request.addParameter("size", "3");
        final String format = Strings.format("""
            {
              "query": {
                "ids": {
                  "values": [ "2", "3" ]
                }
              }
            }""");
        request.setJsonEntity(format);
        return client().performRequest(request);
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
            .endObject()
            .endObject()
            .endObject();
        createIndex.setJsonEntity(Strings.toString(mappings));
        return createIndex;
    }

    private static Request indexDocument(final String id, final String keywordValue) throws IOException {
        final Request indexRequest = new Request("POST", "/test/_doc/");
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
