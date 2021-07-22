/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.ParsedMediaType;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.TypeQueryV7Builder;
import org.elasticsearch.rest.action.document.RestIndexAction;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RestApiCompatibleUpgradeIT extends AbstractFullClusterRestartTestCase {
    final ParsedMediaType parsedMediaType = XContentType.VND_JSON.toParsedMediaType();
    final String v7MediaType = parsedMediaType
        .responseContentTypeHeader(Map.of("compatible-with", String.valueOf(RestApiVersion.V_7.major)));
    final ContentType v7ContentType = ContentType.create(parsedMediaType.mediaTypeWithoutParameters(),
        new BasicNameValuePair("compatible-with", String.valueOf(RestApiVersion.V_7.major)));


    public void testTypeQuery() throws IOException {
        if (isRunningAgainstOldCluster()) {
            String doc = "{\"foo\": \"bar\"}";
            createIndexAndPostDoc("/test_type_query_1/cat/1", doc, true);
            createIndexAndPostDoc("/test_type_query_2/_doc/1", doc, false);
        } else {
            assertTypeQueryHits("test_type_query_1", "cat", 1);
            assertTypeQueryHits("test_type_query_1", "dog", 0);
            assertTypeQueryHits("test_type_query_1", "_doc", 0);
            assertTypeQueryHits("test_type_query_1", "_default_", 0);

            assertTypeQueryHits("test_type_query_2", "_doc", 1);
            assertTypeQueryHits("test_type_query_2", "dog", 0);
            assertTypeQueryHits("test_type_query_2", "_default_", 0);

        }
    }

    private void createIndexAndPostDoc(String endpoint, String doc, boolean expectWarning) throws IOException {
        Request createDoc = new Request("PUT", endpoint);
        createDoc.addParameter("refresh", "true");
        RequestOptions options = expectWarning ? expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE) : RequestOptions.DEFAULT;
        createDoc.setOptions(options.toBuilder()
            .addHeader("Accept", v7MediaType));
        createDoc.setEntity(new NStringEntity(doc, v7ContentType));
        client().performRequest(createDoc);
    }

    private void assertTypeQueryHits(String index, String type, int numHits) throws IOException {
        Request searchRequest = new Request("GET", "/" + index + "/_search");

        searchRequest.setOptions(expectWarnings(TypeQueryV7Builder.TYPES_DEPRECATION_MESSAGE).toBuilder()
            .addHeader("Accept", v7MediaType));
        searchRequest.setEntity(new NStringEntity("{ \"query\": { \"type\" : {\"value\": \"" + type + "\"} }}", v7ContentType));

        Map<String, Object> response = entityAsMap(client().performRequest(searchRequest));
        assertNoFailures(response);
        assertThat(extractTotalHits(response), equalTo(numHits));
    }

}
