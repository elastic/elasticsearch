/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClearRolesCacheResponseTests extends ESTestCase {

    public void testParseFromXContent() throws IOException {
        final ElasticsearchException exception = new ElasticsearchException("test");
        final String nodesHeader = "\"_nodes\": { \"total\": 2, \"successful\": 1, \"failed\": 1, \"failures\": [ "
            + Strings.toString(exception) + "] },";
        final String clusterName = "\"cluster_name\": \"cn\",";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{" + nodesHeader + clusterName +  "\"nodes\" : {} }")) {

            ClearRolesCacheResponse response = ClearRolesCacheResponse.fromXContent(parser);
            assertNotNull(response);
            assertThat(response.getNodes(), empty());
            assertThat(response.getClusterName(), equalTo("cn"));
            assertThat(response.getHeader().getSuccessful(), equalTo(1));
            assertThat(response.getHeader().getFailed(), equalTo(1));
            assertThat(response.getHeader().getTotal(), equalTo(2));
            assertThat(response.getHeader().getFailures(), hasSize(1));
            assertThat(response.getHeader().getFailures().get(0).getMessage(), containsString("reason=test"));
        }

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{" + nodesHeader + clusterName + "\"nodes\" : { \"id1\": { \"name\": \"a\"}, \"id2\": { \"name\": \"b\"}}}"
            )
        ) {

            ClearRolesCacheResponse response = ClearRolesCacheResponse.fromXContent(parser);
            assertNotNull(response);
            assertThat(response.getNodes(), hasSize(2));
            assertThat(response.getNodes().get(0).getId(), equalTo("id1"));
            assertThat(response.getNodes().get(0).getName(), equalTo("a"));
            assertThat(response.getNodes().get(1).getId(), equalTo("id2"));
            assertThat(response.getNodes().get(1).getName(), equalTo("b"));
        }
    }
}
