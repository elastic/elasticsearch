/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClearRealmCacheResponseTests extends ESTestCase {

    public void testParseFromXContent() throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                """
                    {
                      "_nodes": { "total": 2, "successful": 1, "failed": 1, "failures": [ {"type":"exception","reason":"test"}] },
                      "cluster_name": "cn",
                      "nodes" : {}
                    }"""
            )
        ) {

            ClearRealmCacheResponse response = ClearRealmCacheResponse.fromXContent(parser);
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
                """
                    {
                      "_nodes": { "total": 2, "successful": 1, "failed": 1, "failures": [ {"type":"exception","reason":"test"}] },
                      "cluster_name": "cn",
                      "nodes" : { "id1": { "name": "a"}, "id2": { "name": "b"}}
                    }"""
            )
        ) {

            ClearRealmCacheResponse response = ClearRealmCacheResponse.fromXContent(parser);
            assertNotNull(response);
            assertThat(response.getNodes(), hasSize(2));
            assertThat(response.getNodes().get(0).getId(), equalTo("id1"));
            assertThat(response.getNodes().get(0).getName(), equalTo("a"));
            assertThat(response.getNodes().get(1).getId(), equalTo("id2"));
            assertThat(response.getNodes().get(1).getName(), equalTo("b"));
        }
    }

}
