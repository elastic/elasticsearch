/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Map;

public abstract class SearchContextCleanupTestCase extends JdbcIntegrationTestCase {

    public void testSearchContextCleanupEventually() throws Exception {
        index("pit-test", builder -> builder.field("foo", "bar"));

        String pitId = null;
        Response openResponse = null;
        try {
            Request open = new Request("POST", "/_pit");
            open.addParameter("index", "pit-test");
            open.addParameter("keep_alive", "1m");
            openResponse = client().performRequest(open);
            assertOK(openResponse);

            Map<String, Object> openMap = responseAsMap(openResponse);
            pitId = (String) openMap.get("id");
            assertNotNull(pitId);
        } finally {
            if (pitId != null) {
                Request close = new Request("DELETE", "/_pit");
                try (XContentBuilder body = JsonXContent.contentBuilder()) {
                    body.startObject();
                    body.field("id", pitId);
                    body.endObject();
                    close.setJsonEntity(Strings.toString(body));
                }
                assertOK(client().performRequest(close));
            }
        }

        assertNoSearchContexts();
    }
}
