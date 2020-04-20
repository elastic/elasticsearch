/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.eql;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public abstract class CommonEqlRestTestCase extends ESRestTestCase {

    private static final String defaultValidationIndexName = "eql_search_validation_test";
    private static final String validQuery = "process where user = 'SYSTEM'";

    private static final String[][] testBadRequests = {
            {null, "request body or source parameter is required"},
            {"{}", "query is null or empty"},
            {"{\"query\": \"\"}", "query is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"timestamp_field\": \"\"}", "timestamp field is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"event_category_field\": \"\"}", "event category field is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"implicit_join_key_field\": \"\"}", "implicit join key field is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"size\": 0}", "size must be greater than 0"},
            {"{\"query\": \"" + validQuery + "\", \"size\": -1}", "size must be greater than 0"},
            {"{\"query\": \"" + validQuery + "\", \"search_after\": null}", "search_after doesn't support values of type: VALUE_NULL"},
            {"{\"query\": \"" + validQuery + "\", \"search_after\": []}", "must contains at least one value"},
            {"{\"query\": \"" + validQuery + "\", \"filter\": null}", "filter doesn't support values of type: VALUE_NULL"},
            {"{\"query\": \"" + validQuery + "\", \"filter\": {}}", "query malformed, empty clause found"}
    };

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    @Before
    public void setup() throws Exception {
        createIndex(defaultValidationIndexName, Settings.EMPTY);
    }

    @After
    public void cleanup() throws Exception {
        deleteIndex(defaultValidationIndexName);
    }

    public void testBadRequests() throws Exception {
        final String contentType = "application/json";
        for (String[] test : testBadRequests) {
            final String endpoint = "/" + defaultValidationIndexName + "/_eql/search";
            Request request = new Request("GET", endpoint);
            request.setJsonEntity(test[0]);

            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            Response response = e.getResponse();

            assertThat(response.getHeader("Content-Type"), containsString(contentType));
            assertThat(EntityUtils.toString(response.getEntity()), containsString(test[1]));
            assertThat(response.getStatusLine().getStatusCode(), is(400));
        }
    }
}
