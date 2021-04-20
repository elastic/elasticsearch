/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.matchesRegex;

public class CatIndicesWithSecurityIT extends ESRestTestCase {
    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("cat_test_user", new SecureString("cat-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testHiddenIndexWithVisibleAlias() throws IOException {
        // Create the index and alias
        {
            final Request createRequest = new Request("PUT", ".index_hidden");
            createRequest.setJsonEntity(
                "{\"settings\": {\"index.hidden\": true, \"number_of_replicas\":  0}, \"aliases\": {\"index_allowed\": {}}}");
            final Response createResponse = adminClient().performRequest(createRequest);
            assertOK(createResponse);
            ensureGreen("index_allowed");
        }

        // And it should be visible by a user that has permissions for only the alias
        {
            final Request catRequest = new Request("GET", "_cat/indices?format=txt");
            final Response catResponse = client().performRequest(catRequest);
            assertOK(catResponse);

            final String resp = EntityUtils.toString(catResponse.getEntity());
            assertThat(resp, matchesRegex("(?s)^?green\\s+open\\s+\\.index_hidden.*$"));
        }
    }

    public void testHiddenIndexWithHiddenAlias() throws IOException {
        // Create the index and alias
        {
            final Request createRequest = new Request("PUT", ".index_hidden");
            createRequest.setJsonEntity("{\"settings\": {\"index.hidden\": true, \"number_of_replicas\":  0}, "
                + "\"aliases\": {\"index_allowed\": {\"is_hidden\":  true}}}");
            final Response createResponse = adminClient().performRequest(createRequest);
            assertOK(createResponse);
            ensureGreen("index_allowed");
        }

        // It should *not* be visible
        {
            final Request catRequest = new Request("GET", "_cat/indices?format=txt");
            final Response catResponse = client().performRequest(catRequest);
            assertOK(catResponse);

            final String resp = EntityUtils.toString(catResponse.getEntity());
            assertThat(resp, matchesRegex("(?s)\\W*")); // Should have no text
        }

        // But we should be able to see the index if we ask for hidden things
        {
            final Request catRequest = new Request("GET", "_cat/indices?format=txt&expand_wildcards=all,hidden");
            final Response catResponse = client().performRequest(catRequest);
            assertOK(catResponse);

            final String resp = EntityUtils.toString(catResponse.getEntity());
            assertThat(resp, matchesRegex("(?s)^?green\\s+open\\s+\\.index_hidden.*$"));
        }
    }

    public void testVisibleIndexWithHiddenAlias() throws IOException {
        // Create the index and alias
        {
            final Request createRequest = new Request("PUT", "visible_index");
            createRequest.setJsonEntity("{\"settings\": {\"number_of_replicas\":  0}, "
                + "\"aliases\": {\"index_allowed\": {\"is_hidden\":  true}}}");
            final Response createResponse = adminClient().performRequest(createRequest);
            assertOK(createResponse);
            ensureGreen("index_allowed");
        }

        // It should *not* be visible, because this user only has access through the *hidden* alias
        {
            final Request catRequest = new Request("GET", "_cat/indices?format=txt");
            final Response catResponse = client().performRequest(catRequest);
            assertOK(catResponse);

            final String resp = EntityUtils.toString(catResponse.getEntity());
            assertThat(resp, matchesRegex("(?s)\\W*")); // Should have no text
        }

        // But we should be able to see the index if we ask for hidden things
        {
            final Request catRequest = new Request("GET", "_cat/indices?format=txt&expand_wildcards=all,hidden");
            final Response catResponse = client().performRequest(catRequest);
            assertOK(catResponse);

            final String resp = EntityUtils.toString(catResponse.getEntity());
            assertThat(resp, matchesRegex("(?s)^?green\\s+open\\s+visible_index.*$"));
        }
    }
}
