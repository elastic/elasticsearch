/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

public class AutoCreateIndexIT extends ESRestTestCase {

    /**
     * Check that setting {@link AutoCreateIndex#AUTO_CREATE_INDEX_SETTING} to <code>false</code>
     * disable the automatic creation on indices.
     */
    public void testCannotAutoCreateIndexWhenDisabled() throws IOException {
        configureAutoCreateIndex(false);

        // Attempt to add a document to a non-existing index. Auto-creating the index should fail owing to the setting above.
        final Request indexDocumentRequest = new Request("POST", "recipe_kr/_doc/123456");
        indexDocumentRequest.setJsonEntity("{ \"name\": \"Kimchi\" }");
        final ResponseException responseException = expectThrows(ResponseException.class, this::indexDocument);

        assertThat(
            Streams.copyToString(new InputStreamReader(responseException.getResponse().getEntity().getContent(), UTF_8)),
            containsString("no such index [recipe_kr] and [action.auto_create_index] is [false]")
        );
    }

    /**
     * Check that automatically creating an index is allowed, even when {@link AutoCreateIndex#AUTO_CREATE_INDEX_SETTING}
     * is <code>false</code>, when the index name matches a template and that template has <code>allow_auto_create</code>
     * set to <code>true</code>.
     */
    public void testCanAutoCreateIndexWhenAllowedByTemplate() throws IOException {
        configureAutoCreateIndex(false);

        createTemplateWithAllowAutoCreate(true);

        // Attempt to add a document to a non-existing index. Auto-creating the index should succeed because the index name
        // matches the template pattern
        assertOK(this.indexDocument());
    }

    /**
     * Check that automatically creating an index is disallowed when the index name matches a template and that template has
     * <code>allow_auto_create</code> explicitly to <code>false</code>, even when {@link AutoCreateIndex#AUTO_CREATE_INDEX_SETTING}
     * is set to <code>true</code>.
     */
    public void testCannotAutoCreateIndexWhenDisallowedByTemplate() throws IOException {
        configureAutoCreateIndex(true);

        createTemplateWithAllowAutoCreate(false);

        // Attempt to add a document to a non-existing index. Auto-creating the index should succeed because the index name
        // matches the template pattern
        final ResponseException responseException = expectThrows(ResponseException.class, this::indexDocument);

        assertThat(
            Streams.copyToString(new InputStreamReader(responseException.getResponse().getEntity().getContent(), UTF_8)),
            containsString("no such index [composable template [recipe*] forbids index auto creation]")
        );
    }

    private void configureAutoCreateIndex(boolean value) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("persistent")
            .field(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), value)
            .endObject()
            .endObject();

        final Request settingsRequest = new Request("PUT", "_cluster/settings");
        settingsRequest.setJsonEntity(Strings.toString(builder));
        final Response settingsResponse = client().performRequest(settingsRequest);
        assertOK(settingsResponse);
    }

    private void createTemplateWithAllowAutoCreate(Boolean allowAutoCreate) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .array("index_patterns", "recipe*")
            .field("allow_auto_create", allowAutoCreate)
            .endObject();

        final Request createTemplateRequest = new Request("PUT", "_index_template/recipe_template");
        createTemplateRequest.setJsonEntity(Strings.toString(builder));
        final Response createTemplateResponse = client().performRequest(createTemplateRequest);
        assertOK(createTemplateResponse);
    }

    private Response indexDocument() throws IOException {
        final Request indexDocumentRequest = new Request("POST", "recipe_kr/_doc/123456");
        indexDocumentRequest.setJsonEntity("{ \"name\": \"Kimchi\" }");
        return client().performRequest(indexDocumentRequest);
    }
}
