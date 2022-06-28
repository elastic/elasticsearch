/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

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
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

public class AutoCreateDataStreamIT extends ESRestTestCase {

    /**
     * Check that setting {@link AutoCreateIndex#AUTO_CREATE_INDEX_SETTING} to <code>false</code>
     * does not affect the ability to auto-create data streams, which are not subject to the setting.
     */
    public void testCanAutoCreateDataStreamWhenAutoCreateIndexDisabled() throws IOException {
        configureAutoCreateIndex(false);
        createTemplateWithAllowAutoCreate(null);
        assertOK(this.indexDocument());
    }

    /**
     * Check that automatically creating a data stream is allowed when the index name matches a template
     * and that template has <code>allow_auto_create</code> set to <code>true</code>.
     */
    public void testCanAutoCreateDataStreamWhenExplicitlyAllowedByTemplate() throws IOException {
        createTemplateWithAllowAutoCreate(true);

        // Attempt to add a document to a non-existing index. Auto-creating the index should succeed because the index name
        // matches the template pattern
        assertOK(this.indexDocument());
    }

    /**
     * Check that automatically creating a data stream is disallowed when the data stream name matches a template and that template has
     * <code>allow_auto_create</code> explicitly to <code>false</code>.
     */
    public void testCannotAutoCreateDataStreamWhenDisallowedByTemplate() throws IOException {
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
        XContentBuilder b = JsonXContent.contentBuilder();
        b.startObject();
        {
            b.array("index_patterns", "recipe*");
            if (allowAutoCreate != null) {
                b.field("allow_auto_create", allowAutoCreate);
            }
            b.startObject("data_stream");
            b.endObject();
        }
        b.endObject();

        final Request createTemplateRequest = new Request("PUT", "_index_template/recipe_template");
        createTemplateRequest.setJsonEntity(Strings.toString(b));
        final Response createTemplateResponse = client().performRequest(createTemplateRequest);
        assertOK(createTemplateResponse);
    }

    private Response indexDocument() throws IOException {
        final Request indexDocumentRequest = new Request("POST", "recipe_kr/_doc");
        indexDocumentRequest.setJsonEntity("{ \"@timestamp\": \"" + Instant.now() + "\", \"name\": \"Kimchi\" }");
        return client().performRequest(indexDocumentRequest);
    }
}
