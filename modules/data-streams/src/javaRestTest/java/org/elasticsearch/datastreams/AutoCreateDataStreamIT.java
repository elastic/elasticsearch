/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

@SuppressWarnings("resource")
public class AutoCreateDataStreamIT extends DisabledSecurityDataStreamTestCase {

    /**
     * Check that setting {@link AutoCreateIndex#AUTO_CREATE_INDEX_SETTING} to <code>false</code>
     * does not affect the ability to auto-create data streams, which are not subject to the setting.
     */
    public void testCanAutoCreateDataStreamWhenAutoCreateIndexDisabled() throws IOException {
        configureAutoCreateIndex(false);
        createTemplate(null, true);
        assertOK(this.indexDocument());
    }

    /**
     * Check that automatically creating a data stream is allowed when the index name matches a template
     * and that template has <code>allow_auto_create</code> set to <code>true</code>.
     */
    public void testCanAutoCreateDataStreamWhenExplicitlyAllowedByTemplate() throws IOException {
        createTemplate(true, true);

        // Attempt to add a document to a non-existing index. Auto-creating the index should succeed because the index name
        // matches the template pattern
        assertOK(this.indexDocument());
    }

    /**
     * Check that automatically creating a data stream is disallowed when the data stream name matches a template and that template has
     * <code>allow_auto_create</code> explicitly to <code>false</code>.
     */
    public void testCannotAutoCreateDataStreamWhenDisallowedByTemplate() throws IOException {
        createTemplate(false, true);

        // Auto-creating the index should fail when the template disallows that
        final ResponseException responseException = expectThrows(ResponseException.class, this::indexDocument);

        assertThat(
            Streams.copyToString(new InputStreamReader(responseException.getResponse().getEntity().getContent(), UTF_8)),
            containsString("no such index [recipe_kr] and composable template [recipe*] forbids index auto creation")
        );
    }

    /**
     * Check that if <code>require_data_stream</code> is set to <code>true</code>, automatically creating an index is allowed only
     * if its name matches an index template AND it contains a data-stream template
     */
    public void testCannotAutoCreateDataStreamWhenNoDataStreamTemplateMatch() throws IOException {
        createTemplate(true, true);

        final Request request = prepareIndexRequest("ingredients_kr");
        request.addParameter(DocWriteRequest.REQUIRE_DATA_STREAM, Boolean.TRUE.toString());

        // Attempt to add a document to a non-existing index. Auto-creating the index should fail because the index name doesn't
        // match the template pattern and the request requires a data stream template
        final ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertThat(
            Streams.copyToString(new InputStreamReader(responseException.getResponse().getEntity().getContent(), UTF_8)),
            containsString(
                "no such index [ingredients_kr] and the index creation request requires a data stream, "
                    + "but no matching index template with data stream template was found for it"
            )
        );
    }

    /**
     * Check that if <code>require_data_stream</code> is set to <code>true</code>, automatically creating an index is allowed only
     * if its name matches an index template AND it contains a data-stream template
     */
    public void testCannotAutoCreateDataStreamWhenMatchingTemplateIsNotDataStream() throws IOException {
        createTemplate(true, false);

        final Request request = prepareIndexRequest("recipe_kr");
        request.addParameter(DocWriteRequest.REQUIRE_DATA_STREAM, Boolean.TRUE.toString());

        // Attempt to add a document to a non-existing index. Auto-creating the index should fail because the index name doesn't
        // match the template pattern and the request requires a data stream template
        final ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertThat(
            Streams.copyToString(new InputStreamReader(responseException.getResponse().getEntity().getContent(), UTF_8)),
            containsString(
                "no such index [recipe_kr] and the index creation request requires a data stream, "
                    + "but no matching index template with data stream template was found for it"
            )
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

    private void createTemplate(Boolean allowAutoCreate, boolean addDataStreamTemplate) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder();
        b.startObject();
        {
            b.array("index_patterns", "recipe*");
            if (allowAutoCreate != null) {
                b.field("allow_auto_create", allowAutoCreate);
            }
            if (addDataStreamTemplate) {
                b.startObject("data_stream");
                b.endObject();
            }
        }
        b.endObject();

        final Request createTemplateRequest = new Request("PUT", "_index_template/recipe_template");
        createTemplateRequest.setJsonEntity(Strings.toString(b));
        final Response createTemplateResponse = client().performRequest(createTemplateRequest);
        assertOK(createTemplateResponse);
    }

    private Response indexDocument() throws IOException {
        final Request indexDocumentRequest = prepareIndexRequest("recipe_kr");
        return client().performRequest(indexDocumentRequest);
    }

    private Request prepareIndexRequest(String indexName) {
        final Request indexDocumentRequest = new Request("POST", indexName + "/_doc");
        indexDocumentRequest.setJsonEntity("{ \"@timestamp\": \"" + Instant.now() + "\", \"name\": \"Kimchi\" }");
        return indexDocumentRequest;
    }
}
