/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PutUserRequestBuilderTests extends ESTestCase {

    public void testNullValuesForEmailAndFullName() throws IOException {
        final String json = "{\n" +
                "    \"roles\": [\n" +
                "      \"kibana4\"\n" +
                "    ],\n" +
                "    \"full_name\": null,\n" +
                "    \"email\": null,\n" +
                "    \"metadata\": {}\n" +
                "}";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, null);

        PutUserRequest request = builder.request();
        assertThat(request.username(), is("kibana4"));
        assertThat(request.roles(), arrayContaining("kibana4"));
        assertThat(request.fullName(), nullValue());
        assertThat(request.email(), nullValue());
        assertThat(request.metadata().isEmpty(), is(true));
        assertTrue(request.enabled());
    }

    public void testMissingEmailFullName() throws Exception {
        final String json = "{\n" +
                "    \"roles\": [\n" +
                "      \"kibana4\"\n" +
                "    ],\n" +
                "    \"metadata\": {}\n" +
                "}";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, null);

        PutUserRequest request = builder.request();
        assertThat(request.username(), is("kibana4"));
        assertThat(request.roles(), arrayContaining("kibana4"));
        assertThat(request.fullName(), nullValue());
        assertThat(request.email(), nullValue());
        assertThat(request.metadata().isEmpty(), is(true));
    }

    public void testWithFullNameAndEmail() throws IOException {
        final String json = "{\n" +
                "    \"roles\": [\n" +
                "      \"kibana4\"\n" +
                "    ],\n" +
                "    \"full_name\": \"Kibana User\",\n" +
                "    \"email\": \"kibana@elastic.co\",\n" +
                "    \"metadata\": {}\n" +
                "}";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, null);

        PutUserRequest request = builder.request();
        assertThat(request.username(), is("kibana4"));
        assertThat(request.roles(), arrayContaining("kibana4"));
        assertThat(request.fullName(), is("Kibana User"));
        assertThat(request.email(), is("kibana@elastic.co"));
        assertThat(request.metadata().isEmpty(), is(true));
    }

    public void testInvalidFullname() throws IOException {
        final String json = "{\n" +
                "    \"roles\": [\n" +
                "      \"kibana4\"\n" +
                "    ],\n" +
                "    \"full_name\": [ \"Kibana User\" ],\n" +
                "    \"email\": \"kibana@elastic.co\",\n" +
                "    \"metadata\": {}\n" +
                "}";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, null));
        assertThat(e.getMessage(), containsString("expected field [full_name] to be of type string"));
    }

    public void testInvalidEmail() throws IOException {
        final String json = "{\n" +
                "    \"roles\": [\n" +
                "      \"kibana4\"\n" +
                "    ],\n" +
                "    \"full_name\": \"Kibana User\",\n" +
                "    \"email\": [ \"kibana@elastic.co\" ],\n" +
                "    \"metadata\": {}\n" +
                "}";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, null));
        assertThat(e.getMessage(), containsString("expected field [email] to be of type string"));
    }

    public void testWithEnabled() throws IOException {
        final String json = "{\n" +
                "    \"roles\": [\n" +
                "      \"kibana4\"\n" +
                "    ],\n" +
                "    \"full_name\": \"Kibana User\",\n" +
                "    \"email\": \"kibana@elastic.co\",\n" +
                "    \"metadata\": {}\n," +
                "    \"enabled\": false\n" +
                "}";

        PutUserRequestBuilder builder = new PutUserRequestBuilder(mock(Client.class));
        PutUserRequest request =
            builder.source("kibana4", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON, null).request();
        assertFalse(request.enabled());
    }
}
