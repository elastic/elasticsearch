/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link VersionHttpResource}.
 */
public class VersionHttpResourceTests extends ESTestCase {

    private final String owner = getClass().getSimpleName();
    private final RestClient client = mock(RestClient.class);

    public void testDoCheckAndPublishSuccess() throws IOException {
        final Version minimumVersion = VersionUtils.randomVersion(random());
        final Version version = randomFrom(minimumVersion, Version.CURRENT);
        final Response response = responseForVersion(version);

        final VersionHttpResource resource = new VersionHttpResource(owner, minimumVersion);

        assertTrue(resource.doCheckAndPublish(client));

        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedParsing() throws IOException {
        // malformed JSON
        final Response response = responseForJSON("{");

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        assertFalse(resource.doCheckAndPublish(client));

        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedFieldMissing() throws IOException {
        // malformed response; imagining that we may change it in the future or someone breaks filter_path
        final Response response = responseForJSON("{\"version.number\":\"" + Version.CURRENT + "\"}");

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        assertFalse(resource.doCheckAndPublish(client));

        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedFieldWrongType() throws IOException {
        // malformed response (should be {version: { number : ... }})
        final Response response = responseForJSON("{\"version\":\"" + Version.CURRENT + "\"}");

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        assertFalse(resource.doCheckAndPublish(client));

        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedWithIOException() throws IOException {
        Request request = new Request("GET", "/");
        request.addParameter("filter_path", "version.number");
        when(client.performRequest(request)).thenThrow(new IOException("expected"));

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        assertFalse(resource.doCheckAndPublish(client));
    }

    private Response responseForJSON(final String json) throws IOException {
        final NStringEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);

        final Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(entity);

        Request request = new Request("GET", "/");
        request.addParameter("filter_path", "version.number");
        when(client.performRequest(request)).thenReturn(response);

        return response;
    }

    private Response responseForVersion(final Version version) throws IOException {
        return responseForJSON("{\"version\":{\"number\":\"" + version + "\"}}");
    }

}
