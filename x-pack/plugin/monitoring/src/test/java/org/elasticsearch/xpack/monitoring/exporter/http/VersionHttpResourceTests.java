/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpResource.ResourcePublishResult;

import java.io.IOException;

import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.mockPublishResultActionListener;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.whenPerformRequestAsyncWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link VersionHttpResource}.
 */
public class VersionHttpResourceTests extends ESTestCase {

    private final String owner = getClass().getSimpleName();
    private final RestClient client = mock(RestClient.class);
    private final ActionListener<ResourcePublishResult> publishListener = mockPublishResultActionListener();

    public void testDoCheckAndPublishSuccess() {
        final Version minimumVersion = VersionUtils.randomVersion(random());
        final Version version = randomFrom(minimumVersion, Version.CURRENT);
        final Response response = responseForVersion(version);

        final VersionHttpResource resource = new VersionHttpResource(owner, minimumVersion);

        resource.doCheckAndPublish(client, publishListener);

        verify(publishListener).onResponse(ResourcePublishResult.ready());
        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedParsing() {
        // malformed JSON
        final Response response = responseForJSON("{");

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        resource.doCheckAndPublish(client, publishListener);

        verify(publishListener).onFailure(any(Exception.class));
        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedFieldMissing() {
        // malformed response; imagining that we may change it in the future or someone breaks filter_path
        final Response response = responseForJSON("{\"version.number\":\"" + Version.CURRENT + "\"}");

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        resource.doCheckAndPublish(client, publishListener);

        verify(publishListener).onFailure(any(Exception.class));
        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedFieldWrongType() {
        // malformed response (should be {version: { number : ... }})
        final Response response = responseForJSON("{\"version\":\"" + Version.CURRENT + "\"}");

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        resource.doCheckAndPublish(client, publishListener);

        verify(publishListener).onFailure(any(Exception.class));
        verify(response).getEntity();
    }

    public void testDoCheckAndPublishFailedWithIOException() {
        final Request request = new Request("GET", "/");
        request.addParameter("filter_path", "version.number");

        whenPerformRequestAsyncWith(client, request, new IOException("expected"));

        final VersionHttpResource resource = new VersionHttpResource(owner, Version.CURRENT);

        resource.doCheckAndPublish(client, publishListener);

        verify(publishListener).onFailure(any(Exception.class));
    }

    private Response responseForJSON(final String json) {
        final NStringEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);

        final Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(entity);

        final Request request = new Request("GET", "/");
        request.addParameter("filter_path", "version.number");

        whenPerformRequestAsyncWith(client, request, response);

        return response;
    }

    private Response responseForVersion(final Version version) {
        return responseForJSON("{\"version\":{\"number\":\"" + version + "\"}}");
    }

}
