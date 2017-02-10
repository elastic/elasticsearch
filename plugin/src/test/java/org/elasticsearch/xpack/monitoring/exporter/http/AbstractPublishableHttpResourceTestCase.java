/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test helper for any {@link PublishableHttpResource}.
 */
public abstract class AbstractPublishableHttpResourceTestCase extends ESTestCase {

    protected final String owner = getClass().getSimpleName();
    @Nullable
    protected final TimeValue masterTimeout = randomFrom(TimeValue.timeValueMinutes(5), null);

    protected final RestClient client = mock(RestClient.class);

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient) doCheck} against the {@code resource} and assert that it returns
     * {@code true} given a {@link RestStatus} that is {@link RestStatus#OK}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckExists(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName)
            throws IOException {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, successfulCheckStatus(), CheckResponse.EXISTS);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient) doCheck} against the {@code resource} and assert that it returns
     * {@code false} given a {@link RestStatus} that is not {@link RestStatus#OK}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckDoesNotExist(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName)
            throws IOException {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, notFoundCheckStatus(), CheckResponse.DOES_NOT_EXIST);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient) doCheck} against the {@code resource} that throws an exception and assert
     * that it returns {@code false}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckWithException(final PublishableHttpResource resource,
                                            final String resourceBasePath, final String resourceName)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final ResponseException responseException = responseException("GET", endpoint, failedCheckStatus());
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);

        when(client.performRequest("GET", endpoint, resource.getParameters())).thenThrow(e);

        assertThat(resource.doCheck(client), is(CheckResponse.ERROR));
    }

    /**
     * Perform {@link PublishableHttpResource#doPublish(RestClient) doPublish} against the {@code resource} and assert that it returns
     * {@code true} given a {@link RestStatus} that is {@link RestStatus#OK} or {@link RestStatus#CREATED}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     * @param bodyType The request body provider's type.
     */
    protected void assertPublishSucceeds(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final Class<? extends HttpEntity> bodyType)
            throws IOException {
        doPublishWithStatusCode(resource, resourceBasePath, resourceName, bodyType, successfulPublishStatus(), true);
    }

    /**
     * Perform {@link PublishableHttpResource#doPublish(RestClient) doPublish} against the {@code resource} and assert that it returns
     * {@code false} given a {@link RestStatus} that is neither {@link RestStatus#OK} or {@link RestStatus#CREATED}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     * @param bodyType The request body provider's type.
     */
    protected void assertPublishFails(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                      final Class<? extends HttpEntity> bodyType)
            throws IOException {
        doPublishWithStatusCode(resource, resourceBasePath, resourceName, bodyType, failedPublishStatus(), false);
    }

    /**
     * Perform {@link PublishableHttpResource#doPublish(RestClient) doPublish} against the {@code resource} that throws an exception and
     * assert that it returns {@code false}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertPublishWithException(final PublishableHttpResource resource,
                                              final String resourceBasePath, final String resourceName,
                                              final Class<? extends HttpEntity> bodyType)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"));

        when(client.performRequest(eq("PUT"), eq(endpoint), eq(resource.getParameters()), any(bodyType))).thenThrow(e);

        assertThat(resource.doPublish(client), is(false));
    }

    protected void assertParameters(final PublishableHttpResource resource) {
        final Map<String, String> parameters = resource.getParameters();

        if (masterTimeout != null) {
            assertThat(parameters.get("master_timeout"), is(masterTimeout.toString()));
        }

        assertThat(parameters.get("filter_path"), is("$NONE"));
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status,
                                         final CheckResponse expected)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status);

        doCheckWithStatusCode(resource, endpoint, expected, response);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String endpoint, final CheckResponse expected,
                                         final Response response)
            throws IOException {
        when(client.performRequest("GET", endpoint, resource.getParameters())).thenReturn(response);

        assertThat(resource.doCheck(client), is(expected));
    }

    private void doPublishWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final Class<? extends HttpEntity> bodyType,
                                         final RestStatus status,
                                         final boolean expected)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status);

        when(client.performRequest(eq("PUT"), eq(endpoint), eq(resource.getParameters()), any(bodyType))).thenReturn(response);

        assertThat(resource.doPublish(client), is(expected));
    }

    protected RestStatus successfulCheckStatus() {
        return RestStatus.OK;
    }

    protected RestStatus notFoundCheckStatus() {
        return RestStatus.NOT_FOUND;
    }

    protected RestStatus failedCheckStatus() {
        final Predicate<RestStatus> ignoreStatus = (final RestStatus status) -> status == RestStatus.OK || status == RestStatus.NOT_FOUND;
        return randomValueOtherThanMany(ignoreStatus, () -> randomFrom(RestStatus.values()));
    }

    protected RestStatus successfulPublishStatus() {
        return randomFrom(RestStatus.OK, RestStatus.CREATED);
    }

    protected RestStatus failedPublishStatus() {
        final Predicate<RestStatus> ignoreStatus = (final RestStatus status) -> status == RestStatus.OK || status == RestStatus.CREATED;
        return randomValueOtherThanMany(ignoreStatus, () -> randomFrom(RestStatus.values()));
    }

    protected String concatenateEndpoint(final String resourceBasePath, final String resourceName) {
        return resourceBasePath + "/" + resourceName;
    }

    protected Response response(final String method, final String endpoint, final RestStatus status) {
        final Response response = mock(Response.class);
        // fill out the response enough so that the exception can be constructed
        final RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn(method);
        when(requestLine.getUri()).thenReturn(endpoint);
        final StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(status.getStatus());

        when(response.getRequestLine()).thenReturn(requestLine);
        when(response.getStatusLine()).thenReturn(statusLine);

        return response;
    }

    protected ResponseException responseException(final String method, final String endpoint, final RestStatus status) {
        try {
            return new ResponseException(response(method, endpoint, status));
        } catch (final IOException e) {
            throw new IllegalStateException("update responseException to properly build the ResponseException", e);
        }
    }

}
