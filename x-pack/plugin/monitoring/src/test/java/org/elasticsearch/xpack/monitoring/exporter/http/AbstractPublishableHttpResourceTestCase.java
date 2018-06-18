/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.HashMap;
import org.apache.http.HttpEntity;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.GET_DOES_NOT_EXIST;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.GET_EXISTS;
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
    protected final TimeValue masterTimeout = randomFrom(TimeValue.timeValueMinutes(5), TimeValue.MINUS_ONE, null);

    protected final RestClient client = mock(RestClient.class);

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient) doCheck} against the {@code resource} and assert that it returns
     * {@code EXISTS} given a {@link RestStatus} that is {@link RestStatus#OK}.
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
     * {@code DOES_NOT_EXIST} given a {@link RestStatus} that is not {@link RestStatus#OK}.
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
     * that it returns {@code ERROR}.
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

        when(client.performRequest("GET", endpoint, getParameters(resource.getParameters()))).thenThrow(e);

        assertThat(resource.doCheck(client), is(CheckResponse.ERROR));
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient) doCheck} against the {@code resource}, expecting a {@code DELETE}, and
     * assert that it returns {@code EXISTS} given a {@link RestStatus} that is {@link RestStatus#OK} or {@link RestStatus#NOT_FOUND}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckAsDeleteExists(final PublishableHttpResource resource,
                                             final String resourceBasePath, final String resourceName)
            throws IOException {
        final RestStatus status = randomFrom(successfulCheckStatus(), notFoundCheckStatus());

        doCheckAsDeleteWithStatusCode(resource, resourceBasePath, resourceName, status, CheckResponse.EXISTS);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient) doCheck} against the {@code resource} that throws an exception and assert
     * that it returns {@code ERRPR} when performing a {@code DELETE} rather than the more common {@code GET}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckAsDeleteWithException(final PublishableHttpResource resource,
                                                    final String resourceBasePath, final String resourceName)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final ResponseException responseException = responseException("DELETE", endpoint, failedCheckStatus());
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);

        when(client.performRequest("DELETE", endpoint, deleteParameters(resource.getParameters()))).thenThrow(e);

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
        final Map<String, String> parameters = new HashMap<>(resource.getParameters());

        if (masterTimeout != null && TimeValue.MINUS_ONE.equals(masterTimeout) == false) {
            assertThat(parameters.remove("master_timeout"), is(masterTimeout.toString()));
        } else {
            assertFalse(parameters.containsKey("master_timeout"));
        }

        assertThat(parameters.remove("filter_path"), is("$NONE"));
        assertThat(parameters.isEmpty(), is(true));
    }

    protected void assertVersionParameters(final PublishableHttpResource resource) {
        final Map<String, String> parameters = new HashMap<>(resource.getParameters());

        if (masterTimeout != null && TimeValue.MINUS_ONE.equals(masterTimeout) == false) {
            assertThat(parameters.remove("master_timeout"), is(masterTimeout.toString()));
        } else {
            assertFalse(parameters.containsKey("master_timeout"));
        }

        assertThat(parameters.remove("filter_path"), is("*.version"));
        assertThat(parameters.isEmpty(), is(true));
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status,
                                         final CheckResponse expected)
            throws IOException {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, status, expected, null);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status, final CheckResponse expected, final HttpEntity entity)
            throws IOException {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, status, GET_EXISTS, GET_DOES_NOT_EXIST, expected, entity);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status, final Set<Integer> exists, final Set<Integer> doesNotExist,
                                         final CheckResponse expected)
            throws IOException {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, status, exists, doesNotExist, expected, null);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status, final Set<Integer> exists, final Set<Integer> doesNotExist,
                                         final CheckResponse expected, final HttpEntity entity)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status, entity);

        doCheckWithStatusCode(resource, getParameters(resource.getParameters(), exists, doesNotExist), endpoint, expected, response);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String endpoint, final CheckResponse expected,
                                         final Response response)
            throws IOException {
        doCheckWithStatusCode(resource, getParameters(resource.getParameters()), endpoint, expected, response);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final Map<String, String> expectedParameters,
                                         final String endpoint, final CheckResponse expected,
                                         final Response response)
            throws IOException {
        when(client.performRequest("GET", endpoint, expectedParameters)).thenReturn(response);

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

    protected void doCheckAsDeleteWithStatusCode(final PublishableHttpResource resource,
                                                 final String resourceBasePath, final String resourceName,
                                                 final RestStatus status,
                                                 final CheckResponse expected)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("DELETE", endpoint, status);

        doCheckAsDeleteWithStatusCode(resource, endpoint, expected, response);
    }

    protected void doCheckAsDeleteWithStatusCode(final PublishableHttpResource resource,
                                                 final String endpoint, final CheckResponse expected,
                                                 final Response response)
            throws IOException {
        when(client.performRequest("DELETE", endpoint, deleteParameters(resource.getParameters()))).thenReturn(response);

        assertThat(resource.doCheck(client), is(expected));
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
        return response(method, endpoint, status, null);
    }

    protected Response response(final String method, final String endpoint, final RestStatus status, final HttpEntity entity) {
        final Response response = mock(Response.class);
        // fill out the response enough so that the exception can be constructed
        final RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn(method);
        when(requestLine.getUri()).thenReturn(endpoint);
        final StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(status.getStatus());

        when(response.getRequestLine()).thenReturn(requestLine);
        when(response.getStatusLine()).thenReturn(statusLine);

        when(response.getEntity()).thenReturn(entity);

        return response;
    }

    protected ResponseException responseException(final String method, final String endpoint, final RestStatus status) {
        try {
            return new ResponseException(response(method, endpoint, status));
        } catch (final IOException e) {
            throw new IllegalStateException("update responseException to properly build the ResponseException", e);
        }
    }

    protected Map<String, String> getParameters(final Map<String, String> parameters) {
        return getParameters(parameters, GET_EXISTS, GET_DOES_NOT_EXIST);
    }

    protected Map<String, String> getParameters(final Map<String, String> parameters,
                                                final Set<Integer> exists, final Set<Integer> doesNotExist) {
        final Set<Integer> statusCodes = Sets.union(exists, doesNotExist);
        final Map<String, String> parametersWithIgnore = new HashMap<>(parameters);

        parametersWithIgnore.putIfAbsent("ignore", statusCodes.stream().map(i -> i.toString()).collect(Collectors.joining(",")));

        return parametersWithIgnore;
    }

    protected Map<String, String> deleteParameters(final Map<String, String> parameters) {
        final Map<String, String> parametersWithIgnore = new HashMap<>(parameters);

        parametersWithIgnore.putIfAbsent("ignore", "404");

        return parametersWithIgnore;
    }

    protected HttpEntity entityForResource(final CheckResponse expected, final String resourceName, final int minimumVersion) {
        HttpEntity entity = null;

        switch (expected) {
            // the version check is what is expected to cause it to be replaced
            case DOES_NOT_EXIST:
                final int olderVersion = minimumVersion - randomIntBetween(1, 10000);

                entity = randomFrom(
                    new StringEntity("{}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"" + resourceName + "\":{}}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"" + resourceName + "\":{\"version\":\"123\"}}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"" + resourceName + "\":{\"version\":" + olderVersion + "}}", ContentType.APPLICATION_JSON)
                );
                break;
            // the version is there and it's exactly what we specify
            case EXISTS:
                entity = new StringEntity("{\"" + resourceName + "\":{\"version\":" + minimumVersion + "}}", ContentType.APPLICATION_JSON);
                break;
            // malformed
            case ERROR:
                entity = randomFrom(
                    new StringEntity("{", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"" + resourceName + "\":\"not an object\"}", ContentType.APPLICATION_JSON)
                );
                break;
            default:
                fail("Unhandled/unknown CheckResponse");
        }

        return entity;
    }

    protected HttpEntity entityForClusterAlert(final CheckResponse expected, final int minimumVersion) {
        HttpEntity entity = null;

        switch (expected) {
            // the version check is what is expected to cause it to be replaced
            case DOES_NOT_EXIST:
                final int olderVersion = minimumVersion - randomIntBetween(1, 10000);

                entity = randomFrom(
                    new StringEntity("{}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"metadata\":{}}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"metadata\":{\"xpack\":{\"version_created\":\"123\"}}}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"metadata\":{\"xpack\":{\"version_created\":" + olderVersion + "}}}}", ContentType.APPLICATION_JSON)
                );
                break;
            // the version is there and it's exactly what we specify
            case EXISTS:
                entity = new StringEntity("{\"metadata\":{\"xpack\":{\"version_created\":" +
                                          minimumVersion + "}}}", ContentType.APPLICATION_JSON);
                break;
            // malformed
            case ERROR:
                entity = randomFrom(
                    new StringEntity("{", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"\"metadata\":\"not an object\"}", ContentType.APPLICATION_JSON),
                    new StringEntity("{\"\"metadata\":{\"xpack\":\"not an object\"}}", ContentType.APPLICATION_JSON)
                );
                break;
            default:
                fail("Unhandled/unknown CheckResponse");
        }

        return entity;
    }

}
