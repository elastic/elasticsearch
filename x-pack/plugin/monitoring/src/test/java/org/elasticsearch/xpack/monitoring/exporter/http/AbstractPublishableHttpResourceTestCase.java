/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpResource.ResourcePublishResult;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.mockBooleanActionListener;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.mockPublishResultActionListener;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.whenPerformRequestAsyncWith;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.wrapMockListener;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.GET_DOES_NOT_EXIST;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.GET_EXISTS;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Base test helper for any {@link PublishableHttpResource}.
 */
public abstract class AbstractPublishableHttpResourceTestCase extends ESTestCase {

    protected final String owner = getClass().getSimpleName();
    @Nullable
    protected final TimeValue masterTimeout = randomFrom(TimeValue.timeValueMinutes(5), TimeValue.MINUS_ONE, null);

    protected final RestClient client = mock(RestClient.class);
    protected final ActionListener<Boolean> checkListener = mockBooleanActionListener();
    protected final ActionListener<ResourcePublishResult> publishListener = mockPublishResultActionListener();

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient, ActionListener) doCheck} against the {@code resource} and assert that it
     * returns {@code onResponse(false)} given a {@link RestStatus} that is not {@link RestStatus#OK}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckDoesNotExist(final PublishableHttpResource resource,
                                           final String resourceBasePath,
                                           final String resourceName) {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, notFoundCheckStatus(), false);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient, ActionListener) doCheck} against the {@code resource} that throws an
     * exception and assert that it returns {@code onFailure}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckWithException(final PublishableHttpResource resource,
                                            final String resourceBasePath, final String resourceName) {
        assertCheckWithException(resource, getParameters(resource.getDefaultParameters()), resourceBasePath, resourceName);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient, ActionListener) doCheck} against the {@code resource} that throws an
     * exception and assert that it returns {@code onFailure}.
     *
     * @param resource The resource to execute.
     * @param expectedParameters The test-supplied parameters for the {@code Request}.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckWithException(final PublishableHttpResource resource,
                                            final Map<String, String> expectedParameters,
                                            final String resourceBasePath, final String resourceName) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final ResponseException responseException = responseException("GET", endpoint, failedCheckStatus());
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);

        final Request request = new Request("GET", endpoint);
        addParameters(request, expectedParameters);
        whenPerformRequestAsyncWith(client, request, e);

        resource.doCheck(client, wrapMockListener(checkListener));

        verifyCheckListener(null);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient, ActionListener) doCheck} against the {@code resource}, expecting a
     * {@code DELETE}, and assert that it returns {@code onResponse(true)} given a {@link RestStatus} that is {@link RestStatus#OK} or
     * {@link RestStatus#NOT_FOUND}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckAsDeleteExists(final PublishableHttpResource resource,
                                             final String resourceBasePath, final String resourceName) {
        final RestStatus status = randomFrom(successfulCheckStatus(), notFoundCheckStatus());

        doCheckAsDeleteWithStatusCode(resource, resourceBasePath, resourceName, status, true);
    }

    /**
     * Perform {@link PublishableHttpResource#doCheck(RestClient, ActionListener) doCheck} against the {@code resource} that throws an
     * exception and assert that it returns {@code onFailure()} when performing a {@code DELETE} rather than the more common {@code GET}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertCheckAsDeleteWithException(final PublishableHttpResource resource,
                                                    final String resourceBasePath, final String resourceName) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final ResponseException responseException = responseException("DELETE", endpoint, failedCheckStatus());
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);

        final Request request = new Request("DELETE", endpoint);
        addParameters(request, deleteParameters(resource.getDefaultParameters()));
        whenPerformRequestAsyncWith(client, request, e);

        resource.doCheck(client, wrapMockListener(checkListener));

        verifyCheckListener(null);
    }

    /**
     * Perform {@link PublishableHttpResource#doPublish(RestClient, ActionListener) doPublish} against the {@code resource} and assert that
     * it returns {@code onResponse(true)} given a {@link RestStatus} that is {@link RestStatus#OK} or {@link RestStatus#CREATED}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param resourceName The resource name (e.g., the template or pipeline name).
     * @param parameters Map of query string parameters, if any.
     * @param bodyType The request body provider's type.
     */
    protected void assertPublishSucceeds(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         Map<String, String> parameters,
                                         final Class<? extends HttpEntity> bodyType) {
        doPublishWithStatusCode(resource, resourceBasePath, resourceName, parameters, bodyType, successfulPublishStatus(), true);
    }

    /**
     * Perform {@link PublishableHttpResource#doPublish(RestClient, ActionListener) doPublish} against the {@code resource} that throws an
     * exception and assert that it returns {@code onResponse(false)}.
     *
     * @param resource The resource to execute.
     * @param resourceBasePath The base endpoint (e.g., "/_template")
     * @param parameters Map of query string parameters, if any.
     * @param resourceName The resource name (e.g., the template or pipeline name).
     */
    protected void assertPublishWithException(final PublishableHttpResource resource,
                                              final String resourceBasePath, final String resourceName,
                                              Map<String, String> parameters,
                                              final Class<? extends HttpEntity> bodyType) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"));

        whenPerformRequestAsyncWith(client, e);

        resource.doPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(null);

        Map <String, String> allParameters = new HashMap<>();
        allParameters.putAll(resource.getDefaultParameters());
        allParameters.putAll(parameters);

        final ArgumentCaptor<Request> request = ArgumentCaptor.forClass(Request.class);
        verify(client).performRequestAsync(request.capture(), any(ResponseListener.class));
        assertThat(request.getValue().getMethod(), is("PUT"));
        assertThat(request.getValue().getEndpoint(), is(endpoint));
        assertThat(request.getValue().getParameters(), is(allParameters));
        assertThat(request.getValue().getEntity(), instanceOf(bodyType));
    }

    protected void assertParameters(final PublishableHttpResource resource) {
        final Map<String, String> parameters = new HashMap<>(resource.getDefaultParameters());

        if (masterTimeout != null && TimeValue.MINUS_ONE.equals(masterTimeout) == false) {
            assertThat(parameters.remove("master_timeout"), is(masterTimeout.toString()));
        } else {
            assertFalse(parameters.containsKey("master_timeout"));
        }

        assertThat(parameters.remove("filter_path"), is("$NONE"));
        assertThat(parameters.isEmpty(), is(true));
    }

    protected void assertVersionParameters(final PublishableHttpResource resource) {
        final Map<String, String> parameters = new HashMap<>(resource.getDefaultParameters());

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
                                         final Boolean expected) {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, status, expected, null);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status, final Boolean expected, final HttpEntity entity) {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, status, GET_EXISTS, GET_DOES_NOT_EXIST, expected, entity);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status, final Set<Integer> exists, final Set<Integer> doesNotExist,
                                         final Boolean expected) {
        doCheckWithStatusCode(resource, resourceBasePath, resourceName, status, exists, doesNotExist, expected, null);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         final RestStatus status, final Set<Integer> exists, final Set<Integer> doesNotExist,
                                         final Boolean expected, final HttpEntity entity) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status, entity);

        doCheckWithStatusCode(resource, getParameters(resource.getDefaultParameters(), exists, doesNotExist), endpoint, expected, response);
    }

    protected void doCheckWithStatusCode(final PublishableHttpResource resource, final Map<String, String> expectedParameters,
                                         final String endpoint, final Boolean expected,
                                         final Response response) {
        final Request request = new Request("GET", endpoint);
        addParameters(request, expectedParameters);

        whenPerformRequestAsyncWith(client, request, response);

        resource.doCheck(client, wrapMockListener(checkListener));

        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verifyCheckListener(expected);
    }

    private void doPublishWithStatusCode(final PublishableHttpResource resource, final String resourceBasePath, final String resourceName,
                                         Map<String, String> parameters,
                                         final Class<? extends HttpEntity> bodyType,
                                         final RestStatus status,
                                         final boolean errorFree) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status);

        whenPerformRequestAsyncWith(client, response);

        resource.doPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(errorFree ? ResourcePublishResult.ready() : null);

        final ArgumentCaptor<Request> request = ArgumentCaptor.forClass(Request.class);
        verify(client).performRequestAsync(request.capture(), any(ResponseListener.class));

        Map <String, String> allParameters = new HashMap<>();
        allParameters.putAll(resource.getDefaultParameters());
        allParameters.putAll(parameters);

        assertThat(request.getValue().getMethod(), is("PUT"));
        assertThat(request.getValue().getEndpoint(), is(endpoint));
        assertThat(request.getValue().getParameters(), is(allParameters));
        assertThat(request.getValue().getEntity(), instanceOf(bodyType));
    }

    protected void doCheckAsDeleteWithStatusCode(final PublishableHttpResource resource,
                                                 final String resourceBasePath, final String resourceName,
                                                 final RestStatus status,
                                                 final Boolean expected) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("DELETE", endpoint, status);

        doCheckAsDeleteWithStatusCode(resource, endpoint, expected, response);
    }

    protected void doCheckAsDeleteWithStatusCode(final PublishableHttpResource resource,
                                                 final String endpoint, final Boolean expected,
                                                 final Response response) {
        final Request request = new Request("DELETE", endpoint);
        addParameters(request, deleteParameters(resource.getDefaultParameters()));
        whenPerformRequestAsyncWith(client, request, response);

        resource.doCheck(client, wrapMockListener(checkListener));

        verifyCheckListener(expected);
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

    protected HttpEntity entityForResource(final Boolean expected, final String resourceName, final int minimumVersion) {
        if (expected == Boolean.FALSE) {
            // the version check is what is expected to cause it to be replaced
            final int olderVersion = minimumVersion - randomIntBetween(1, 10000);

            return randomFrom(
                new StringEntity("{}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"" + resourceName + "\":{}}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"" + resourceName + "\":{\"version\":\"123\"}}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"" + resourceName + "\":{\"version\":" + olderVersion + "}}", ContentType.APPLICATION_JSON)
            );
        } else if (expected == Boolean.TRUE) {
            // the version is there and it's exactly what we specify
            return new StringEntity("{\"" + resourceName + "\":{\"version\":" + minimumVersion + "}}", ContentType.APPLICATION_JSON);
        } else { // expected = null, which is for malformed/failure
            // malformed
            return randomFrom(
                new StringEntity("{", ContentType.APPLICATION_JSON),
                new StringEntity("{\"" + resourceName + "\":\"not an object\"}", ContentType.APPLICATION_JSON)
            );
        }
    }

    protected HttpEntity entityForClusterAlert(final Boolean expected, final int minimumVersion) {
        if (expected == Boolean.FALSE) {
            // the version check is what is expected to cause it to be replaced
            final int olderVersion = minimumVersion - randomIntBetween(1, 10000);

            return randomFrom(
                new StringEntity("{}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"metadata\":{}}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"metadata\":{\"xpack\":{\"version_created\":\"123\"}}}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"metadata\":{\"xpack\":{\"version_created\":" + olderVersion + "}}}}", ContentType.APPLICATION_JSON)
            );
        } else if (expected == Boolean.TRUE) {
            // the version is there and it's exactly what we specify
            return new StringEntity("{\"metadata\":{\"xpack\":{\"version_created\":" +
                                    minimumVersion + "}}}", ContentType.APPLICATION_JSON);
        } else { // expected == null, which is for malformed/failure
            // malformed
            return randomFrom(
                new StringEntity("{", ContentType.APPLICATION_JSON),
                new StringEntity("{\"\"metadata\":\"not an object\"}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"\"metadata\":{\"xpack\":\"not an object\"}}", ContentType.APPLICATION_JSON)
            );
        }
    }

    protected void addParameters(final Request request, final Map<String, String> parameters) {
        for (Map.Entry<String, String> param : parameters.entrySet()) {
            request.addParameter(param.getKey(), param.getValue());
        }
    }

    protected void verifyPublishListener(final ResourcePublishResult expected) {
        if (expected == null) {
            verify(publishListener, never()).onResponse(any());
            verify(publishListener).onFailure(any(Exception.class));
        } else {
            verify(publishListener).onResponse(expected);
            verify(publishListener, never()).onFailure(any(Exception.class));
        }
    }

    protected void verifyCheckListener(final Boolean expected) {
        if (expected == null) {
            verify(checkListener, never()).onResponse(anyBoolean());
            verify(checkListener).onFailure(any(Exception.class));
        } else {
            verify(checkListener).onResponse(expected);
            verify(checkListener, never()).onFailure(any(Exception.class));
        }
    }

}
