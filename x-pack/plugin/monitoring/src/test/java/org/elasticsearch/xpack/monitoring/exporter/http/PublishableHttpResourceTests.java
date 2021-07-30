/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.Collections;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import org.elasticsearch.xpack.monitoring.exporter.http.HttpResource.ResourcePublishResult;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.whenPerformRequestAsyncWith;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.wrapMockListener;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link PublishableHttpResource}.
 */
public class PublishableHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final String ownerType = "ownerthing";
    private final String resourceBasePath = "/_fake";
    private final String resourceName = ".my_thing";
    private final String resourceType = "thingamajig";
    private final Logger logger = mock(Logger.class);
    private final HttpEntity entity = mock(HttpEntity.class);
    private final Supplier<HttpEntity> body = () -> entity;

    private final PublishableHttpResource resource =
            new MockHttpResource(owner, masterTimeout, PublishableHttpResource.NO_BODY_PARAMETERS);

    public void testCheckForResourceExists() throws IOException {
        assertCheckForResource(successfulCheckStatus(), true, "{} [{}] found on the [{}] {}");
    }

    public void testCheckForResourceDoesNotExist() throws IOException {
        assertCheckForResource(notFoundCheckStatus(), false, "{} [{}] does not exist on the [{}] {}");
    }

    public void testCheckForResourceUnexpectedResponse() throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus failedStatus = failedCheckStatus();
        final Response response = response("GET", endpoint, failedStatus);
        final Request request = new Request("GET", endpoint);
        addParameters(request, getParameters(resource.getDefaultParameters()));

        whenPerformRequestAsyncWith(client, request, response);

        assertCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, null, response);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), any(ResponseException.class));

        verifyNoMoreInteractions(client, logger);
    }

    public void testVersionCheckForResourceExists() {
        assertVersionCheckForResource(successfulCheckStatus(), true, randomInt(), "{} [{}] found on the [{}] {}");
    }

    public void testVersionCheckForResourceDoesNotExist() {
        if (randomBoolean()) {
            // it literally does not exist
            assertVersionCheckForResource(notFoundCheckStatus(), false,
                                          randomInt(), "{} [{}] does not exist on the [{}] {}");
        } else {
            // it DOES exist, but the version needs to be replaced
            assertVersionCheckForResource(successfulCheckStatus(), false,
                                          randomInt(), "{} [{}] found on the [{}] {}");
        }
    }

    public void testVersionCheckForResourceUnexpectedResponse() {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus failedStatus = failedCheckStatus();
        final Response response = response("GET", endpoint, failedStatus);
        final XContent xContent = mock(XContent.class);
        final int minimumVersion = randomInt();
        final Request request = new Request("GET", endpoint);
        addParameters(request, getParameters(resource.getDefaultParameters()));

        whenPerformRequestAsyncWith(client, request, response);

        resource.versionCheckForResource(client, wrapMockListener(checkListener), logger,
                                         resourceBasePath, resourceName, resourceType, owner, ownerType,
                                         xContent, minimumVersion);

        verifyCheckListener(null);
        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), any(ResponseException.class));

        verifyNoMoreInteractions(client, logger);
    }

    public void testVersionCheckForResourceMalformedResponse() {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus okStatus = successfulCheckStatus();
        final int minimumVersion = randomInt();
        final HttpEntity entity = entityForResource(null, resourceName, minimumVersion);
        final Response response = response("GET", endpoint, okStatus, entity);
        final XContent xContent = mock(XContent.class);
        final Request request = new Request("GET", endpoint);
        addParameters(request, getParameters(resource.getDefaultParameters()));

        whenPerformRequestAsyncWith(client, request, response);

        resource.versionCheckForResource(client, wrapMockListener(checkListener), logger,
                                         resourceBasePath, resourceName, resourceType, owner, ownerType,
                                         xContent, minimumVersion);

        verifyCheckListener(null);
        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(logger).debug("{} [{}] found on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(logger, times(2)).error(any(org.apache.logging.log4j.util.Supplier.class), any(ResponseException.class));

        verifyNoMoreInteractions(client, logger);
    }

    public void testCheckForResourceErrors() throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus failedStatus = failedCheckStatus();
        final ResponseException responseException = responseException("GET", endpoint, failedStatus);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
        final Response response = e == responseException ? responseException.getResponse() : null;

        final Request request = new Request("GET", endpoint);
        addParameters(request, getParameters(resource.getDefaultParameters()));

        whenPerformRequestAsyncWith(client, request, e);

        assertCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, null, response);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), eq(e));

        verifyNoMoreInteractions(client, logger);
    }

    public void testPutResourceTrue() {
        assertPutResource(successfulPublishStatus(), true);
    }

    public void testPutResourceFalse() {
        assertPutResource(failedPublishStatus(), false);
    }

    public void testPutResourceFalseWithException() {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"));
        final Request request = new Request("PUT", endpoint);
        addParameters(request, resource.getDefaultParameters());
        request.setEntity(entity);

        whenPerformRequestAsyncWith(client, request, e);

        final Map<String, String> parameters = Collections.emptyMap();
        resource.putResource(client, wrapMockListener(publishListener), logger, resourceBasePath, resourceName, parameters, body,
            resourceType, owner, ownerType);

        verifyPublishListener(null);

        verify(logger).trace("uploading {} [{}] to the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), eq(e));

        verifyNoMoreInteractions(client, logger);
    }

    public void testDeleteResourceTrue() {
        final RestStatus status = randomFrom(successfulCheckStatus(), notFoundCheckStatus());

        assertDeleteResource(status, true);
    }

    public void testDeleteResourceFalse() {
        assertDeleteResource(failedCheckStatus(), false);
    }

    public void testDeleteResourceErrors() {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus failedStatus = failedCheckStatus();
        final ResponseException responseException = responseException("DELETE", endpoint, failedStatus);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
        final Map<String, String> deleteParameters = deleteParameters(resource.getDefaultParameters());
        final Request request = new Request("DELETE", endpoint);
        addParameters(request, deleteParameters);

        whenPerformRequestAsyncWith(client, request, e);

        resource.deleteResource(client, wrapMockListener(checkListener), logger, resourceBasePath, resourceName, resourceType, owner,
            ownerType);

        verifyCheckListener(null);

        verify(logger).trace("deleting {} [{}] from the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), eq(e));

        verifyNoMoreInteractions(client, logger);
    }

    public void testParameters() {
        assertParameters(resource);
    }

    public void testDoCheckAndPublishIgnoresPublishWhenCheckErrors() {
        final PublishableHttpResource resource =
                new MockHttpResource(owner, masterTimeout, PublishableHttpResource.NO_BODY_PARAMETERS, null, true);

        resource.doCheckAndPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(null);
    }

    public void testDoCheckAndPublish() {
        // not an error (the third state)
        final boolean exists = randomBoolean();
        final boolean publish = randomBoolean();

        final PublishableHttpResource resource =
                new MockHttpResource(owner, masterTimeout, PublishableHttpResource.NO_BODY_PARAMETERS, exists, publish);

        resource.doCheckAndPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(new ResourcePublishResult(exists || publish));
    }

    public void testShouldReplaceResourceRethrowsIOException() throws IOException {
        final Response response = mock(Response.class);
        final HttpEntity entity = mock(HttpEntity.class);
        final XContent xContent = mock(XContent.class);

        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenThrow(new IOException("TEST - expected"));

        expectThrows(IOException.class, () -> resource.shouldReplaceResource(response, xContent, resourceName, randomInt()));
    }

    public void testShouldReplaceResourceThrowsExceptionForMalformedResponse() {
        final Response response = mock(Response.class);
        final HttpEntity entity = entityForResource(null, resourceName, randomInt());
        final XContent xContent = XContentType.JSON.xContent();

        when(response.getEntity()).thenReturn(entity);

        expectThrows(RuntimeException.class, () -> resource.shouldReplaceResource(response, xContent, resourceName, randomInt()));
    }

    public void testShouldReplaceResourceReturnsTrueVersionIsNotExpected() throws IOException {
        final int minimumVersion = randomInt();
        final Response response = mock(Response.class);
        final HttpEntity entity = entityForResource(false, resourceName, minimumVersion);
        final XContent xContent = XContentType.JSON.xContent();

        when(response.getEntity()).thenReturn(entity);

        assertThat(resource.shouldReplaceResource(response, xContent, resourceName, minimumVersion), is(true));
    }

    public void testShouldReplaceResourceChecksVersion() throws IOException {
        final int minimumVersion = randomInt();
        final int version = randomInt();
        final boolean shouldReplace = version < minimumVersion;

        final Response response = mock(Response.class);
        // { "resourceName": { "version": randomLong } }
        final HttpEntity entity =
                new StringEntity("{\"" + resourceName + "\":{\"version\":" + version + "}}", ContentType.APPLICATION_JSON);
        final XContent xContent = XContentType.JSON.xContent();

        when(response.getEntity()).thenReturn(entity);

        assertThat(resource.shouldReplaceResource(response, xContent, resourceName, minimumVersion), is(shouldReplace));
    }

    @SuppressLoggerChecks(reason = "mock logger used")
    private void assertCheckForResource(final RestStatus status, final Boolean expected, final String debugLogMessage)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status);
        final Request request = new Request("GET", endpoint);
        addParameters(request, getParameters(resource.getDefaultParameters()));

        whenPerformRequestAsyncWith(client, request, response);

        assertCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, expected, response);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));

        if (expected != null) {
            verify(response).getStatusLine();
        } else {
            verify(response).getStatusLine();
            verify(response).getRequestLine();
            verify(response).getHost();
            verify(response).getEntity();
        }

        verify(logger).debug(debugLogMessage, resourceType, resourceName, owner, ownerType);

        verifyNoMoreInteractions(client, response, logger);
    }

    @SuppressLoggerChecks(reason = "mock logger used")
    private void assertVersionCheckForResource(final RestStatus status, final Boolean expected,
                                               final int minimumVersion,
                                               final String debugLogMessage) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final boolean shouldReplace = status == RestStatus.OK && expected == Boolean.FALSE;
        final HttpEntity entity = status == RestStatus.OK ? entityForResource(expected, resourceName, minimumVersion) : null;
        final Response response = response("GET", endpoint, status, entity);
        final XContent xContent = XContentType.JSON.xContent();
        final Request request = new Request("GET", endpoint);
        addParameters(request, getParameters(resource.getDefaultParameters()));

        whenPerformRequestAsyncWith(client, request, response);

        resource.versionCheckForResource(client, wrapMockListener(checkListener), logger,
                                         resourceBasePath, resourceName, resourceType, owner, ownerType,
                                         xContent, minimumVersion);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));

        if (shouldReplace || expected) {
            verify(response).getStatusLine();
            verify(response).getEntity();
        } else if (expected == false) {
            verify(response).getStatusLine();
        } else { // expected == null
            verify(response).getStatusLine();
            verify(response).getRequestLine();
            verify(response).getHost();
            verify(response).getEntity();
        }

        verifyCheckListener(expected);
        verify(logger).debug(debugLogMessage, resourceType, resourceName, owner, ownerType);

        verifyNoMoreInteractions(client, response, logger);
    }

    private void assertPutResource(final RestStatus status, final boolean errorFree) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("PUT", endpoint, status);
        final Request request = new Request("PUT", endpoint);
        addParameters(request, resource.getDefaultParameters());
        request.setEntity(entity);

        whenPerformRequestAsyncWith(client, request, response);

        final Map<String, String> parameters = Collections.emptyMap();
        resource.putResource(client, wrapMockListener(publishListener), logger, resourceBasePath, resourceName, parameters, body,
            resourceType, owner, ownerType);

        verifyPublishListener(errorFree ? ResourcePublishResult.ready() : null);
        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(response).getStatusLine();

        verify(logger).trace("uploading {} [{}] to the [{}] {}", resourceType, resourceName, owner, ownerType);

        if (errorFree) {
            verify(logger).debug("{} [{}] uploaded to the [{}] {}", resourceType, resourceName, owner, ownerType);
        } else {
            ArgumentCaptor<RuntimeException> e = ArgumentCaptor.forClass(RuntimeException.class);

            verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), e.capture());

            assertThat(e.getValue().getMessage(),
                       is("[" + resourceBasePath + "/" + resourceName + "] responded with [" + status.getStatus() + "]"));
        }

        verifyNoMoreInteractions(client, response, logger, entity);
    }

    @SuppressWarnings("unchecked")
    private void assertCheckForResource(final RestClient client, final Logger logger,
                                        final String resourceBasePath, final String resourceName, final String resourceType,
                                        final Boolean expected, final Response response)
            throws IOException {
        final CheckedFunction<Response, Boolean, IOException> responseChecker = mock(CheckedFunction.class);
        final CheckedFunction<Response, Boolean, IOException> dneResponseChecker = mock(CheckedFunction.class);

        if (expected != null) {
            // invert expected to keep the same value
            when(responseChecker.apply(response)).thenReturn(false == expected);
            when(dneResponseChecker.apply(response)).thenReturn(false == expected);
        }

        resource.checkForResource(client, wrapMockListener(checkListener), logger, resourceBasePath, resourceName, resourceType, owner,
            ownerType, PublishableHttpResource.GET_EXISTS, PublishableHttpResource.GET_DOES_NOT_EXIST,
            responseChecker, dneResponseChecker);

        if (expected == Boolean.TRUE) {
            verify(responseChecker).apply(response);
            verifyZeroInteractions(dneResponseChecker);
        } else if (expected == Boolean.FALSE) {
            verifyZeroInteractions(responseChecker);
            verify(dneResponseChecker).apply(response);
        } else {
            verifyZeroInteractions(responseChecker, dneResponseChecker);
        }

        verifyCheckListener(expected);
    }

    private void assertDeleteResource(final RestStatus status, final boolean expected) {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("DELETE", endpoint, status);
        final Map<String, String> deleteParameters = deleteParameters(resource.getDefaultParameters());
        final Request request = new Request("DELETE", endpoint);
        addParameters(request, deleteParameters);

        whenPerformRequestAsyncWith(client, request, response);

        resource.deleteResource(client, wrapMockListener(checkListener), logger, resourceBasePath, resourceName, resourceType, owner,
            ownerType);

        verify(client).performRequestAsync(eq(request), any(ResponseListener.class));
        verify(response).getStatusLine();

        verify(logger).trace("deleting {} [{}] from the [{}] {}", resourceType, resourceName, owner, ownerType);

        if (expected) {
            verify(logger).debug("{} [{}] deleted from the [{}] {}", resourceType, resourceName, owner, ownerType);
            verifyCheckListener(true);
        } else {
            ArgumentCaptor<RuntimeException> e = ArgumentCaptor.forClass(RuntimeException.class);

            verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), e.capture());

            assertThat(e.getValue().getMessage(),
                       is("[" + resourceBasePath + "/" + resourceName + "] responded with [" + status.getStatus() + "]"));
            verifyCheckListener(null);
        }

        verifyNoMoreInteractions(client, response, logger, entity);
    }

}
