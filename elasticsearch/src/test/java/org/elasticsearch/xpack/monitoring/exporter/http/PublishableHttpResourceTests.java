/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse;

import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
        assertCheckForResource(successfulCheckStatus(), CheckResponse.EXISTS, "{} [{}] found on the [{}] {}");
    }

    public void testCheckForResourceDoesNotExist() throws IOException {
        assertCheckForResource(notFoundCheckStatus(), CheckResponse.DOES_NOT_EXIST, "{} [{}] does not exist on the [{}] {}");
    }

    public void testCheckForResourceUnexpectedResponse() throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus failedStatus = failedCheckStatus();
        final Response response = response("GET", endpoint, failedStatus);

        when(client.performRequest("GET", endpoint, resource.getParameters())).thenReturn(response);

        sometimesAssertSimpleCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, CheckResponse.ERROR, response);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequest("GET", endpoint, resource.getParameters());
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), any(ResponseException.class));

        verifyNoMoreInteractions(client, logger);
    }

    public void testCheckForResourceErrors() throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final RestStatus failedStatus = failedCheckStatus();
        final ResponseException responseException = responseException("GET", endpoint, failedStatus);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
        final Response response = e == responseException ? responseException.getResponse() : null;

        when(client.performRequest("GET", endpoint, resource.getParameters())).thenThrow(e);

        sometimesAssertSimpleCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, CheckResponse.ERROR, response);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequest("GET", endpoint, resource.getParameters());
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), eq(e));

        verifyNoMoreInteractions(client, logger);
    }

    public void testPutResourceTrue() throws IOException {
        assertPutResource(successfulPublishStatus(), true);
    }

    public void testPutResourceFalse() throws IOException {
        assertPutResource(failedPublishStatus(), false);
    }

    public void testPutResourceFalseWithException() throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Exception e = randomFrom(new IOException("expected"), new RuntimeException("expected"));

        when(client.performRequest("PUT", endpoint, resource.getParameters(), entity)).thenThrow(e);

        assertThat(resource.putResource(client, logger, resourceBasePath, resourceName, body, resourceType, owner, ownerType), is(false));

        verify(logger).trace("uploading {} [{}] to the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequest("PUT", endpoint, resource.getParameters(), entity);
        verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), eq(e));

        verifyNoMoreInteractions(client, logger);
    }

    public void testParameters() {
        assertParameters(resource);
    }

    public void testDoCheckAndPublishIgnoresPublishWhenCheckErrors() {
        final PublishableHttpResource resource =
                new MockHttpResource(owner, masterTimeout, PublishableHttpResource.NO_BODY_PARAMETERS, CheckResponse.ERROR, true);

        assertThat(resource.doCheckAndPublish(client), is(false));
    }

    public void testDoCheckAndPublish() {
        // not an error (the third state)
        final PublishableHttpResource.CheckResponse exists = randomBoolean() ? CheckResponse.EXISTS : CheckResponse.DOES_NOT_EXIST;
        final boolean publish = randomBoolean();

        final PublishableHttpResource resource =
                new MockHttpResource(owner, masterTimeout, PublishableHttpResource.NO_BODY_PARAMETERS, exists, publish);

        assertThat(resource.doCheckAndPublish(client), is(exists == CheckResponse.EXISTS || publish));
    }

    @SuppressLoggerChecks(reason = "mock usage")
    private void assertCheckForResource(final RestStatus status, final CheckResponse expected, final String debugLogMessage)
            throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("GET", endpoint, status);

        when(client.performRequest("GET", endpoint, resource.getParameters())).thenReturn(response);

        sometimesAssertSimpleCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, expected, response);

        verify(logger).trace("checking if {} [{}] exists on the [{}] {}", resourceType, resourceName, owner, ownerType);
        verify(client).performRequest("GET", endpoint, resource.getParameters());

        if (expected == CheckResponse.EXISTS) {
            verify(response).getStatusLine();
        } else {
            // 3 times because it also is used in the exception message
            verify(response, times(3)).getStatusLine();
            verify(response, times(2)).getRequestLine();
            verify(response).getHost();
            verify(response).getEntity();
        }

        verify(logger).debug(debugLogMessage, resourceType, resourceName, owner, ownerType);

        verifyNoMoreInteractions(client, response, logger);
    }

    private void assertPutResource(final RestStatus status, final boolean expected) throws IOException {
        final String endpoint = concatenateEndpoint(resourceBasePath, resourceName);
        final Response response = response("PUT", endpoint, status);

        when(client.performRequest("PUT", endpoint, resource.getParameters(), entity)).thenReturn(response);

        assertThat(resource.putResource(client, logger, resourceBasePath, resourceName, body, resourceType, owner, ownerType),
                   is(expected));

        verify(client).performRequest("PUT", endpoint, resource.getParameters(), entity);
        verify(response).getStatusLine();

        verify(logger).trace("uploading {} [{}] to the [{}] {}", resourceType, resourceName, owner, ownerType);

        if (expected) {
            verify(logger).debug("{} [{}] uploaded to the [{}] {}", resourceType, resourceName, owner, ownerType);
        } else {
            ArgumentCaptor<RuntimeException> e = ArgumentCaptor.forClass(RuntimeException.class);

            verify(logger).error(any(org.apache.logging.log4j.util.Supplier.class), e.capture());

            assertThat(e.getValue().getMessage(),
                       is("[" + resourceBasePath + "/" + resourceName + "] responded with [" + status.getStatus() + "]"));
        }

        verifyNoMoreInteractions(client, response, logger, entity);
    }

    private void sometimesAssertSimpleCheckForResource(final RestClient client, final Logger logger,
                                                       final String resourceBasePath,
                                                       final String resourceName, final String resourceType,
                                                       final CheckResponse expected, final Response response) {
        // sometimes use the simple check
        if (randomBoolean()) {
            assertThat(resource.simpleCheckForResource(client, logger, resourceBasePath, resourceName, resourceType, owner, ownerType),
                    is(expected));
        } else {
            final Tuple<CheckResponse, Response> responseTuple =
                    resource.checkForResource(client, logger, resourceBasePath, resourceName, resourceType, owner, ownerType);

            assertThat(responseTuple.v1(), is(expected));
            assertThat(responseTuple.v2(), is(response));
        }
    }

}
