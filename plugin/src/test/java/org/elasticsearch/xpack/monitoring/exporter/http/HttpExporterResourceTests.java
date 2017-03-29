/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.ResolversRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils.DATA_INDEX;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HttpExporter} explicitly for its resource handling.
 */
public class HttpExporterResourceTests extends AbstractPublishableHttpResourceTestCase {

    /**
     * kibana, logstash, beats
     */
    private final int EXPECTED_TYPES = MonitoringTemplateUtils.NEW_DATA_TYPES.length;
    private final int EXPECTED_TEMPLATES = 6;

    private final RestClient client = mock(RestClient.class);
    private final Response versionResponse = mock(Response.class);

    private final MultiHttpResource resources =
            HttpExporter.createResources(new Exporter.Config("_http", "http", Settings.EMPTY), new ResolversRegistry(Settings.EMPTY));

    public void testInvalidVersionBlocks() throws IOException {
        final HttpEntity entity = new StringEntity("{\"version\":{\"number\":\"unknown\"}}", ContentType.APPLICATION_JSON);

        when(versionResponse.getEntity()).thenReturn(entity);
        when(client.performRequest(eq("GET"), eq("/"), anyMapOf(String.class, String.class))).thenReturn(versionResponse);

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become clean
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyNoMoreInteractions(client);
    }

    public void testTypeMappingCheckBlocksAfterSuccessfulVersion() throws IOException {
        final Exception exception = failureGetException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 0;

        whenValidVersionResponse();

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final boolean successfulFirst = randomBoolean();
            // -2 from one success + a necessary failure after it!
            final int extraPasses = Math.max(randomIntBetween(0, EXPECTED_TYPES - 2), 0);
            final int successful = randomIntBetween(0, extraPasses);
            final int unsuccessful = extraPasses - successful;

            final Response first = successfulFirst ? successfulGetTypeMappingResponse() : unsuccessfulGetTypeMappingResponse();

            final List<Response> otherResponses = getTypeMappingResponses(successful, unsuccessful);

            // last check fails implies that N - 2 publishes succeeded!
            when(client.performRequest(eq("GET"), startsWith("/" + DATA_INDEX + "/_mapping/"), anyMapOf(String.class, String.class)))
                    .thenReturn(first, otherResponses.toArray(new Response[otherResponses.size()]))
                    .thenThrow(exception);
            whenSuccessfulPutTypeMappings(otherResponses.size() + 1);

            expectedGets += 1 + successful + unsuccessful;
            expectedPuts = (successfulFirst ? 0 : 1) + unsuccessful;
        } else {
            when(client.performRequest(eq("GET"), startsWith("/" + DATA_INDEX + "/_mapping/"), anyMapOf(String.class, String.class)))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(expectedGets);
        verifyPutTypeMappings(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testTypeMappingPublishBlocksAfterSuccessfulVersion() throws IOException {
        final Exception exception = failurePutException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 1;

        whenValidVersionResponse();

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final Response firstSuccess = successfulPutResponse();
            // -2 from one success + a necessary failure after it!
            final int extraPasses = randomIntBetween(0, EXPECTED_TYPES - 2);
            final int successful = randomIntBetween(0, extraPasses);
            final int unsuccessful = extraPasses - successful;

            final List<Response> otherResponses = successfulPutResponses(unsuccessful);

            // first one passes for sure, so we need an extra "unsuccessful" GET
            whenGetTypeMappingResponse(successful, unsuccessful + 2);

            // previous publishes must have succeeded
            when(client.performRequest(eq("PUT"),
                                       startsWith("/" + DATA_INDEX + "/_mapping/"),
                                       anyMapOf(String.class, String.class),
                                       any(HttpEntity.class)))
                    .thenReturn(firstSuccess, otherResponses.toArray(new Response[otherResponses.size()]))
                    .thenThrow(exception);

            // GETs required for each PUT attempt (first is guaranteed "unsuccessful")
            expectedGets += successful + unsuccessful + 1;
            // unsuccessful are PUT attempts + the guaranteed successful PUT (first)
            expectedPuts += unsuccessful + 1;
        } else {
            // fail the check so that it has to attempt the PUT
            whenGetTypeMappingResponse(0, 1);

            when(client.performRequest(eq("PUT"),
                                       startsWith("/" + DATA_INDEX + "/_mapping/"),
                                       anyMapOf(String.class, String.class),
                                       any(HttpEntity.class)))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(expectedGets);
        verifyPutTypeMappings(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testTemplateCheckBlocksAfterSuccessfulTypeMapping() throws IOException {
        final int successfulGetTypeMappings = randomIntBetween(0, EXPECTED_TYPES);
        final int unsuccessfulGetTypeMappings = EXPECTED_TYPES - successfulGetTypeMappings;
        final Exception exception = failureGetException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 0;

        whenValidVersionResponse();
        whenGetTypeMappingResponse(successfulGetTypeMappings, unsuccessfulGetTypeMappings);
        whenSuccessfulPutTypeMappings(EXPECTED_TYPES);

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final boolean successfulFirst = randomBoolean();
            // -2 from one success + a necessary failure after it!
            final int extraPasses = randomIntBetween(0, EXPECTED_TEMPLATES - 2);
            final int successful = randomIntBetween(0, extraPasses);
            final int unsuccessful = extraPasses - successful;

            final Response first = successfulFirst ? successfulGetResponse() : unsuccessfulGetResponse();

            final List<Response> otherResponses = getResponses(successful, unsuccessful);

            // last check fails implies that N - 2 publishes succeeded!
            when(client.performRequest(eq("GET"), startsWith("/_template/"), anyMapOf(String.class, String.class)))
                    .thenReturn(first, otherResponses.toArray(new Response[otherResponses.size()]))
                    .thenThrow(exception);
            whenSuccessfulPutTemplates(otherResponses.size() + 1);

            expectedGets += 1 + successful + unsuccessful;
            expectedPuts = (successfulFirst ? 0 : 1) + unsuccessful;
        } else {
            when(client.performRequest(eq("GET"), startsWith("/_template/"), anyMapOf(String.class, String.class)))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(EXPECTED_TYPES);
        verifyPutTypeMappings(unsuccessfulGetTypeMappings);
        verifyGetTemplates(expectedGets);
        verifyPutTemplates(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testTemplatePublishBlocksAfterSuccessfulTypeMapping() throws IOException {
        final int successfulGetTypeMappings = randomIntBetween(0, EXPECTED_TYPES);
        final int unsuccessfulGetTypeMappings = EXPECTED_TYPES - successfulGetTypeMappings;
        final Exception exception = failurePutException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 1;

        whenValidVersionResponse();
        whenGetTypeMappingResponse(successfulGetTypeMappings, unsuccessfulGetTypeMappings);
        whenSuccessfulPutTypeMappings(EXPECTED_TYPES);

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final Response firstSuccess = successfulPutResponse();
            // -2 from one success + a necessary failure after it!
            final int extraPasses = randomIntBetween(0, EXPECTED_TEMPLATES - 2);
            final int successful = randomIntBetween(0, extraPasses);
            final int unsuccessful = extraPasses - successful;

            final List<Response> otherResponses = successfulPutResponses(unsuccessful);

            // first one passes for sure, so we need an extra "unsuccessful" GET
            whenGetTemplates(successful, unsuccessful + 2);

            // previous publishes must have succeeded
            when(client.performRequest(eq("PUT"), startsWith("/_template/"), anyMapOf(String.class, String.class), any(HttpEntity.class)))
                    .thenReturn(firstSuccess, otherResponses.toArray(new Response[otherResponses.size()]))
                    .thenThrow(exception);

            // GETs required for each PUT attempt (first is guaranteed "unsuccessful")
            expectedGets += successful + unsuccessful + 1;
            // unsuccessful are PUT attempts + the guaranteed successful PUT (first)
            expectedPuts += unsuccessful + 1;
        } else {
            // fail the check so that it has to attempt the PUT
            whenGetTemplates(0, 1);

            when(client.performRequest(eq("PUT"), startsWith("/_template/"), anyMapOf(String.class, String.class), any(HttpEntity.class)))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(EXPECTED_TYPES);
        verifyPutTypeMappings(unsuccessfulGetTypeMappings);
        verifyGetTemplates(expectedGets);
        verifyPutTemplates(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testPipelineCheckBlocksAfterSuccessfulTemplates() throws IOException {
        final int successfulGetTypeMappings = randomIntBetween(0, EXPECTED_TYPES);
        final int unsuccessfulGetTypeMappings = EXPECTED_TYPES - successfulGetTypeMappings;
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final Exception exception = failureGetException();

        whenValidVersionResponse();
        whenGetTypeMappingResponse(successfulGetTypeMappings, unsuccessfulGetTypeMappings);
        whenSuccessfulPutTypeMappings(EXPECTED_TYPES);
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(EXPECTED_TEMPLATES);

        // we only expect a single pipeline for now
        when(client.performRequest(eq("GET"), startsWith("/_ingest/pipeline/"), anyMapOf(String.class, String.class)))
                .thenThrow(exception);

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(EXPECTED_TYPES);
        verifyPutTypeMappings(unsuccessfulGetTypeMappings);
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(1);
        verifyPutPipelines(0);
        verifyNoMoreInteractions(client);
    }

    public void testPipelinePublishBlocksAfterSuccessfulTemplates() throws IOException {
        final int successfulGetTypeMappings = randomIntBetween(0, EXPECTED_TYPES);
        final int unsuccessfulGetTypeMappings = EXPECTED_TYPES - successfulGetTypeMappings;
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final Exception exception = failurePutException();

        whenValidVersionResponse();
        whenGetTypeMappingResponse(successfulGetTypeMappings, unsuccessfulGetTypeMappings);
        whenSuccessfulPutTypeMappings(EXPECTED_TYPES);
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(EXPECTED_TEMPLATES);
        // pipeline can't be there
        whenGetPipelines(0, 1);

        // we only expect a single pipeline for now
        when(client.performRequest(eq("PUT"),
                                   startsWith("/_ingest/pipeline/"),
                                   anyMapOf(String.class, String.class),
                                   any(HttpEntity.class)))
                .thenThrow(exception);

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(EXPECTED_TYPES);
        verifyPutTypeMappings(unsuccessfulGetTypeMappings);
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(1);
        verifyPutPipelines(1);
        verifyNoMoreInteractions(client);
    }

    public void testSuccessfulChecks() throws IOException {
        final int successfulGetTypeMappings = randomIntBetween(0, EXPECTED_TYPES);
        final int unsuccessfulGetTypeMappings = EXPECTED_TYPES - successfulGetTypeMappings;
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final int successfulGetPipelines = randomIntBetween(0, 1);
        final int unsuccessfulGetPipelines = 1 - successfulGetPipelines;

        whenValidVersionResponse();
        whenGetTypeMappingResponse(successfulGetTypeMappings, unsuccessfulGetTypeMappings);
        whenSuccessfulPutTypeMappings(EXPECTED_TYPES);
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(unsuccessfulGetTemplates);
        whenGetPipelines(successfulGetPipelines, unsuccessfulGetPipelines);
        whenSuccessfulPutPipelines(1);
        whenSuccessfulBackwardsCompatibilityAliases();

        assertTrue(resources.isDirty());

        // it should be able to proceed!
        assertTrue(resources.checkAndPublish(client));
        assertFalse(resources.isDirty());

        verifyVersionCheck();
        verifyGetTypeMappings(EXPECTED_TYPES);
        verifyPutTypeMappings(unsuccessfulGetTypeMappings);
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(1);
        verifyPutPipelines(unsuccessfulGetPipelines);
        verifyBackwardsCompatibilityAliases();
        verifyNoMoreInteractions(client);
    }

    private Exception failureGetException() {
        final ResponseException responseException = responseException("GET", "/_get_something", failedCheckStatus());

        return randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
    }

    private Exception failurePutException() {
        final ResponseException responseException = responseException("PUT", "/_put_something", failedPublishStatus());

        return randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
    }

    private Response successfulGetResponse() {
        return response("GET", "/_get_something", successfulCheckStatus());
    }

    private Response unsuccessfulGetResponse() {
        return response("GET", "/_get_something", notFoundCheckStatus());
    }

    private Response successfulGetTypeMappingResponse() {
        final Response response;

        if (randomBoolean()) {
            // it returned 200, but we also need it to contain _something_ in the JSON {...}
            final HttpEntity entity = new StringEntity("{\"" + DATA_INDEX + "\":{}}", ContentType.APPLICATION_JSON);

            response = successfulGetResponse();

            when(response.getEntity()).thenReturn(entity);
        } else {
            // simulates the index does not exist
            response = unsuccessfulGetResponse();
        }

        return response;
    }

    private Response unsuccessfulGetTypeMappingResponse() {
        // "unsuccessful" for type mappings is a response code 200, but the response is literally "{}"
        final Response response = successfulGetResponse();
        final HttpEntity entity = new StringEntity("{}", ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(entity);

        return response;
    }

    private List<Response> getTypeMappingResponses(final int successful, final int unsuccessful) {
        final List<Response> responses = new ArrayList<>(successful + unsuccessful);

        for (int i = 0; i < successful; ++i) {
            responses.add(successfulGetTypeMappingResponse());
        }

        for (int i = 0; i < unsuccessful; ++i) {
            responses.add(unsuccessfulGetTypeMappingResponse());
        }

        return responses;
    }

    private List<Response> getResponses(final int successful, final int unsuccessful) {
        final List<Response> responses = new ArrayList<>(successful + unsuccessful);

        for (int i = 0; i < successful; ++i) {
            responses.add(successfulGetResponse());
        }

        for (int i = 0; i < unsuccessful; ++i) {
            responses.add(unsuccessfulGetResponse());
        }

        return responses;
    }

    private Response successfulPutResponse() {
        final Response response = mock(Response.class);
        final StatusLine statusLine = mock(StatusLine.class);

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(randomFrom(RestStatus.OK, RestStatus.CREATED).getStatus());

        return response;
    }

    private List<Response> successfulPutResponses(final int successful) {
        final List<Response> responses = new ArrayList<>(successful);

        for (int i = 0; i < successful; ++i) {
            responses.add(successfulPutResponse());
        }

        return responses;
    }

    private void whenValidVersionResponse() throws IOException {
        final HttpEntity entity = new StringEntity("{\"version\":{\"number\":\"" + Version.CURRENT + "\"}}", ContentType.APPLICATION_JSON);

        when(versionResponse.getEntity()).thenReturn(entity);
        when(client.performRequest(eq("GET"), eq("/"), anyMapOf(String.class, String.class))).thenReturn(versionResponse);
    }

    private void whenGetTypeMappingResponse(final int successful, final int unsuccessful) throws IOException {
        final List<Response> gets = getTypeMappingResponses(successful, unsuccessful);

        if (gets.size() == 1) {
            when(client.performRequest(eq("GET"), startsWith("/" + DATA_INDEX + "/_mapping"), anyMapOf(String.class, String.class)))
                    .thenReturn(gets.get(0));
        } else {
            when(client.performRequest(eq("GET"), startsWith("/" + DATA_INDEX + "/_mapping"), anyMapOf(String.class, String.class)))
                    .thenReturn(gets.get(0), gets.subList(1, gets.size()).toArray(new Response[gets.size() - 1]));
        }
    }

    private void whenSuccessfulPutTypeMappings(final int successful) throws IOException {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(eq("PUT"),
                                       startsWith("/" + DATA_INDEX + "/_mapping"),
                                       anyMapOf(String.class, String.class), any(HttpEntity.class)))
                    .thenReturn(successfulPuts.get(0));
        } else if (successful > 1) {
            when(client.performRequest(eq("PUT"),
                                       startsWith("/" + DATA_INDEX + "/_mapping"),
                                       anyMapOf(String.class, String.class), any(HttpEntity.class)))
                    .thenReturn(successfulPuts.get(0), successfulPuts.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void whenGetTemplates(final int successful, final int unsuccessful) throws IOException {
        final List<Response> gets = getResponses(successful, unsuccessful);

        if (gets.size() == 1) {
            when(client.performRequest(eq("GET"), startsWith("/_template/"), anyMapOf(String.class, String.class)))
                    .thenReturn(gets.get(0));
        } else {
            when(client.performRequest(eq("GET"), startsWith("/_template/"), anyMapOf(String.class, String.class)))
                    .thenReturn(gets.get(0), gets.subList(1, gets.size()).toArray(new Response[gets.size() - 1]));
        }
    }

    private void whenSuccessfulPutTemplates(final int successful) throws IOException {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(eq("PUT"), startsWith("/_template/"), anyMapOf(String.class, String.class), any(HttpEntity.class)))
                    .thenReturn(successfulPuts.get(0));
        } else if (successful > 1) {
            when(client.performRequest(eq("PUT"), startsWith("/_template/"), anyMapOf(String.class, String.class), any(HttpEntity.class)))
                    .thenReturn(successfulPuts.get(0), successfulPuts.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void whenGetPipelines(final int successful, final int unsuccessful) throws IOException {
        final List<Response> gets = getResponses(successful, unsuccessful);

        if (gets.size() == 1) {
            when(client.performRequest(eq("GET"), startsWith("/_ingest/pipeline/"), anyMapOf(String.class, String.class)))
                    .thenReturn(gets.get(0));
        } else {
            when(client.performRequest(eq("GET"), startsWith("/_ingest/pipeline/"), anyMapOf(String.class, String.class)))
                    .thenReturn(gets.get(0), gets.subList(1, gets.size()).toArray(new Response[gets.size() - 1]));
        }
    }

    private void whenSuccessfulPutPipelines(final int successful) throws IOException {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(eq("PUT"),
                                       startsWith("/_ingest/pipeline/"),
                                       anyMapOf(String.class, String.class),
                                       any(HttpEntity.class)))
                    .thenReturn(successfulPuts.get(0));
        } else if (successful > 1) {
            when(client.performRequest(eq("PUT"),
                                       startsWith("/_ingest/pipeline/"),
                                       anyMapOf(String.class, String.class),
                                       any(HttpEntity.class)))
                    .thenReturn(successfulPuts.get(0), successfulPuts.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void whenSuccessfulBackwardsCompatibilityAliases() throws IOException {
        // Just return no indexes so we won't have to mock adding aliases

        final Response response = mock(Response.class);
        final StatusLine statusLine = mock(StatusLine.class);

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(RestStatus.OK.getStatus());
        when(response.getEntity()).thenReturn(new StringEntity("{}", ContentType.APPLICATION_JSON));

        when(client.performRequest(eq("GET"),
                    startsWith("/.marvel-es-1-*"),
                    anyMapOf(String.class, String.class)))
                .thenReturn(response);
    }

    private void verifyVersionCheck() throws IOException {
        verify(client).performRequest(eq("GET"), eq("/"), anyMapOf(String.class, String.class));
    }

    private void verifyGetTypeMappings(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(eq("GET"), startsWith("/" + DATA_INDEX + "/_mapping"), anyMapOf(String.class, String.class));
    }

    private void verifyPutTypeMappings(final int called) throws IOException {
        verify(client, times(called)).performRequest(eq("PUT"),                                  // method
                                                     startsWith("/" + DATA_INDEX + "/_mapping"), // endpoint
                                                     anyMapOf(String.class, String.class),       // parameters (e.g., timeout)
                                                     any(HttpEntity.class));                     // raw template
    }

    private void verifyGetTemplates(final int called) throws IOException {
        verify(client, times(called)).performRequest(eq("GET"), startsWith("/_template/"), anyMapOf(String.class, String.class));
    }

    private void verifyPutTemplates(final int called) throws IOException {
        verify(client, times(called)).performRequest(eq("PUT"),                            // method
                                                     startsWith("/_template/"),            // endpoint
                                                     anyMapOf(String.class, String.class), // parameters (e.g., timeout)
                                                     any(HttpEntity.class));               // raw template
    }

    private void verifyGetPipelines(final int called) throws IOException {
        verify(client, times(called)).performRequest(eq("GET"), startsWith("/_ingest/pipeline/"), anyMapOf(String.class, String.class));
    }

    private void verifyPutPipelines(final int called) throws IOException {
        verify(client, times(called)).performRequest(eq("PUT"),                            // method
                                                     startsWith("/_ingest/pipeline/"),     // endpoint
                                                     anyMapOf(String.class, String.class), // parameters (e.g., timeout)
                                                     any(HttpEntity.class));               // raw template
    }

    private void verifyBackwardsCompatibilityAliases() throws IOException {
        verify(client).performRequest(eq("GET"), startsWith("/.marvel-es-1-*"), anyMapOf(String.class, String.class));
    }
}
