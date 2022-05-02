/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpResource.ResourcePublishResult;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.monitoring.MonitoringTemplateRegistry.TEMPLATE_NAMES;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.whenPerformRequestAsyncWith;
import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.wrapMockListener;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HttpExporter} explicitly for its resource handling.
 */
public class HttpExporterResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final ClusterState state = mockClusterState(true);
    private final ClusterService clusterService = mockClusterService(state);
    private final MockLicenseState licenseState = mock(MockLicenseState.class);
    private final boolean remoteClusterHasWatcher = randomBoolean();
    private final boolean validLicense = randomBoolean();

    /**
     * kibana, logstash, beats and enterprise search
     */
    private final int EXPECTED_TEMPLATES = TEMPLATE_NAMES.length;
    private final int EXPECTED_WATCHES = ClusterAlertsUtil.WATCH_IDS.length;

    private final RestClient client = mock(RestClient.class);
    private final Response versionResponse = mock(Response.class);
    private final List<String> templateNames = new ArrayList<>(EXPECTED_TEMPLATES);
    private final List<String> watchNames = new ArrayList<>(EXPECTED_WATCHES);

    private final Settings exporterSettings = Settings.builder()
        .put("xpack.monitoring.exporters._http.cluster_alerts.management.enabled", true)
        .build();

    private final MultiHttpResource resources = HttpExporter.createResources(
        new Exporter.Config("_http", "http", exporterSettings, clusterService, licenseState)
    ).allResources;

    @Before
    public void setupResources() {
        templateNames.addAll(Arrays.stream(TEMPLATE_NAMES).collect(Collectors.toList()));
        watchNames.addAll(Arrays.stream(ClusterAlertsUtil.WATCH_IDS).map(id -> "my_cluster_uuid_" + id).collect(Collectors.toList()));

        assertThat("Not all templates are supplied", templateNames, hasSize(EXPECTED_TEMPLATES));
        assertThat("Not all watches are supplied", watchNames, hasSize(EXPECTED_WATCHES));
    }

    public void awaitCheckAndPublish(final Boolean expected) {
        awaitCheckAndPublish(resources, expected);
    }

    public void awaitCheckAndPublish(HttpResource resource, final Boolean expected) {
        awaitCheckAndPublish(resource, expected != null ? new ResourcePublishResult(expected) : null);
    }

    public void awaitCheckAndPublish(HttpResource resource, final ResourcePublishResult expected) {
        resource.checkAndPublish(client, wrapMockListener(publishListener));
        verifyPublishListener(expected);
    }

    public void testInvalidVersionBlocks() {
        final HttpEntity entity = new StringEntity("{\"version\":{\"number\":\"3.0.0\"}}", ContentType.APPLICATION_JSON);

        when(versionResponse.getEntity()).thenReturn(entity);
        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), is("/")), versionResponse);

        assertTrue(resources.isDirty());
        awaitCheckAndPublish(
            resources,
            new ResourcePublishResult(
                false,
                "version [3.0.0] < [7.0.0] and NOT supported for [xpack.monitoring.exporters._http]",
                HttpResource.State.DIRTY
            )
        );
        // ensure it didn't magically become clean
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyNoMoreInteractions(client);
    }

    public void testTemplateCheckBlocksAfterSuccessfulVersion() {
        final Exception exception = failureGetException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        final ResourcePublishResult expectedResult;

        whenValidVersionResponse();

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final boolean successfulFirst = randomBoolean();
            // -2 from one success + a necessary failure after it!
            final int extraPasses = randomIntBetween(0, EXPECTED_TEMPLATES - 2);
            final int successful = randomIntBetween(0, extraPasses);
            final int unsuccessful = extraPasses - successful;
            final String templateName = templateNames.get(0);

            final Response first;

            if (successfulFirst) {
                first = successfulGetResourceResponse("/_template/", templateName);
            } else {
                first = unsuccessfulGetResourceResponse("/_template/", templateName);
            }

            final List<Response> otherResponses = getTemplateResponses(1, successful, unsuccessful);

            // last check fails implies that N - 2 publishes succeeded!
            whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), startsWith("/_template/")), first, otherResponses, exception);

            // Since we return a "Not Ready" response on any templates that are not available (instead
            // of trying to publish them), we set the expected number of gets to be the first run of successful responses
            // plus the first failure
            if (successfulFirst) {
                expectedGets += successful + 1; // the string of successes, then the last failure.
            }

            if (successfulFirst && unsuccessful == 0) {
                // In this case, there will be only one failed response, and it'll be an exception
                expectedResult = null;
            } else {
                // The first bad response will be either a 404 or a template with an old version
                String missingTemplateName = TEMPLATE_NAMES[expectedGets - 1];
                expectedResult = new ResourcePublishResult(
                    false,
                    "waiting for remote monitoring cluster to install "
                        + "appropriate template ["
                        + missingTemplateName
                        + "] (version mismatch or missing)",
                    HttpResource.State.DIRTY
                );
            }
        } else {
            whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), startsWith("/_template/")), exception);
            expectedResult = null;
        }

        assertTrue(resources.isDirty());
        awaitCheckAndPublish(resources, expectedResult);
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(expectedGets);
        verifyNoMoreInteractions(client);
    }

    public void testWatcherCheckBlocksAfterSuccessfulTemplatePublish() {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final Exception exception = failureGetException();

        whenValidVersionResponse();
        whenGetTemplates(EXPECTED_TEMPLATES);

        // there's only one check
        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), is("/_xpack")), exception);

        assertTrue(resources.isDirty());
        awaitCheckAndPublish(null);
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyWatcherCheck();
        verifyNoMoreInteractions(client);
    }

    public void testWatchCheckBlocksAfterSuccessfulWatcherCheck() {
        final Exception exception = validLicense ? failureGetException() : failureDeleteException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 0;

        whenValidVersionResponse();
        whenGetTemplates(EXPECTED_TEMPLATES);
        whenWatcherCanBeUsed(validLicense);

        // failure in the middle of various watches being checked/published; suggests a node dropped
        if (firstSucceeds) {
            // getting _and_ putting watches
            if (validLicense) {
                final boolean successfulFirst = randomBoolean();
                // -2 from one success + a necessary failure after it!
                final int extraPasses = randomIntBetween(0, EXPECTED_WATCHES - 2);
                final int successful = randomIntBetween(0, extraPasses);
                final int unsuccessful = extraPasses - successful;

                final String watchId = watchNames.get(0);
                final Response first = successfulFirst ? successfulGetWatchResponse(watchId) : unsuccessfulGetWatchResponse(watchId);
                final List<Response> otherResponses = getWatcherResponses(1, successful, unsuccessful);

                // last check fails implies that N - 2 publishes succeeded!
                whenPerformRequestAsyncWith(
                    client,
                    new RequestMatcher(is("GET"), startsWith("/_watcher/watch/")),
                    first,
                    otherResponses,
                    exception
                );
                whenSuccessfulPutWatches(otherResponses.size() + 1);

                // +1 for the "first"
                expectedGets += 1 + successful + unsuccessful;
                expectedPuts = (successfulFirst ? 0 : 1) + unsuccessful;
            } else {
                // deleting watches
                // - 1 from necessary failure after it!
                final int successful = randomIntBetween(1, EXPECTED_WATCHES - 1);

                // there is no form of an unsuccessful delete; only success or error
                final List<Response> responses = successfulDeleteResponses(successful);

                whenPerformRequestAsyncWith(
                    client,
                    new RequestMatcher(is("DELETE"), startsWith("/_watcher/watch/")),
                    responses.get(0),
                    responses.subList(1, responses.size()),
                    exception
                );

                expectedGets += successful;
            }
        } else {
            final String method = validLicense ? "GET" : "DELETE";

            whenPerformRequestAsyncWith(client, new RequestMatcher(is(method), startsWith("/_watcher/watch/")), exception);
        }

        assertTrue(resources.isDirty());
        awaitCheckAndPublish(null);
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyWatcherCheck();
        if (validLicense) {
            verifyGetWatches(expectedGets);
            verifyPutWatches(expectedPuts);
        } else {
            verifyDeleteWatches(expectedGets);
        }
        verifyNoMoreInteractions(client);
    }

    public void testWatchPublishBlocksAfterSuccessfulWatcherCheck() {
        final Exception exception = failurePutException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 1;

        whenValidVersionResponse();
        whenGetTemplates(EXPECTED_TEMPLATES);
        // license needs to be valid, otherwise we'll do DELETEs, which are tested earlier
        whenWatcherCanBeUsed(true);

        // failure in the middle of various watches being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final Response firstSuccess = successfulPutResponse();
            // -2 from one success + a necessary failure after it!
            final int extraPasses = randomIntBetween(0, EXPECTED_WATCHES - 2);
            final int successful = randomIntBetween(0, extraPasses);
            final int unsuccessful = extraPasses - successful;

            final List<Response> otherResponses = successfulPutResponses(unsuccessful);

            // first one passes for sure, so we need an extra "unsuccessful" GET
            whenGetWatches(successful, unsuccessful + 2);

            // previous publishes must have succeeded
            whenPerformRequestAsyncWith(
                client,
                new RequestMatcher(is("PUT"), startsWith("/_watcher/watch/")),
                firstSuccess,
                otherResponses,
                exception
            );

            // GETs required for each PUT attempt (first is guaranteed "unsuccessful")
            expectedGets += successful + unsuccessful + 1;
            // unsuccessful are PUT attempts + the guaranteed successful PUT (first)
            expectedPuts += unsuccessful + 1;
        } else {
            // fail the check so that it has to attempt the PUT
            whenGetWatches(0, 1);

            whenPerformRequestAsyncWith(client, new RequestMatcher(is("PUT"), startsWith("/_watcher/watch/")), exception);
        }

        assertTrue(resources.isDirty());
        awaitCheckAndPublish(null);
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyWatcherCheck();
        verifyGetWatches(expectedGets);
        verifyPutWatches(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testDeployClusterAlerts() {
        final Exception exception = failurePutException();

        whenValidVersionResponse();
        whenGetTemplates(EXPECTED_TEMPLATES);
        // license needs to be valid, otherwise we'll do DELETEs, which are tested earlier
        whenWatcherCanBeUsed(true);

        // a number of watches are mocked as present
        final int existingWatches = randomIntBetween(0, EXPECTED_WATCHES);

        // For completeness's sake. GET/PUT watches wont be called by the resources.
        // Instead it tries to DELETE the watches ignoring them not existing.
        whenGetWatches(existingWatches, EXPECTED_WATCHES - existingWatches);
        whenPerformRequestAsyncWith(client, new RequestMatcher(is("PUT"), startsWith("/_watcher/watch/")), exception);
        whenPerformRequestAsyncWith(
            client,
            new RequestMatcher(is("DELETE"), startsWith("/_watcher/watch/")),
            successfulDeleteResponses(EXPECTED_WATCHES)
        );

        // Create resources that are configured to remove all watches
        Settings removalExporterSettings = Settings.builder()
            .put(exporterSettings)
            .put("xpack.monitoring.migration.decommission_alerts", true)
            .build();
        MultiHttpResource overrideResource = HttpExporter.createResources(
            new Exporter.Config("_http", "http", removalExporterSettings, clusterService, licenseState)
        ).allResources;

        assertTrue(overrideResource.isDirty());
        awaitCheckAndPublish(overrideResource, true);
        // Should proceed
        assertFalse(overrideResource.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyWatcherCheck();
        verifyGetWatches(0);
        verifyPutWatches(0);
        verifyDeleteWatches(EXPECTED_WATCHES);
        verifyNoMoreInteractions(client);

        assertWarnings(
            "[xpack.monitoring.migration.decommission_alerts] setting was deprecated in Elasticsearch and will be "
                + "removed in a future release.",
            "[xpack.monitoring.exporters._http.cluster_alerts.management.enabled] setting was deprecated in Elasticsearch and "
                + "will be removed in a future release."
        );
    }

    public void testSuccessfulChecksOnElectedMasterNode() {
        final int successfulGetWatches = randomIntBetween(0, EXPECTED_WATCHES);
        final int unsuccessfulGetWatches = EXPECTED_WATCHES - successfulGetWatches;

        whenValidVersionResponse();
        whenGetTemplates(EXPECTED_TEMPLATES);
        if (remoteClusterHasWatcher) {
            whenWatcherCanBeUsed(validLicense);
            if (validLicense) {
                whenGetWatches(successfulGetWatches, unsuccessfulGetWatches);
                whenSuccessfulPutWatches(unsuccessfulGetWatches);
            } else {
                whenSuccessfulDeleteWatches(EXPECTED_WATCHES);
            }
        } else {
            whenWatcherCannotBeUsed();
        }

        assertTrue(resources.isDirty());

        // it should be able to proceed!
        awaitCheckAndPublish(true);
        assertFalse(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyWatcherCheck();
        if (remoteClusterHasWatcher) {
            if (validLicense) {
                verifyGetWatches(EXPECTED_WATCHES);
                verifyPutWatches(unsuccessfulGetWatches);
            } else {
                verifyDeleteWatches(EXPECTED_WATCHES);
            }
        }
        verifyNoMoreInteractions(client);
    }

    /**
     * If the node is not the elected master node, then it should never check Watcher or send Watches (Cluster Alerts).
     */
    public void testSuccessfulChecksIfNotElectedMasterNode() {
        final ClusterState mockState = mockClusterState(false);
        final ClusterService mockClusterService = mockClusterService(mockState);

        final MultiHttpResource allResources = HttpExporter.createResources(
            new Exporter.Config("_http", "http", exporterSettings, mockClusterService, licenseState)
        ).allResources;

        whenValidVersionResponse();
        whenGetTemplates(EXPECTED_TEMPLATES);

        assertTrue(allResources.isDirty());

        // it should be able to proceed! (note: we are not using the instance "resources" here)
        allResources.checkAndPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(ResourcePublishResult.ready());
        assertFalse(allResources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyNoMoreInteractions(client);

        assertWarnings(
            "[xpack.monitoring.exporters._http.cluster_alerts.management.enabled] setting was deprecated in Elasticsearch "
                + "and will be removed in a future release."
        );
    }

    private Exception failureGetException() {
        final ResponseException responseException = responseException("GET", "/_get_something", failedCheckStatus());

        return randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
    }

    private Exception failurePutException() {
        final ResponseException responseException = responseException("PUT", "/_put_something", failedPublishStatus());

        return randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
    }

    private Exception failureDeleteException() {
        final ResponseException responseException = responseException("DELETE", "/_delete_something", failedCheckStatus());

        return randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
    }

    private Response unsuccessfulGetResponse() {
        return response("GET", "/_get_something", notFoundCheckStatus());
    }

    private Response successfulGetWatchResponse(final String watchId) {
        final HttpEntity goodEntity = entityForClusterAlert(true, ClusterAlertsUtil.LAST_UPDATED_VERSION);

        return response("GET", "/_watcher/watch/" + watchId, successfulCheckStatus(), goodEntity);
    }

    private Response unsuccessfulGetWatchResponse(final String watchId) {
        if (randomBoolean()) {
            final HttpEntity badEntity = entityForClusterAlert(false, ClusterAlertsUtil.LAST_UPDATED_VERSION);

            return response("GET", "/_watcher/watch/" + watchId, successfulCheckStatus(), badEntity);
        }

        return unsuccessfulGetResponse();
    }

    private Response successfulGetResourceResponse(final String resourcePath, final String resourceName) {
        final HttpEntity goodEntity = entityForResource(true, resourceName, MonitoringTemplateUtils.LAST_UPDATED_VERSION);
        if (logger.isTraceEnabled()) {
            try {
                logger.trace("Generated HTTP response for resource [{}]: [{}]", resourceName, EntityUtils.toString(goodEntity));
            } catch (IOException e) {
                logger.warn("Generated HTTP response for resource [" + resourceName + "] that cannot be deserialized!", e);
            }
        }
        return response("GET", resourcePath + resourceName, successfulCheckStatus(), goodEntity);
    }

    private Response unsuccessfulGetResourceResponse(final String resourcePath, final String resourceName) {
        if (randomBoolean()) {
            final HttpEntity badEntity = entityForResource(false, resourceName, MonitoringTemplateUtils.LAST_UPDATED_VERSION);
            if (logger.isTraceEnabled()) {
                try {
                    logger.trace("Generated bad HTTP entity for resource [{}]: [{}]", resourceName, EntityUtils.toString(badEntity));
                } catch (IOException e) {
                    logger.warn("Generated bad HTTP response for resource [" + resourceName + "] that cannot be deserialized!", e);
                }
            }

            return response("GET", resourcePath + resourceName, successfulCheckStatus(), badEntity);
        }

        logger.trace("Generated NOT FOUND response for resource [{}]", resourceName);
        return unsuccessfulGetResponse();
    }

    private List<Response> getResourceResponses(
        final String resourcePath,
        final List<String> resourceNames,
        final int skip,
        final int successful,
        final int unsuccessful
    ) {
        final List<Response> responses = new ArrayList<>(successful + unsuccessful);

        for (int i = 0; i < successful; ++i) {
            responses.add(successfulGetResourceResponse(resourcePath, resourceNames.get(i + skip)));
        }

        for (int i = 0; i < unsuccessful; ++i) {
            responses.add(unsuccessfulGetResourceResponse(resourcePath, resourceNames.get(i + successful + skip)));
        }

        return responses;
    }

    private List<Response> getTemplateResponses(final int skip, final int successful, final int unsuccessful) {
        return getResourceResponses("/_template/", templateNames, skip, successful, unsuccessful);
    }

    private List<Response> getWatcherResponses(final int skip, final int successful, final int unsuccessful) {
        final List<Response> responses = new ArrayList<>(successful + unsuccessful);

        for (int i = 0; i < successful; ++i) {
            responses.add(successfulGetWatchResponse(watchNames.get(i + skip)));
        }

        for (int i = 0; i < unsuccessful; ++i) {
            responses.add(unsuccessfulGetWatchResponse(watchNames.get(i + successful + skip)));
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

    private Response successfulDeleteResponse() {
        final RestStatus status = randomFrom(successfulCheckStatus(), notFoundCheckStatus());

        return response("DELETE", "/_delete_something", status);
    }

    private List<Response> successfulDeleteResponses(final int successful) {
        final List<Response> responses = new ArrayList<>(successful);

        for (int i = 0; i < successful; ++i) {
            responses.add(successfulDeleteResponse());
        }

        return responses;
    }

    private void whenValidVersionResponse() {
        final HttpEntity entity = new StringEntity("{\"version\":{\"number\":\"" + Version.CURRENT + "\"}}", ContentType.APPLICATION_JSON);

        when(versionResponse.getEntity()).thenReturn(entity);
        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), is("/")), versionResponse);
    }

    private void whenGetTemplates(final int successful) {
        final List<Response> gets = getTemplateResponses(0, successful, 0);

        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), startsWith("/_template/")), gets);
    }

    private void whenWatcherCanBeUsed(final boolean validLicenseToReturn) {
        final Metadata metadata = mock(Metadata.class);

        when(state.metadata()).thenReturn(metadata);
        when(metadata.clusterUUID()).thenReturn("the_clusters_uuid");

        when(licenseState.isAllowed(Monitoring.MONITORING_CLUSTER_ALERTS_FEATURE)).thenReturn(validLicenseToReturn);

        final HttpEntity entity = new StringEntity(
            "{\"features\":{\"watcher\":{\"enabled\":true,\"available\":true}}}",
            ContentType.APPLICATION_JSON
        );
        final Response successfulGet = response("GET", "_xpack", successfulCheckStatus(), entity);

        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), is("/_xpack")), successfulGet);
    }

    private void whenWatcherCannotBeUsed() {
        final Response response;
        if (randomBoolean()) {
            final HttpEntity entity = randomFrom(
                new StringEntity("""
                    {"features":{"watcher":{"enabled":false,"available":true}}}""", ContentType.APPLICATION_JSON),
                new StringEntity("""
                    {"features":{"watcher":{"enabled":true,"available":false}}}""", ContentType.APPLICATION_JSON),
                new StringEntity("{}", ContentType.APPLICATION_JSON)
            );

            response = response("GET", "_xpack", successfulCheckStatus(), entity);
        } else {
            response = response("GET", "_xpack", notFoundCheckStatus());
        }

        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), is("/_xpack")), response);
    }

    private void whenGetWatches(final int successful, final int unsuccessful) {
        final List<Response> gets = getWatcherResponses(0, successful, unsuccessful);

        whenPerformRequestAsyncWith(client, new RequestMatcher(is("GET"), startsWith("/_watcher/watch/")), gets);
    }

    private void whenSuccessfulPutWatches(final int successful) {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        whenPerformRequestAsyncWith(client, new RequestMatcher(is("PUT"), startsWith("/_watcher/watch/")), successfulPuts);
    }

    private void whenSuccessfulDeleteWatches(final int successful) {
        final List<Response> successfulDeletes = successfulDeleteResponses(successful);

        // empty is possible if they all exist
        whenPerformRequestAsyncWith(client, new RequestMatcher(is("DELETE"), startsWith("/_watcher/watch/")), successfulDeletes);
    }

    private void verifyVersionCheck() {
        verify(client).performRequestAsync(argThat(new RequestMatcher(is("GET"), is("/"))::matches), any(ResponseListener.class));
    }

    private void verifyGetTemplates(final int called) {
        verify(client, times(called)).performRequestAsync(
            argThat(new RequestMatcher(is("GET"), startsWith("/_template/"))::matches),
            any(ResponseListener.class)
        );
    }

    private void verifyPutTemplates(final int called) {
        verify(client, times(called)).performRequestAsync(
            argThat(new RequestMatcher(is("PUT"), startsWith("/_template/"))::matches),
            any(ResponseListener.class)
        );
    }

    private void verifyWatcherCheck() {
        verify(client).performRequestAsync(argThat(new RequestMatcher(is("GET"), is("/_xpack"))::matches), any(ResponseListener.class));
    }

    private void verifyDeleteWatches(final int called) {
        verify(client, times(called)).performRequestAsync(
            argThat(new RequestMatcher(is("DELETE"), startsWith("/_watcher/watch/"))::matches),
            any(ResponseListener.class)
        );
    }

    private void verifyGetWatches(final int called) {
        verify(client, times(called)).performRequestAsync(
            argThat(new RequestMatcher(is("GET"), startsWith("/_watcher/watch/"))::matches),
            any(ResponseListener.class)
        );
    }

    private void verifyPutWatches(final int called) {
        verify(client, times(called)).performRequestAsync(
            argThat(new RequestMatcher(is("PUT"), startsWith("/_watcher/watch/"))::matches),
            any(ResponseListener.class)
        );
    }

    private ClusterService mockClusterService(final ClusterState clusterState) {
        final ClusterService mockClusterService = mock(ClusterService.class);

        when(mockClusterService.state()).thenReturn(clusterState);

        return mockClusterService;
    }

    private ClusterState mockClusterState(final boolean electedMaster) {
        final ClusterState mockState = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(mockState.nodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(electedMaster);

        return mockState;
    }

    private static class RequestMatcher extends TypeSafeMatcher<Request> {
        private final Matcher<String> method;
        private final Matcher<String> endpoint;

        private RequestMatcher(Matcher<String> method, Matcher<String> endpoint) {
            this.method = method;
            this.endpoint = endpoint;
        }

        @Override
        protected boolean matchesSafely(Request item) {
            return method.matches(item.getMethod()) && endpoint.matches(item.getEndpoint());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("method is ").appendDescriptionOf(method);
            description.appendText(" and endpoint is ").appendDescriptionOf(endpoint);
        }
    }
}
