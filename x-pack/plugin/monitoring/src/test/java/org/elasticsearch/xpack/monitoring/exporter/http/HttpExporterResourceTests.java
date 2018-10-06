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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.OLD_TEMPLATE_IDS;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.PIPELINE_IDS;
import static org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_IDS;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse.DOES_NOT_EXIST;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse.EXISTS;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.argThat;
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
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final boolean remoteClusterHasWatcher = randomBoolean();
    private final boolean validLicense = randomBoolean();
    private final boolean createOldTemplates = randomBoolean();

    /**
     * kibana, logstash, and beats
     */
    private final int EXPECTED_TEMPLATES = TEMPLATE_IDS.length + (createOldTemplates ? OLD_TEMPLATE_IDS.length : 0);
    private final int EXPECTED_PIPELINES = PIPELINE_IDS.length;
    private final int EXPECTED_WATCHES = ClusterAlertsUtil.WATCH_IDS.length;

    private final RestClient client = mock(RestClient.class);
    private final Response versionResponse = mock(Response.class);
    private final List<String> templateNames = new ArrayList<>(EXPECTED_TEMPLATES);
    private final List<String> pipelineNames = new ArrayList<>(EXPECTED_PIPELINES);
    private final List<String> watchNames = new ArrayList<>(EXPECTED_WATCHES);

    private final Settings exporterSettings = Settings.builder()
            .put("xpack.monitoring.exporters._http.index.template.create_legacy_templates", createOldTemplates)
            .build();

    private final MultiHttpResource resources =
            HttpExporter.createResources(
                    new Exporter.Config("_http", "http", exporterSettings, clusterService, licenseState));

    @Before
    public void setupResources() {
        templateNames.addAll(Arrays.stream(TEMPLATE_IDS).map(MonitoringTemplateUtils::templateName).collect(Collectors.toList()));

        if (createOldTemplates) {
            templateNames.addAll(
                    Arrays.stream(OLD_TEMPLATE_IDS).map(MonitoringTemplateUtils::oldTemplateName).collect(Collectors.toList()));
        }

        pipelineNames.addAll(Arrays.stream(PIPELINE_IDS).map(MonitoringTemplateUtils::pipelineName).collect(Collectors.toList()));
        watchNames.addAll(Arrays.stream(ClusterAlertsUtil.WATCH_IDS).map(id -> "my_cluster_uuid_" + id).collect(Collectors.toList()));

        assertThat("Not all templates are supplied", templateNames, hasSize(EXPECTED_TEMPLATES));
        assertThat("Not all pipelines are supplied", pipelineNames, hasSize(EXPECTED_PIPELINES));
        assertThat("Not all watches are supplied", watchNames, hasSize(EXPECTED_WATCHES));
    }

    public void testInvalidVersionBlocks() throws IOException {
        final HttpEntity entity = new StringEntity("{\"version\":{\"number\":\"unknown\"}}", ContentType.APPLICATION_JSON);

        when(versionResponse.getEntity()).thenReturn(entity);
        when(client.performRequest(argThat(new RequestMatcher(is("GET"), is("/")))))
            .thenReturn(versionResponse);

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become clean
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyNoMoreInteractions(client);
    }

    public void testTemplateCheckBlocksAfterSuccessfulVersion() throws IOException {
        final Exception exception = failureGetException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 0;

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
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_template/")))))
                .thenReturn(first, otherResponses.toArray(new Response[otherResponses.size()]))
                .thenThrow(exception);
            whenSuccessfulPutTemplates(otherResponses.size() + 1);

            expectedGets += 1 + successful + unsuccessful;
            expectedPuts = (successfulFirst ? 0 : 1) + unsuccessful;
        } else {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_template/")))))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(expectedGets);
        verifyPutTemplates(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testTemplatePublishBlocksAfterSuccessfulVersion() throws IOException {
        final Exception exception = failurePutException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 1;

        whenValidVersionResponse();

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
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_template/")))))
                    .thenReturn(firstSuccess, otherResponses.toArray(new Response[otherResponses.size()]))
                    .thenThrow(exception);

            // GETs required for each PUT attempt (first is guaranteed "unsuccessful")
            expectedGets += successful + unsuccessful + 1;
            // unsuccessful are PUT attempts + the guaranteed successful PUT (first)
            expectedPuts += unsuccessful + 1;
        } else {
            // fail the check so that it has to attempt the PUT
            whenGetTemplates(0, 1);

            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_template/")))))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(expectedGets);
        verifyPutTemplates(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testPipelineCheckBlocksAfterSuccessfulTemplates() throws IOException {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final Exception exception = failureGetException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 0;

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(EXPECTED_TEMPLATES);

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final boolean successfulFirst = randomBoolean();
            final String pipelineName = pipelineNames.get(0);

            final Response first;

            if (successfulFirst) {
                first = successfulGetResourceResponse("/_ingest/pipeline/", pipelineName);
            } else {
                first = unsuccessfulGetResourceResponse("/_ingest/pipeline/", pipelineName);
            }

            // last check fails
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_ingest/pipeline/")))))
                    .thenReturn(first)
                    .thenThrow(exception);
            if (successfulFirst == false) {
                whenSuccessfulPutPipelines(1);
            }

            expectedGets = EXPECTED_PIPELINES;
            expectedPuts = successfulFirst ? 0 : 1;
        } else {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_ingest/pipeline/")))))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(expectedGets);
        verifyPutPipelines(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testPipelinePublishBlocksAfterSuccessfulTemplates() throws IOException {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final Exception exception = failurePutException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 1;

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(EXPECTED_TEMPLATES);

        // failure in the middle of various templates being checked/published; suggests a node dropped
        if (firstSucceeds) {
            final Response firstSuccess = successfulPutResponse();

            // We only have two pipelines for now, so the both GETs need to be "unsuccessful" for until we have a third
            whenGetPipelines(0, 2);

            // previous publishes must have succeeded
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_ingest/pipeline/")))))
                    .thenReturn(firstSuccess)
                    .thenThrow(exception);

            // GETs required for each PUT attempt (first is guaranteed "unsuccessful")
            expectedGets += 1;
            // unsuccessful are PUT attempts
            expectedPuts += 1;
        } else {
            // fail the check so that it has to attempt the PUT
            whenGetPipelines(0, 1);

            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_ingest/pipeline/")))))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(expectedGets);
        verifyPutPipelines(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testWatcherCheckBlocksAfterSuccessfulPipelines() throws IOException {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final int successfulGetPipelines = randomIntBetween(0, EXPECTED_PIPELINES);
        final int unsuccessfulGetPipelines = EXPECTED_PIPELINES - successfulGetPipelines;
        final Exception exception = failureGetException();

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(unsuccessfulGetTemplates);
        whenGetPipelines(successfulGetPipelines, unsuccessfulGetPipelines);
        whenSuccessfulPutPipelines(unsuccessfulGetPipelines);

        // there's only one check
        when(client.performRequest(argThat(new RequestMatcher(is("GET"), is("/_xpack")))))
                .thenThrow(exception);

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(EXPECTED_PIPELINES);
        verifyPutPipelines(unsuccessfulGetPipelines);
        verifyWatcherCheck();
        verifyNoMoreInteractions(client);
    }

    public void testWatchCheckBlocksAfterSuccessfulWatcherCheck() throws IOException {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final int successfulGetPipelines = randomIntBetween(0, EXPECTED_PIPELINES);
        final int unsuccessfulGetPipelines = EXPECTED_PIPELINES - successfulGetPipelines;
        final Exception exception = validLicense ? failureGetException() : failureDeleteException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 0;

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(unsuccessfulGetTemplates);
        whenGetPipelines(successfulGetPipelines, unsuccessfulGetPipelines);
        whenSuccessfulPutPipelines(unsuccessfulGetPipelines);
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
                when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_xpack/watcher/watch/")))))
                        .thenReturn(first, otherResponses.toArray(new Response[otherResponses.size()]))
                        .thenThrow(exception);
                whenSuccessfulPutWatches(otherResponses.size() + 1);

                // +1 for the "first"
                expectedGets += 1 + successful + unsuccessful;
                expectedPuts = (successfulFirst ? 0 : 1) + unsuccessful;
            // deleting watches
            } else {
                // - 1 from necessary failure after it!
                final int successful = randomIntBetween(1, EXPECTED_WATCHES - 1);

                // there is no form of an unsuccessful delete; only success or error
                final List<Response> responses = successfulDeleteResponses(successful);

                when(client.performRequest(argThat(new RequestMatcher(is("DELETE"), startsWith("/_xpack/watcher/watch/")))))
                        .thenReturn(responses.get(0), responses.subList(1, successful).toArray(new Response[successful - 1]))
                        .thenThrow(exception);

                expectedGets += successful;
            }
        } else {
            final String method = validLicense ? "GET" : "DELETE";

            when(client.performRequest(argThat(new RequestMatcher(is(method), startsWith("/_xpack/watcher/watch/")))))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(EXPECTED_PIPELINES);
        verifyPutPipelines(unsuccessfulGetPipelines);
        verifyWatcherCheck();
        if (validLicense) {
            verifyGetWatches(expectedGets);
            verifyPutWatches(expectedPuts);
        } else {
            verifyDeleteWatches(expectedGets);
        }
        verifyNoMoreInteractions(client);
    }

    public void testWatchPublishBlocksAfterSuccessfulWatcherCheck() throws IOException {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final int successfulGetPipelines = randomIntBetween(0, EXPECTED_PIPELINES);
        final int unsuccessfulGetPipelines = EXPECTED_PIPELINES - successfulGetPipelines;
        final Exception exception = failurePutException();
        final boolean firstSucceeds = randomBoolean();
        int expectedGets = 1;
        int expectedPuts = 1;

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(unsuccessfulGetTemplates);
        whenGetPipelines(successfulGetPipelines, unsuccessfulGetPipelines);
        whenSuccessfulPutPipelines(unsuccessfulGetPipelines);
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
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(firstSuccess, otherResponses.toArray(new Response[otherResponses.size()]))
                    .thenThrow(exception);

            // GETs required for each PUT attempt (first is guaranteed "unsuccessful")
            expectedGets += successful + unsuccessful + 1;
            // unsuccessful are PUT attempts + the guaranteed successful PUT (first)
            expectedPuts += unsuccessful + 1;
        } else {
            // fail the check so that it has to attempt the PUT
            whenGetWatches(0, 1);

            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_xpack/watcher/watch/")))))
                    .thenThrow(exception);
        }

        assertTrue(resources.isDirty());
        assertFalse(resources.checkAndPublish(client));
        // ensure it didn't magically become not-dirty
        assertTrue(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(EXPECTED_PIPELINES);
        verifyPutPipelines(unsuccessfulGetPipelines);
        verifyWatcherCheck();
        verifyGetWatches(expectedGets);
        verifyPutWatches(expectedPuts);
        verifyNoMoreInteractions(client);
    }

    public void testSuccessfulChecksOnElectedMasterNode() throws IOException {
        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final int successfulGetPipelines = randomIntBetween(0, EXPECTED_PIPELINES);
        final int unsuccessfulGetPipelines = EXPECTED_PIPELINES - successfulGetPipelines;
        final int successfulGetWatches = randomIntBetween(0, EXPECTED_WATCHES);
        final int unsuccessfulGetWatches = EXPECTED_WATCHES - successfulGetWatches;

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(unsuccessfulGetTemplates);
        whenGetPipelines(successfulGetPipelines, unsuccessfulGetPipelines);
        whenSuccessfulPutPipelines(unsuccessfulGetPipelines);
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
        assertTrue(resources.checkAndPublish(client));
        assertFalse(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(EXPECTED_PIPELINES);
        verifyPutPipelines(unsuccessfulGetPipelines);
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
    public void testSuccessfulChecksIfNotElectedMasterNode() throws IOException {
        final ClusterState state = mockClusterState(false);
        final ClusterService clusterService = mockClusterService(state);

        final MultiHttpResource resources =
                HttpExporter.createResources(
                        new Exporter.Config("_http", "http", exporterSettings, clusterService, licenseState));

        final int successfulGetTemplates = randomIntBetween(0, EXPECTED_TEMPLATES);
        final int unsuccessfulGetTemplates = EXPECTED_TEMPLATES - successfulGetTemplates;
        final int successfulGetPipelines = randomIntBetween(0, 1);
        final int unsuccessfulGetPipelines = EXPECTED_PIPELINES - successfulGetPipelines;

        whenValidVersionResponse();
        whenGetTemplates(successfulGetTemplates, unsuccessfulGetTemplates);
        whenSuccessfulPutTemplates(unsuccessfulGetTemplates);
        whenGetPipelines(successfulGetPipelines, unsuccessfulGetPipelines);
        whenSuccessfulPutPipelines(1);

        assertTrue(resources.isDirty());

        // it should be able to proceed!
        assertTrue(resources.checkAndPublish(client));
        assertFalse(resources.isDirty());

        verifyVersionCheck();
        verifyGetTemplates(EXPECTED_TEMPLATES);
        verifyPutTemplates(unsuccessfulGetTemplates);
        verifyGetPipelines(EXPECTED_PIPELINES);
        verifyPutPipelines(unsuccessfulGetPipelines);
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

    private Exception failureDeleteException() {
        final ResponseException responseException = responseException("DELETE", "/_delete_something", failedCheckStatus());

        return randomFrom(new IOException("expected"), new RuntimeException("expected"), responseException);
    }

    private Response unsuccessfulGetResponse() {
        return response("GET", "/_get_something", notFoundCheckStatus());
    }

    private Response successfulGetWatchResponse(final String watchId) {
        final HttpEntity goodEntity = entityForClusterAlert(EXISTS, ClusterAlertsUtil.LAST_UPDATED_VERSION);

        return response("GET", "/_xpack/watcher/watch/" + watchId, successfulCheckStatus(), goodEntity);
    }
    private Response unsuccessfulGetWatchResponse(final String watchId) {
        if (randomBoolean()) {
            final HttpEntity badEntity = entityForClusterAlert(DOES_NOT_EXIST, ClusterAlertsUtil.LAST_UPDATED_VERSION);

            return response("GET", "/_xpack/watcher/watch/" + watchId, successfulCheckStatus(), badEntity);
        }

        return unsuccessfulGetResponse();
    }

    private Response successfulGetResourceResponse(final String resourcePath, final String resourceName) {
        final HttpEntity goodEntity = entityForResource(EXISTS, resourceName, MonitoringTemplateUtils.LAST_UPDATED_VERSION);

        return response("GET", resourcePath + resourceName, successfulCheckStatus(), goodEntity);
    }

    private Response unsuccessfulGetResourceResponse(final String resourcePath, final String resourceName) {
        if (randomBoolean()) {
            final HttpEntity badEntity = entityForResource(DOES_NOT_EXIST, resourceName, MonitoringTemplateUtils.LAST_UPDATED_VERSION);

            return response("GET", resourcePath + resourceName, successfulCheckStatus(), badEntity);
        }

        return unsuccessfulGetResponse();
    }

    private List<Response> getResourceResponses(final String resourcePath, final List<String> resourceNames,
                                                final int skip, final int successful, final int unsuccessful) {
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

    private List<Response> getPipelineResponses(final int skip, final int successful, final int unsuccessful) {
        return getResourceResponses("/_ingest/pipeline/", pipelineNames, skip, successful, unsuccessful);
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

    private void whenValidVersionResponse() throws IOException {
        final HttpEntity entity = new StringEntity("{\"version\":{\"number\":\"" + Version.CURRENT + "\"}}", ContentType.APPLICATION_JSON);

        when(versionResponse.getEntity()).thenReturn(entity);
        when(client.performRequest(argThat(new RequestMatcher(is("GET"), is("/")))))
                .thenReturn(versionResponse);
    }

    private void whenGetTemplates(final int successful, final int unsuccessful) throws IOException {
        final List<Response> gets = getTemplateResponses(0, successful, unsuccessful);

        if (gets.size() == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_template/")))))
                    .thenReturn(gets.get(0));
        } else {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_template/")))))
                    .thenReturn(gets.get(0), gets.subList(1, gets.size()).toArray(new Response[gets.size() - 1]));
        }
    }

    private void whenSuccessfulPutTemplates(final int successful) throws IOException {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_template/")))))
                    .thenReturn(successfulPuts.get(0));
        } else if (successful > 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_template/")))))
                    .thenReturn(successfulPuts.get(0), successfulPuts.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void whenGetPipelines(final int successful, final int unsuccessful) throws IOException {
        final List<Response> gets = getPipelineResponses(0, successful, unsuccessful);

        if (gets.size() == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_ingest/pipeline/")))))
                    .thenReturn(gets.get(0));
        } else {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_ingest/pipeline/")))))
                    .thenReturn(gets.get(0), gets.subList(1, gets.size()).toArray(new Response[gets.size() - 1]));
        }
    }

    private void whenSuccessfulPutPipelines(final int successful) throws IOException {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_ingest/pipeline/")))))
                    .thenReturn(successfulPuts.get(0));
        } else if (successful > 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_ingest/pipeline/")))))
                    .thenReturn(successfulPuts.get(0), successfulPuts.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void whenWatcherCanBeUsed(final boolean validLicense) throws IOException {
        final MetaData metaData = mock(MetaData.class);

        when(state.metaData()).thenReturn(metaData);
        when(metaData.clusterUUID()).thenReturn("the_clusters_uuid");

        when(licenseState.isMonitoringClusterAlertsAllowed()).thenReturn(validLicense);

        final HttpEntity entity =
                new StringEntity("{\"features\":{\"watcher\":{\"enabled\":true,\"available\":true}}}", ContentType.APPLICATION_JSON);
        final Response successfulGet = response("GET", "_xpack", successfulCheckStatus(), entity);

        // empty is possible if they all exist
        when(client.performRequest(argThat(new RequestMatcher(is("GET"), is("/_xpack")))))
                .thenReturn(successfulGet);
    }

    private void whenWatcherCannotBeUsed() throws IOException {
        final Response response;
        if (randomBoolean()) {
            final HttpEntity entity = randomFrom(
                new StringEntity("{\"features\":{\"watcher\":{\"enabled\":false,\"available\":true}}}", ContentType.APPLICATION_JSON),
                new StringEntity("{\"features\":{\"watcher\":{\"enabled\":true,\"available\":false}}}", ContentType.APPLICATION_JSON),
                new StringEntity("{}", ContentType.APPLICATION_JSON)
            );

            response = response("GET", "_xpack", successfulCheckStatus(), entity);
        } else {
            response = response("GET", "_xpack", notFoundCheckStatus());
        }

        // empty is possible if they all exist
        when(client.performRequest(argThat(new RequestMatcher(is("GET"), is("/_xpack")))))
                .thenReturn(response);
    }

    private void whenGetWatches(final int successful, final int unsuccessful) throws IOException {
        final List<Response> gets = getWatcherResponses(0, successful, unsuccessful);

        if (gets.size() == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(gets.get(0));
        } else {
            when(client.performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(gets.get(0), gets.subList(1, gets.size()).toArray(new Response[gets.size() - 1]));
        }
    }

    private void whenSuccessfulPutWatches(final int successful) throws IOException {
        final List<Response> successfulPuts = successfulPutResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(successfulPuts.get(0));
        } else if (successful > 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(successfulPuts.get(0), successfulPuts.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void whenSuccessfulDeleteWatches(final int successful) throws IOException {
        final List<Response> successfulDeletes = successfulDeleteResponses(successful);

        // empty is possible if they all exist
        if (successful == 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("DELETE"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(successfulDeletes.get(0));
        } else if (successful > 1) {
            when(client.performRequest(argThat(new RequestMatcher(is("DELETE"), startsWith("/_xpack/watcher/watch/")))))
                    .thenReturn(successfulDeletes.get(0), successfulDeletes.subList(1, successful).toArray(new Response[successful - 1]));
        }
    }

    private void verifyVersionCheck() throws IOException {
        verify(client).performRequest(argThat(new RequestMatcher(is("GET"), is("/"))));
    }

    private void verifyGetTemplates(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_template/"))));
    }

    private void verifyPutTemplates(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_template/"))));
    }

    private void verifyGetPipelines(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_ingest/pipeline/"))));
    }

    private void verifyPutPipelines(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_ingest/pipeline/"))));
    }

    private void verifyWatcherCheck() throws IOException {
        verify(client).performRequest(argThat(new RequestMatcher(is("GET"), is("/_xpack"))));
    }

    private void verifyDeleteWatches(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("DELETE"), startsWith("/_xpack/watcher/watch/"))));
    }

    private void verifyGetWatches(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("GET"), startsWith("/_xpack/watcher/watch/"))));
    }

    private void verifyPutWatches(final int called) throws IOException {
        verify(client, times(called))
                .performRequest(argThat(new RequestMatcher(is("PUT"), startsWith("/_xpack/watcher/watch/"))));
    }

    private ClusterService mockClusterService(final ClusterState state) {
        final ClusterService clusterService = mock(ClusterService.class);

        when(clusterService.state()).thenReturn(state);

        return clusterService;
    }

    private ClusterState mockClusterState(final boolean electedMaster) {
        final ClusterState state = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(state.nodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(electedMaster);

        return state;
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
