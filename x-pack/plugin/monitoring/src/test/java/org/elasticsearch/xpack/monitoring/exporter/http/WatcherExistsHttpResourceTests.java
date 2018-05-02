/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.Collections;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.CheckResponse;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.GET_EXISTS;
import static org.elasticsearch.xpack.monitoring.exporter.http.WatcherExistsHttpResource.XPACK_DOES_NOT_EXIST;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link WatcherExistsHttpResource}.
 */
public class WatcherExistsHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final MultiHttpResource watches = mock(MultiHttpResource.class);

    private final WatcherExistsHttpResource resource = new WatcherExistsHttpResource(owner, clusterService, watches);
    private final Map<String, String> expectedParameters = getParameters(resource.getParameters(), GET_EXISTS, XPACK_DOES_NOT_EXIST);

    public void testDoCheckIgnoresClientWhenNotElectedMaster() {
        whenNotElectedMaster();

        assertThat(resource.doCheck(client), is(CheckResponse.EXISTS));

        verifyZeroInteractions(client);
    }

    public void testDoCheckExistsFor404() throws IOException {
        whenElectedMaster();

        // /_xpack returning a 404 means ES didn't handle the request properly and X-Pack doesn't exist
        doCheckWithStatusCode(resource, "", "_xpack", notFoundCheckStatus(),
                              GET_EXISTS, XPACK_DOES_NOT_EXIST, CheckResponse.EXISTS);
    }

    public void testDoCheckExistsFor400() throws IOException {
        whenElectedMaster();

        // /_xpack returning a 400 means X-Pack does not exist
        doCheckWithStatusCode(resource, "", "_xpack", RestStatus.BAD_REQUEST,
                              GET_EXISTS, XPACK_DOES_NOT_EXIST, CheckResponse.EXISTS);
    }

    public void testDoCheckExistsAsElectedMaster() throws IOException {
        whenElectedMaster();

        final String[] noWatcher = {
            "{}",
            "{\"features\":{\"watcher\":{\"available\":true,\"enabled\":false}}}",
            "{\"features\":{\"watcher\":{\"available\":false,\"enabled\":true}}}",
            "{\"features\":{\"watcher\":{\"available\":true}}}",
            "{\"features\":{\"watcher\":{\"enabled\":true}}}"
        };

        final String endpoint = "/_xpack";
        // success only implies that it responded; it also needs to be available and enabled
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = new StringEntity(randomFrom(noWatcher), ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(responseEntity);

        // returning EXISTS implies that we CANNOT use Watcher to avoid running the publish phase
        doCheckWithStatusCode(resource, expectedParameters, endpoint, CheckResponse.EXISTS, response);

        verify(response).getEntity();
    }

    public void testDoCheckDoesNotExist() throws IOException {
        whenElectedMaster();

        final String[] hasWatcher = {
            "{\"features\":{\"watcher\":{\"available\":true,\"enabled\":true}}}",
            "{\"features\":{\"watcher\":{\"enabled\":true,\"available\":true}}}"
        };

        final String endpoint = "/_xpack";
        // success only implies that it responded; it also needs to be available and enabled
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = new StringEntity(randomFrom(hasWatcher), ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(responseEntity);

        // returning DOES_NOT_EXIST implies that we CAN use Watcher and need to run the publish phase
        doCheckWithStatusCode(resource, expectedParameters, endpoint, CheckResponse.DOES_NOT_EXIST, response);

        verify(response).getEntity();
    }

    public void testDoCheckErrorWithDataException() throws IOException {
        whenElectedMaster();

        final String[] errorWatcher = {
            "{\"features\":{}}", // missing watcher object 'string'
            "{\"watcher\":{\"enabled\":true,\"available\":true}}", // missing features outer object
            "{{}" // extra {
        };

        final String endpoint = "/_xpack";
        // success only implies that it responded; it also needs to be available and enabled
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = new StringEntity(randomFrom(errorWatcher), ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(responseEntity);

        // returning DOES_NOT_EXIST implies that we CAN use Watcher and need to run the publish phase
        doCheckWithStatusCode(resource, endpoint, CheckResponse.ERROR, response);
    }

    public void testDoCheckErrorWithResponseException() throws IOException {
        whenElectedMaster();

        assertCheckWithException(resource, "", "_xpack");
    }

    public void testDoPublishTrue() throws IOException {
        final CheckResponse checkResponse = randomFrom(CheckResponse.EXISTS, CheckResponse.DOES_NOT_EXIST);
        final boolean publish = checkResponse == CheckResponse.DOES_NOT_EXIST;
        final MockHttpResource mockWatch = new MockHttpResource(owner, randomBoolean(), checkResponse, publish);
        final MultiHttpResource watches = new MultiHttpResource(owner, Collections.singletonList(mockWatch));
        final WatcherExistsHttpResource resource = new WatcherExistsHttpResource(owner, clusterService, watches);

        assertTrue(resource.doPublish(client));

        assertThat(mockWatch.checked, is(1));
        assertThat(mockWatch.published, is(publish ? 1 : 0));
    }

    public void testDoPublishFalse() throws IOException {
        final CheckResponse checkResponse = randomFrom(CheckResponse.DOES_NOT_EXIST, CheckResponse.ERROR);
        final MockHttpResource mockWatch = new MockHttpResource(owner, true, checkResponse, false);
        final MultiHttpResource watches = new MultiHttpResource(owner, Collections.singletonList(mockWatch));
        final WatcherExistsHttpResource resource = new WatcherExistsHttpResource(owner, clusterService, watches);

        assertFalse(resource.doPublish(client));

        assertThat(mockWatch.checked, is(1));
        assertThat(mockWatch.published, is(checkResponse == CheckResponse.DOES_NOT_EXIST ? 1 : 0));
    }

    public void testParameters() {
        final Map<String, String> parameters = resource.getParameters();

        assertThat(parameters.get("filter_path"), is(WatcherExistsHttpResource.WATCHER_CHECK_PARAMETERS.get("filter_path")));

        assertThat(parameters.size(), is(1));
    }

    public void testGetResources() {
        assertThat(resource.getWatches(), sameInstance(watches));
    }

    private void whenElectedMaster() {
        final ClusterState state = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(clusterService.state()).thenReturn(state);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);
    }

    private void whenNotElectedMaster() {
        final ClusterState state = mock(ClusterState.class);
        final DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        when(clusterService.state()).thenReturn(state);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(false);
    }

}
