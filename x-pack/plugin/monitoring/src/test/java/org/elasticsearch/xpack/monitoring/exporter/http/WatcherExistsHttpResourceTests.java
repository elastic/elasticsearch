/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpResource.ResourcePublishResult;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.monitoring.exporter.http.AsyncHttpResourceHelper.wrapMockListener;
import static org.elasticsearch.xpack.monitoring.exporter.http.PublishableHttpResource.GET_EXISTS;
import static org.elasticsearch.xpack.monitoring.exporter.http.WatcherExistsHttpResource.XPACK_DOES_NOT_EXIST;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link WatcherExistsHttpResource}.
 */
public class WatcherExistsHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final MultiHttpResource mockWatches = mock(MultiHttpResource.class);

    private final WatcherExistsHttpResource resource = new WatcherExistsHttpResource(owner, clusterService, mockWatches);
    private final Map<String, String> expectedParameters = getParameters(resource.getDefaultParameters(), GET_EXISTS, XPACK_DOES_NOT_EXIST);

    public void testDoCheckIgnoresClientWhenNotElectedMaster() {
        whenNotElectedMaster();

        resource.doCheck(client, wrapMockListener(checkListener));

        verify(checkListener).onResponse(true);
        verifyNoMoreInteractions(client);
    }

    public void testDoCheckExistsFor404() {
        whenElectedMaster();

        // /_xpack returning a 404 means ES didn't handle the request properly and X-Pack doesn't exist
        doCheckWithStatusCode(resource, "", "_xpack", notFoundCheckStatus(), GET_EXISTS, XPACK_DOES_NOT_EXIST, true);
    }

    public void testDoCheckExistsFor400() {
        whenElectedMaster();

        // /_xpack returning a 400 means X-Pack does not exist
        doCheckWithStatusCode(resource, "", "_xpack", RestStatus.BAD_REQUEST, GET_EXISTS, XPACK_DOES_NOT_EXIST, true);
    }

    public void testDoCheckExistsAsElectedMaster() {
        whenElectedMaster();

        final String[] noWatcher = { "{}", """
            {"features":{"watcher":{"available":true,"enabled":false}}}""", """
            {"features":{"watcher":{"available":false,"enabled":true}}}""", """
            {"features":{"watcher":{"available":true}}}""", """
            {"features":{"watcher":{"enabled":true}}}""" };

        final String endpoint = "/_xpack";
        // success only implies that it responded; it also needs to be available and enabled
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = new StringEntity(randomFrom(noWatcher), ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(responseEntity);

        // returning EXISTS implies that we CANNOT use Watcher to avoid running the publish phase
        doCheckWithStatusCode(resource, expectedParameters, endpoint, true, response);

        verify(response).getEntity();
    }

    public void testDoCheckDoesNotExist() {
        whenElectedMaster();

        final String[] hasWatcher = {
            "{\"features\":{\"watcher\":{\"available\":true,\"enabled\":true}}}",
            "{\"features\":{\"watcher\":{\"enabled\":true,\"available\":true}}}" };

        final String endpoint = "/_xpack";
        // success only implies that it responded; it also needs to be available and enabled
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = new StringEntity(randomFrom(hasWatcher), ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(responseEntity);

        // returning DOES_NOT_EXIST implies that we CAN use Watcher and need to run the publish phase
        doCheckWithStatusCode(resource, expectedParameters, endpoint, false, response);

        verify(response).getEntity();
    }

    public void testDoCheckErrorWithDataException() {
        whenElectedMaster();

        final String[] errorWatcher = {
            """
                {"features":{}}""", // missing watcher object 'string'
            """
                {"watcher":{"enabled":true,"available":true}}""", // missing features outer object
            "{{}" // extra {
        };

        final String endpoint = "/_xpack";
        // success only implies that it responded; it also needs to be available and enabled
        final Response response = response("GET", endpoint, successfulCheckStatus());
        final HttpEntity responseEntity = new StringEntity(randomFrom(errorWatcher), ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(responseEntity);

        // returning an error implies that we CAN use Watcher and need to run the publish phase
        doCheckWithStatusCode(resource, expectedParameters, endpoint, null, response);
    }

    public void testDoCheckErrorWithResponseException() {
        whenElectedMaster();

        assertCheckWithException(resource, expectedParameters, "", "_xpack");
    }

    public void testDoPublishTrue() {
        final boolean checkResponse = randomBoolean();
        final boolean publish = checkResponse == false;
        final MockHttpResource mockWatch = new MockHttpResource(owner, randomBoolean(), checkResponse, publish);
        final MultiHttpResource watches = new MultiHttpResource(owner, Collections.singletonList(mockWatch));
        final WatcherExistsHttpResource watcherExistsHttpResource = new WatcherExistsHttpResource(owner, clusterService, watches);

        watcherExistsHttpResource.doPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(ResourcePublishResult.ready());

        assertThat(mockWatch.checked, is(1));
        assertThat(mockWatch.published, is(publish ? 1 : 0));
    }

    public void testDoPublishFalse() {
        final MockHttpResource mockWatch = new MockHttpResource(owner, true, false, false);
        final MultiHttpResource watches = new MultiHttpResource(owner, Collections.singletonList(mockWatch));
        final WatcherExistsHttpResource watcherExistsHttpResource = new WatcherExistsHttpResource(owner, clusterService, watches);

        watcherExistsHttpResource.doPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(new ResourcePublishResult(false));

        assertThat(mockWatch.checked, is(1));
        assertThat(mockWatch.published, is(1));
    }

    public void testDoPublishException() {
        final MockHttpResource mockWatch = new MockHttpResource(owner, true, false, null);
        final MultiHttpResource watches = new MultiHttpResource(owner, Collections.singletonList(mockWatch));
        final WatcherExistsHttpResource watcherExistsHttpResource = new WatcherExistsHttpResource(owner, clusterService, watches);

        watcherExistsHttpResource.doPublish(client, wrapMockListener(publishListener));

        verifyPublishListener(null);

        assertThat(mockWatch.checked, is(1));
        assertThat(mockWatch.published, is(1));
    }

    public void testParameters() {
        final Map<String, String> parameters = resource.getDefaultParameters();

        assertThat(parameters.get("filter_path"), is(WatcherExistsHttpResource.WATCHER_CHECK_PARAMETERS.get("filter_path")));

        assertThat(parameters.size(), is(1));
    }

    public void testGetResources() {
        assertThat(resource.getWatches(), sameInstance(mockWatches));
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
