/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ClusterAlertHttpResource}.
 */
public class ClusterAlertHttpResourceTests extends AbstractPublishableHttpResourceTestCase {

    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final String watchId = randomFrom(ClusterAlertsUtil.WATCH_IDS);
    private final String watchValue = "{\"totally-valid\":{}}";
    private final int minimumVersion = Math.min(ClusterAlertsUtil.LAST_UPDATED_VERSION, Version.CURRENT.id);

    private final ClusterAlertHttpResource resource = new ClusterAlertHttpResource(owner, licenseState, () -> watchId, () -> watchValue);

    public void testIsWatchDefined() {
        final ClusterAlertHttpResource noWatchResource = new ClusterAlertHttpResource(owner, licenseState, () -> watchId, null);

        assertThat(noWatchResource.isWatchDefined(), is(false));
        assertThat(resource.isWatchDefined(), is(true));
    }

    public void testWatchToHttpEntity() throws IOException {
        final byte[] watchValueBytes = watchValue.getBytes(ContentType.APPLICATION_JSON.getCharset());
        final byte[] actualBytes = new byte[watchValueBytes.length];
        final HttpEntity entity = resource.watchToHttpEntity();

        assertThat(entity.getContentType().getValue(), is(ContentType.APPLICATION_JSON.toString()));

        final InputStream byteStream = entity.getContent();

        assertThat(byteStream.available(), is(watchValueBytes.length));
        assertThat(byteStream.read(actualBytes), is(watchValueBytes.length));
        assertArrayEquals(watchValueBytes, actualBytes);

        assertThat(byteStream.available(), is(0));
    }

    public void testDoCheckGetWatchExists() throws IOException {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(true);

        final HttpEntity entity = entityForClusterAlert(true, minimumVersion);

        doCheckWithStatusCode(resource, "/_watcher/watch", watchId, successfulCheckStatus(), true, entity);
    }

    public void testDoCheckGetWatchDoesNotExist() throws IOException {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(true);

        if (randomBoolean()) {
            // it does not exist because it's literally not there
            assertCheckDoesNotExist(resource, "/_watcher/watch", watchId);
        } else {
            // it does not exist because we need to replace it
            final HttpEntity entity = entityForClusterAlert(false, minimumVersion);

            doCheckWithStatusCode(resource, "/_watcher/watch", watchId, successfulCheckStatus(), false, entity);
        }
    }

    public void testDoCheckWithExceptionGetWatchError() throws IOException {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(true);

        if (randomBoolean()) {
            // error because of a server error
            assertCheckWithException(resource, "/_watcher/watch", watchId);
        } else {
            // error because of a malformed response
            final HttpEntity entity = entityForClusterAlert(null, minimumVersion);

            doCheckWithStatusCode(resource, "/_watcher/watch", watchId, successfulCheckStatus(), null, entity);
        }
    }

    public void testDoCheckAsDeleteWatchExistsWhenNoWatchIsSpecified() throws IOException {
        final ClusterAlertHttpResource noWatchResource = new ClusterAlertHttpResource(owner, licenseState, () -> watchId, null);
        final boolean clusterAlertsAllowed = randomBoolean();

        // should not matter
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(clusterAlertsAllowed);

        assertCheckAsDeleteExists(noWatchResource, "/_watcher/watch", watchId);
    }

    public void testDoCheckWithExceptionAsDeleteWatchErrorWhenNoWatchIsSpecified() throws IOException {
        final ClusterAlertHttpResource noWatchResource = new ClusterAlertHttpResource(owner, licenseState, () -> watchId, null);
        final boolean clusterAlertsAllowed = randomBoolean();

        // should not matter
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(clusterAlertsAllowed);

        assertCheckAsDeleteWithException(noWatchResource, "/_watcher/watch", watchId);
    }

    public void testDoCheckAsDeleteWatchExists() throws IOException {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(false);

        assertCheckAsDeleteExists(resource, "/_watcher/watch", watchId);
    }

    public void testDoCheckWithExceptionAsDeleteWatchError() throws IOException {
        when(licenseState.checkFeature(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)).thenReturn(false);

        assertCheckAsDeleteWithException(resource, "/_watcher/watch", watchId);
    }

    public void testDoPublishTrue() throws IOException {
        assertPublishSucceeds(resource, "/_watcher/watch", watchId, Collections.emptyMap(), StringEntity.class);
    }

    public void testDoPublishFalseWithException() throws IOException {
        assertPublishWithException(resource, "/_watcher/watch", watchId, Collections.emptyMap(), StringEntity.class);
    }

    public void testShouldReplaceClusterAlertRethrowsIOException() throws IOException {
        final Response response = mock(Response.class);
        final HttpEntity entity = mock(HttpEntity.class);
        final XContent xContent = mock(XContent.class);

        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenThrow(new IOException("TEST - expected"));

        expectThrows(IOException.class, () -> resource.shouldReplaceClusterAlert(response, xContent, randomInt()));
    }

    public void testShouldReplaceClusterAlertThrowsExceptionForMalformedResponse() {
        final Response response = mock(Response.class);
        final HttpEntity entity = entityForClusterAlert(null, randomInt());
        final XContent xContent = XContentType.JSON.xContent();

        when(response.getEntity()).thenReturn(entity);

        expectThrows(RuntimeException.class, () -> resource.shouldReplaceClusterAlert(response, xContent, randomInt()));
    }

    public void testShouldReplaceClusterAlertReturnsTrueVersionIsNotExpected() throws IOException {
        final int minimumVersion = randomInt();
        final Response response = mock(Response.class);
        final HttpEntity entity = entityForClusterAlert(false, minimumVersion);
        final XContent xContent = XContentType.JSON.xContent();

        when(response.getEntity()).thenReturn(entity);

        assertThat(resource.shouldReplaceClusterAlert(response, xContent, minimumVersion), is(true));
    }

    public void testShouldReplaceCheckAlertChecksVersion() throws IOException {
        final int minimumVersion = randomInt();
        final int version = randomInt();
        final boolean shouldReplace = version < minimumVersion;

        final Response response = mock(Response.class);
        final HttpEntity entity = entityForClusterAlert(true, version);
        final XContent xContent = XContentType.JSON.xContent();

        when(response.getEntity()).thenReturn(entity);

        assertThat(resource.shouldReplaceClusterAlert(response, xContent, minimumVersion), is(shouldReplace));
    }

    public void testParameters() {
        final Map<String, String> parameters = new HashMap<>(resource.getDefaultParameters());

        assertThat(parameters.remove("filter_path"), is("metadata.xpack.version_created"));
        assertThat(parameters.isEmpty(), is(true));
    }

}
