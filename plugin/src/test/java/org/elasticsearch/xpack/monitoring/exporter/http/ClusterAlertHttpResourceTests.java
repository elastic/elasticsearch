/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
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

    private final ClusterAlertHttpResource resource = new ClusterAlertHttpResource(owner, licenseState, () -> watchId, () -> watchValue);

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
        when(licenseState.isMonitoringClusterAlertsAllowed()).thenReturn(true);

        assertCheckExists(resource, "/_xpack/watcher/watch", watchId);
    }

    public void testDoCheckGetWatchDoesNotExist() throws IOException {
        when(licenseState.isMonitoringClusterAlertsAllowed()).thenReturn(true);

        assertCheckDoesNotExist(resource, "/_xpack/watcher/watch", watchId);
    }

    public void testDoCheckWithExceptionGetWatchError() throws IOException {
        when(licenseState.isMonitoringClusterAlertsAllowed()).thenReturn(true);

        assertCheckWithException(resource, "/_xpack/watcher/watch", watchId);
    }

    public void testDoCheckAsDeleteWatchExists() throws IOException {
        when(licenseState.isMonitoringClusterAlertsAllowed()).thenReturn(false);

        assertCheckAsDeleteExists(resource, "/_xpack/watcher/watch", watchId);
    }

    public void testDoCheckWithExceptionAsDeleteWatchError() throws IOException {
        when(licenseState.isMonitoringClusterAlertsAllowed()).thenReturn(false);

        assertCheckAsDeleteWithException(resource, "/_xpack/watcher/watch", watchId);
    }

    public void testDoPublishTrue() throws IOException {
        assertPublishSucceeds(resource, "/_xpack/watcher/watch", watchId, StringEntity.class);
    }

    public void testDoPublishFalse() throws IOException {
        assertPublishFails(resource, "/_xpack/watcher/watch", watchId, StringEntity.class);
    }

    public void testDoPublishFalseWithException() throws IOException {
        assertPublishWithException(resource, "/_xpack/watcher/watch", watchId, StringEntity.class);
    }

    public void testParameters() {
        final Map<String, String> parameters = new HashMap<>(resource.getParameters());

        assertThat(parameters.remove("filter_path"), is("$NONE"));
        assertThat(parameters.isEmpty(), is(true));
    }

}
