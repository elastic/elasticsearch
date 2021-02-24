/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;

@SuppressForbidden(reason = "use a http server")
public class URLBlobContainerRetriesTests extends AbstractBlobContainerRetriesTestCase {
    private static URLHttpClient httpClient;

    @BeforeClass
    public static void setUpHttpClient() {
        final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        final CloseableHttpClient apacheHttpClient = HttpClients.custom()
            .setConnectionManager(connManager)
            .disableAutomaticRetries()
            .build();
        httpClient = new URLHttpClient(apacheHttpClient, connManager);
    }

    @AfterClass
    public static void tearDownHttpClient() throws IOException {
        httpClient.close();
    }

    @Override
    protected String downloadStorageEndpoint(String blob) {
        return "/" + blob;
    }

    @Override
    protected String bytesContentType() {
        return "application/octet-stream";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        return IOException.class;
    }

    @Override
    protected BlobContainer createBlobContainer(Integer maxRetries,
                                                TimeValue readTimeout,
                                                Boolean disableChunkedEncoding,
                                                ByteSizeValue bufferSize) {
        Settings.Builder settings = Settings.builder();

        if (maxRetries != null) {
            settings.put("repositories.uri.http.max_retries", maxRetries);
        }

        if (readTimeout != null) {
            settings.put("repositories.uri.http.socket_timeout", readTimeout);
        }

        try {
            URLBlobStore urlBlobStore = new URLBlobStore(settings.build(), new URL(getEndpointForServer()), httpClient);
            return urlBlobStore.blobContainer(new BlobPath());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getEndpointForServer() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + "/";
    }
}
