/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_HOST_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_PORT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

@SuppressForbidden(reason = "We start an HTTP proxy server to test proxy support for GCS")
public class GcsProxyIntegrationTests extends ESBlobStoreRepositoryIntegTestCase {

    private static HttpServer upstreamServer;
    private static WebProxyServer proxyServer;

    @BeforeClass
    public static void startServers() throws Exception {
        upstreamServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        upstreamServer.start();
        proxyServer = new WebProxyServer();
    }

    @AfterClass
    public static void stopServers() throws IOException {
        upstreamServer.stop(0);
        proxyServer.close();
    }

    @Before
    public void setUpHttpServer() {
        upstreamServer.createContext("/", new ForwardedViaProxyHandler(new GoogleCloudStorageHttpHandler("bucket")));
        upstreamServer.createContext("/token", new ForwardedViaProxyHandler(new FakeOAuth2HttpHandler()));
    }

    @After
    public void tearDownHttpServer() {
        upstreamServer.removeContext("/");
        upstreamServer.removeContext("/token");
    }

    @Override
    protected String repositoryType() {
        return GoogleCloudStorageRepository.TYPE;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings repositorySettings(String repoName) {
        return Settings.builder()
            .put(super.repositorySettings(repoName))
            .put(BUCKET.getKey(), "bucket")
            .put(CLIENT_NAME.getKey(), "test")
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        var secureSettings = new MockSecureSettings();
        secureSettings.setFile(
            CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("test").getKey(),
            TestUtils.createServiceAccount(random())
        );
        String upstreamServerUrl = "http://" + upstreamServer.getAddress().getHostString() + ":" + upstreamServer.getAddress().getPort();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), upstreamServerUrl)
            .put(TOKEN_URI_SETTING.getConcreteSettingForNamespace("test").getKey(), upstreamServerUrl + "/token")
            .put(PROXY_HOST_SETTING.getConcreteSettingForNamespace("test").getKey(), proxyServer.getHost())
            .put(PROXY_PORT_SETTING.getConcreteSettingForNamespace("test").getKey(), proxyServer.getPort())
            .put(PROXY_TYPE_SETTING.getConcreteSettingForNamespace("test").getKey(), "http")
            .setSecureSettings(secureSettings)
            .build();
    }
}
