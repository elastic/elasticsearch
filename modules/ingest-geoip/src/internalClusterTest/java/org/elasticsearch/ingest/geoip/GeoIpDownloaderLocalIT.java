/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@ClusterScope(scope = Scope.TEST)
@SuppressForbidden(reason = "fix test to not use com.sun.net.httpserver.HttpServer, which isn't portable on all JVMs")
public class GeoIpDownloaderLocalIT extends AbstractGeoIpIT {

    private static HttpServer mockServer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getServerAddress() + "/databases")
            .put(GeoIpDownloader.ENABLED_SETTING.getKey(), false)
            .build();
    }

    private static String getServerAddress() {
        InetSocketAddress address = mockServer.getAddress();
        return String.format(Locale.ROOT, "http://%s:%d", address.getHostName(), address.getPort());
    }

    @BeforeClass
    public static void setupMockHttpServer() throws IOException {
        mockServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        mockServer.createContext("/databases", exchange -> {
            String data = String.format("[\n" +
                "  {\n" +
                "    \"md5_hash\": \"61a09e631020091f049b528a17c8db03\",\n" +
                "    \"name\": \"db.mmdb.gz\",\n" +
                "    \"provider\": \"maxmind\",\n" +
                "    \"updated\": 1611619252,\n" +
                "    \"url\": \"%s\"\n" +
                "  }\n" +
                "]", getServerAddress() + "/db.mmdb.gz");
            exchange.sendResponseHeaders(200, data.length());
            exchange.getResponseBody().write(data.getBytes(StandardCharsets.UTF_8));
            exchange.getResponseBody().close();
            exchange.close();
        });
        mockServer.createContext("/db.mmdb.gz", exchange -> {
            exchange.sendResponseHeaders(200, 3);
            exchange.getResponseBody().write(new byte[]{1, 2, 3});
            exchange.getResponseBody().close();
            exchange.close();
        });
        mockServer.createContext("/", exchange -> {
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        });
        mockServer.start();
    }

    @AfterClass
    public static void cleanupMockHttpServer() {
        mockServer.stop(1);
    }

    public void testGeoIpDatabasesDownload() throws Exception {
        assertTrue(client().admin().cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloader.ENABLED_SETTING.getKey(), true))
            .get().isAcknowledged());
        String dbName = "db.mmdb";
        assertBusy(() -> {
            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task =
                PersistentTasksCustomMetadata.getTaskWithId(clusterService().state(), GeoIpDownloader.GEOIP_DOWNLOADER);
            assertNotNull(task);
            GeoIpTaskState state = (GeoIpTaskState) task.getState();
            assertNotNull(state);
            assertEquals(Set.of(dbName), state.getDatabases().keySet());
            GeoIpTaskState.Metadata db = state.getDatabases().get(dbName);
            assertEquals(0, db.getFirstChunk());
            assertEquals(0, db.getLastChunk());
            assertEquals("61a09e631020091f049b528a17c8db03", db.getMd5());
        });

        GetResponse res = client().prepareGet(GeoIpDownloader.DATABASES_INDEX, dbName + "_0").get();
        Map<String, Object> source = res.getSourceAsMap();

        assertEquals(Set.of("name", "chunk", "data"), source.keySet());
        assertEquals(dbName, source.get("name"));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) source.get("data"));
        assertEquals(0, source.get("chunk"));
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 1;
    }
}
