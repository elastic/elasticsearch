/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cloud.gce;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "use http server")
public class GceInstancesServiceTests extends ESTestCase {
    private static HttpServer httpServer;

    /**
     * Creates embedded HTTP server to handle some GCE requests
     */
    @BeforeClass
    public static void startHttpd() throws Exception {
        final String hostAddress = InetAddress.getLoopbackAddress().getHostAddress();
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(hostAddress, 0), 0);

        addSimpleContent("/computeMetadata/v1/project/project-id", GceInstancesServiceTests.class.getSimpleName());
        addSimpleContent("/computeMetadata/v1/project/attributes/google-compute-default-zone", "test-zone");

        httpServer.start();
    }

    public static void addSimpleContent(String path, String value) {
        httpServer.createContext(path, exchange -> {
            Headers headers = exchange.getResponseHeaders();
            headers.add("Content-Type", "text/plain; charset=UTF-8");
            headers.add("Metadata-Flavor", "Google");

            final byte[] responseAsBytes = value.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseAsBytes.length);
            OutputStream responseBody = exchange.getResponseBody();
            responseBody.write(responseAsBytes);
            responseBody.close();
        });
    }

    @AfterClass
    public static void stopHttpd() {
        httpServer.stop(0);
        httpServer = null;
    }

    public void testMetadataServerValues() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(GceMetadataService.GCE_HOST.getKey(),
                "http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort())
            .build();
        GceInstancesService service = new GceInstancesServiceImpl(nodeSettings);
        assertThat(service.project(), is("GceInstancesServiceTests"));
        assertThat(service.zones(), is(Arrays.asList("test-zone")));
    }
}
