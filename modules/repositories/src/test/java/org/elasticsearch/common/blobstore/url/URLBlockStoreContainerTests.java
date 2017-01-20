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

package org.elasticsearch.common.blobstore.url;

import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;

public class URLBlockStoreContainerTests extends ESTestCase {

    private static HttpServer httpServer;
    private static byte[] message = new byte[1024];

    @BeforeClass
    public static void startHttp() throws Exception {
//        logDir = createTempDir();
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 6001), 0);

        httpServer.createContext("/hjdfd", (s) -> {
            s.sendResponseHeaders(200, message.length);
            OutputStream responseBody = s.getResponseBody();
            responseBody.write(message);
            responseBody.close();
        });

        httpServer.start();
    }

    @AfterClass
    public static void stopHttp() throws IOException {
        httpServer.stop(0);
        httpServer = null;
    }

    public void testBlobStore() throws IOException {
        Settings settings = Settings.EMPTY;
        URLBlobStore urlBlobStore = new URLBlobStore(settings, new URL("http://localhost:6001"));
        BlobContainer contain = urlBlobStore.blobContainer(BlobPath.cleanPath());
        InputStream stream = contain.readBlob("/hjdfd");
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream))) {
            System.out.println(bufferedReader.readLine());
        }
    }
}
