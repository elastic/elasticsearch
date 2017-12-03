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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.NoSuchFileException;

@SuppressForbidden(reason = "use http server")
public class URLBlobStoreTests extends ESTestCase {

    private static HttpServer httpServer;
    private static String blobName;
    private static byte[] message = new byte[512];
    private URLBlobStore urlBlobStore;

    @BeforeClass
    public static void startHttp() throws Exception {
        for (int i = 0; i < message.length; ++i) {
            message[i] = randomByte();
        }
        blobName = randomAlphaOfLength(8);

        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 6001), 0);

        httpServer.createContext("/indices/" + blobName, (s) -> {
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

    @Before
    public void storeSetup() throws MalformedURLException {
        Settings settings = Settings.EMPTY;
        String spec = "http://localhost:6001/";
        urlBlobStore = new URLBlobStore(settings, new URL(spec));
    }

    public void testURLBlobStoreCanReadBlob() throws IOException {
        BlobContainer container = urlBlobStore.blobContainer(BlobPath.cleanPath().add("indices"));
        try (InputStream stream = container.readBlob(blobName)) {
            byte[] bytes = new byte[message.length];
            int read = stream.read(bytes);
            assertEquals(message.length, read);
            assertArrayEquals(message, bytes);
        }
    }

    public void testNoBlobFound() throws IOException {
        BlobContainer container = urlBlobStore.blobContainer(BlobPath.cleanPath().add("indices"));
        String incorrectBlobName = "incorrect_" + blobName;
        try (InputStream ignored = container.readBlob(incorrectBlobName)) {
            fail("Should have thrown NoSuchFileException exception");
            ignored.read();
        } catch (NoSuchFileException e) {
            assertEquals(String.format("[%s] blob not found", incorrectBlobName), e.getMessage());
        }
    }
}
