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

package org.elasticsearch.repositories.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageBlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    private static HttpServer httpServer;
    private static byte[] serviceAccount;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        serviceAccount = createServiceAccount();
    }

    @Before
    public void setUpHttpServer() {
        httpServer.createContext("/", new InternalHttpHandler());
        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    @After
    public void tearDownHttpServer() {
        httpServer.removeContext("/");
        httpServer.removeContext("/token");
    }

    @Override
    protected String repositoryType() {
        return GoogleCloudStorageRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(BUCKET.getKey(), "bucket")
            .put(CLIENT_NAME.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder settings = Settings.builder();
        settings.put(super.nodeSettings(nodeOrdinal));

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        settings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint);
        settings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint + "/token");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("test").getKey(), serviceAccount);
        settings.setSecureSettings(secureSettings);

        return settings.build();
    }

    public void testChunkSize() {
        // default chunk size
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE, Settings.EMPTY);
        ByteSizeValue chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetaData);
        assertEquals(GoogleCloudStorageRepository.MAX_CHUNK_SIZE, chunkSize);

        // chunk size in settings
        final int size = randomIntBetween(1, 100);
        repositoryMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                       Settings.builder().put("chunk_size", size + "mb").build());
        chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetaData);
        assertEquals(new ByteSizeValue(size, ByteSizeUnit.MB), chunkSize);

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetaData repoMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "0").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetaData);
        });
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetaData repoMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "-1").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetaData);
        });
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetaData repoMetaData = new RepositoryMetaData("repo", GoogleCloudStorageRepository.TYPE,
                                                                        Settings.builder().put("chunk_size", "101mb").build());
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetaData);
        });
        assertEquals("failed to parse value [101mb] for setting [chunk_size], must be <= [100mb]", e.getMessage());
    }

    private static byte[] createServiceAccount() throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(1024);
        final String privateKey = Base64.getEncoder().encodeToString(keyPairGenerator.generateKeyPair().getPrivate().getEncoded());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), out)) {
            builder.startObject();
            {
                builder.field("type", "service_account");
                builder.field("project_id", getTestClass().getName().toLowerCase(Locale.ROOT));
                builder.field("private_key_id", UUID.randomUUID().toString());
                builder.field("private_key", "-----BEGIN PRIVATE KEY-----\n" + privateKey + "\n-----END PRIVATE KEY-----\n");
                builder.field("client_email", "elastic@appspot.gserviceaccount.com");
                builder.field("client_id", String.valueOf(randomNonNegativeLong()));
            }
            builder.endObject();
        }
        return out.toByteArray();
    }

    /**
     * Minimal HTTP handler that acts as a Google Cloud Storage compliant server
     *
     * Note: it does not support resumable uploads
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
    private static class InternalHttpHandler implements HttpHandler {

        private final ConcurrentMap<String, BytesReference> blobs = new ConcurrentHashMap<>();

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
            try {
                if (Regex.simpleMatch("GET /storage/v1/b/bucket/o*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                    final String prefix = params.get("prefix");

                    final List<Map.Entry<String, BytesReference>> listOfBlobs = blobs.entrySet().stream()
                        .filter(blob -> prefix == null || blob.getKey().startsWith(prefix)).collect(Collectors.toList());

                    final StringBuilder list = new StringBuilder();
                    list.append("{\"kind\":\"storage#objects\",\"items\":[");
                    for (Iterator<Map.Entry<String, BytesReference>> it = listOfBlobs.iterator(); it.hasNext(); ) {
                        Map.Entry<String, BytesReference> blob = it.next();
                        list.append("{\"kind\":\"storage#object\",");
                        list.append("\"bucket\":\"bucket\",");
                        list.append("\"name\":\"").append(blob.getKey()).append("\",");
                        list.append("\"id\":\"").append(blob.getKey()).append("\",");
                        list.append("\"size\":\"").append(blob.getValue().length()).append("\"");
                        list.append('}');

                        if (it.hasNext()) {
                            list.append(',');
                        }
                    }
                    list.append("]}");

                    byte[] response = list.toString().getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else if (Regex.simpleMatch("GET /storage/v1/b/bucket*", request)) {
                    byte[] response = ("{\"kind\":\"storage#bucket\",\"name\":\"bucket\",\"id\":\"0\"}").getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);

                } else if (Regex.simpleMatch("GET /download/storage/v1/b/bucket/o/*", request)) {
                    BytesReference blob = blobs.get(exchange.getRequestURI().getPath().replace("/download/storage/v1/b/bucket/o/", ""));
                    if (blob != null) {
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                        exchange.getResponseBody().write(blob.toBytesRef().bytes);
                    } else {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    }

                } else if (Regex.simpleMatch("DELETE /storage/v1/b/bucket/o/*", request)) {
                    int deletions = 0;
                    for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<String, BytesReference> blob = iterator.next();
                        if (blob.getKey().equals(exchange.getRequestURI().toString())) {
                            iterator.remove();
                            deletions++;
                        }
                    }
                    exchange.sendResponseHeaders((deletions > 0 ? RestStatus.OK : RestStatus.NO_CONTENT).getStatus(), -1);

                } else if (Regex.simpleMatch("POST /batch/storage/v1", request)) {
                    final String uri = "/storage/v1/b/bucket/o/";
                    final StringBuilder batch = new StringBuilder();
                    for (String line : Streams.readAllLines(new BufferedInputStream(exchange.getRequestBody()))) {
                        if (line.length() == 0 || line.startsWith("--") || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                            batch.append(line).append('\n');
                        } else if (line.startsWith("DELETE")) {
                            final String name = line.substring(line.indexOf(uri) + uri.length(), line.lastIndexOf(" HTTP"));
                            if (Strings.hasText(name)) {
                                if (blobs.entrySet().removeIf(blob -> blob.getKey().equals(URLDecoder.decode(name, UTF_8)))) {
                                    batch.append("HTTP/1.1 204 NO_CONTENT").append('\n');
                                    batch.append('\n');
                                }
                            }
                        }
                    }
                    byte[] response = batch.toString().getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", exchange.getRequestHeaders().getFirst("Content-Type"));
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else if (Regex.simpleMatch("POST /upload/storage/v1/b/bucket/*uploadType=multipart*", request)) {
                    byte[] response = new byte[0];
                    try (BufferedInputStream in = new BufferedInputStream(new GZIPInputStream(exchange.getRequestBody()))) {
                        String blob = null;
                        int read;
                        while ((read = in.read()) != -1) {
                            boolean markAndContinue = false;
                            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                                do { // search next consecutive {carriage return, new line} chars and stop
                                    if ((char) read == '\r') {
                                        int next = in.read();
                                        if (next != -1) {
                                            if (next == '\n') {
                                                break;
                                            }
                                            out.write(read);
                                            out.write(next);
                                            continue;
                                        }
                                    }
                                    out.write(read);
                                } while ((read = in.read()) != -1);

                                final String line = new String(out.toByteArray(), UTF_8);
                                if (line.length() == 0 || line.equals("\r\n") || line.startsWith("--")
                                    || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                                    markAndContinue = true;
                                } else if (line.startsWith("{\"bucket\":\"bucket\"")) {
                                    markAndContinue = true;
                                    Matcher matcher = Pattern.compile("\"name\":\"([^\"]*)\"").matcher(line);
                                    if (matcher.find()) {
                                        blob = matcher.group(1);
                                        response = line.getBytes(UTF_8);
                                    }
                                }
                                if (markAndContinue) {
                                    in.mark(Integer.MAX_VALUE);
                                    continue;
                                }
                            }
                            if (blob != null) {
                                in.reset();
                                try (ByteArrayOutputStream binary = new ByteArrayOutputStream()) {
                                    while ((read = in.read()) != -1) {
                                        binary.write(read);
                                    }
                                    binary.flush();
                                    byte[] tmp = binary.toByteArray();
                                    // removes the trailing end "\r\n--__END_OF_PART__--\r\n" which is 23 bytes long
                                    blobs.put(blob, new BytesArray(Arrays.copyOf(tmp, tmp.length - 23)));
                                } finally {
                                    blob = null;
                                }
                            }
                        }
                    }
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else {
                    exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
                }
            } finally {
                exchange.close();
            }
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a fake OAuth2 authentication service")
    private static class FakeOAuth2HttpHandler implements HttpHandler {
        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            byte[] response = ("{\"access_token\":\"foo\",\"token_type\":\"Bearer\",\"expires_in\":3600}").getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        }
    }
}
