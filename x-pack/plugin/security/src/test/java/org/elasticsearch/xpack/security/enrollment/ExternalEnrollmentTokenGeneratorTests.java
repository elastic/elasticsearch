/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.CheckedFunctionUtils.anyCheckedFunction;
import static org.elasticsearch.test.CheckedFunctionUtils.anyCheckedSupplier;
import static org.elasticsearch.xpack.security.enrollment.ExternalEnrollmentTokenGenerator.getFilteredAddresses;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalEnrollmentTokenGeneratorTests extends ESTestCase {
    private Environment environment;

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Enrollment is not supported in FIPS 140-2 as we are using PKCS#12 keystores", inFipsJvm());
    }

    @Before
    public void setupMocks() throws Exception {
        final Path tempDir = createTempDir();
        final Path httpCaPath = tempDir.resolve("httpCa.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.http.ssl.truststore.secure_password", "password");
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "password");
        secureSettings.setString("bootstrap.password", "password");
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.http.ssl.enabled", true)
            .put("xpack.security.authc.api_key.enabled", true)
            .put("xpack.http.ssl.truststore.path", httpCaPath)
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.keystore.path", httpCaPath)
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        environment = new Environment(settings, tempDir);
    }

    public void testCreateSuccess() throws Exception {
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(environment, client);
        final URL baseURL = new URL("http://localhost:9200");
        final URL createAPIKeyURL = externalEnrollmentTokenGenerator.createAPIKeyUrl(baseURL);
        final URL getHttpInfoURL = externalEnrollmentTokenGenerator.getHttpInfoUrl(baseURL);

        final HttpResponse httpResponseOK = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(client.execute(anyString(), any(URL.class), anyString(), any(SecureString.class), anyCheckedSupplier(), anyCheckedFunction()))
            .thenReturn(httpResponseOK);

        String createApiKeyResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("id", "DR6CzXkBDf8amV_48yYX")
                .field("name", "enrollment_token_API_key_VuaCfGcBCdbkQm")
                .field("expiration", "1622652381786")
                .field("api_key", "x3YqU_rqQwm-ESrkExcnOg")
                .endObject();
            createApiKeyResponseBody = Strings.toString(builder);
        }
        when(
            client.execute(
                eq("POST"),
                eq(createAPIKeyURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, createApiKeyResponseBody));

        String getHttpInfoResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .startObject("nodes")
                .startObject("sxLDrFu8SnKepObrEOjPZQ")
                .field("version", "8.0.0")
                .startObject("http")
                .startArray("bound_address")
                .value("[::1]:9200")
                .value("127.0.0.1:9200")
                .value("192.168.0.1:9201")
                .value("172.16.254.1:9202")
                .value("[2001:db8:0:1234:0:567:8:1]:9203")
                .endArray()
                .field("publish_address", "127.0.0.1:9200")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            getHttpInfoResponseBody = Strings.toString(builder);
        }
        when(
            client.execute(
                eq("GET"),
                eq(getHttpInfoURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, getHttpInfoResponseBody));

        final String tokenNode = externalEnrollmentTokenGenerator.createNodeEnrollmentToken(
            "elastic",
            new SecureString("elastic".toCharArray()),
            baseURL
        ).getEncoded();

        Map<String, String> infoNode = getDecoded(tokenNode);
        assertEquals("8.0.0", infoNode.get("ver"));
        assertEquals("[192.168.0.1:9201, 172.16.254.1:9202, [2001:db8:0:1234:0:567:8:1]:9203]", infoNode.get("adr"));
        assertEquals("ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", infoNode.get("fgr"));
        assertEquals("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg", infoNode.get("key"));

        final String tokenKibana = externalEnrollmentTokenGenerator.createKibanaEnrollmentToken(
            "elastic",
            new SecureString("elastic".toCharArray()),
            baseURL
        ).getEncoded();

        Map<String, String> infoKibana = getDecoded(tokenKibana);
        assertEquals("8.0.0", infoKibana.get("ver"));
        assertEquals("[192.168.0.1:9201, 172.16.254.1:9202, [2001:db8:0:1234:0:567:8:1]:9203]", infoKibana.get("adr"));
        assertEquals("ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", infoKibana.get("fgr"));
        assertEquals("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg", infoKibana.get("key"));
    }

    public void testFailedCreateApiKey() throws Exception {
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final URL baseURL = new URL("http://localhost:9200");
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(environment, client);
        final URL createAPIKeyURL = externalEnrollmentTokenGenerator.createAPIKeyUrl(baseURL);

        final HttpResponse httpResponseNotOK = new HttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(createAPIKeyURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseNotOK);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> externalEnrollmentTokenGenerator.createNodeEnrollmentToken("elastic", new SecureString("elastic".toCharArray()), baseURL)
                .getEncoded()
        );
        assertThat(ex.getMessage(), Matchers.containsString("Unexpected response code [400] from calling POST "));
    }

    public void testFailedRetrieveHttpInfo() throws Exception {
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final URL baseURL = new URL("http://localhost:9200");
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(environment, client);
        final URL createAPIKeyURL = externalEnrollmentTokenGenerator.createAPIKeyUrl(baseURL);
        final URL getHttpInfoURL = externalEnrollmentTokenGenerator.getHttpInfoUrl(baseURL);

        final HttpResponse httpResponseOK = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(createAPIKeyURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseOK);

        String createApiKeyResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("id", "DR6CzXkBDf8amV_48yYX")
                .field("name", "enrollment_token_API_key_VuaCfGcBCdbkQm")
                .field("expiration", "1622652381786")
                .field("api_key", "x3YqU_rqQwm-ESrkExcnOg")
                .endObject();
            createApiKeyResponseBody = Strings.toString(builder);
        }
        when(
            client.execute(
                eq("POST"),
                eq(createAPIKeyURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, createApiKeyResponseBody));

        final HttpResponse httpResponseNotOK = new HttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(getHttpInfoURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseNotOK);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> externalEnrollmentTokenGenerator.createNodeEnrollmentToken("elastic", new SecureString("elastic".toCharArray()), baseURL)
                .getEncoded()
        );
        assertThat(ex.getMessage(), Matchers.containsString("Unexpected response code [400] from calling GET "));
    }

    public void testFailedNoCaInKeystore() throws Exception {
        final Path tempDir = createTempDir();
        final Path httpNoCaPath = tempDir.resolve("transport.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/transport.p12"), httpNoCaPath);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.http.ssl.truststore.secure_password", "password");
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "password");
        secureSettings.setString("bootstrap.password", "password");
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.http.ssl.enabled", true)
            .put("xpack.security.authc.api_key.enabled", true)
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.keystore.path", httpNoCaPath)
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        environment = new Environment(settings, tempDir);
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(environment, client);
        final URL baseURL = new URL("http://localhost:9200");
        final URL createAPIKeyURL = externalEnrollmentTokenGenerator.createAPIKeyUrl(baseURL);
        final URL getHttpInfoURL = externalEnrollmentTokenGenerator.getHttpInfoUrl(baseURL);

        final HttpResponse httpResponseOK = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(createAPIKeyURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseOK);

        String createApiKeyResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("id", "DR6CzXkBDf8amV_48yYX")
                .field("name", "enrollment_token_API_key_VuaCfGcBCdbkQm")
                .field("expiration", "1622652381786")
                .field("api_key", "x3YqU_rqQwm-ESrkExcnOg")
                .endObject();
            createApiKeyResponseBody = Strings.toString(builder);
        }
        when(
            client.execute(
                eq("POST"),
                eq(createAPIKeyURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, createApiKeyResponseBody));

        final HttpResponse httpResponseNotOK = new HttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(getHttpInfoURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseNotOK);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> externalEnrollmentTokenGenerator.createNodeEnrollmentToken("elastic", new SecureString("elastic".toCharArray()), baseURL)
                .getEncoded()
        );
        assertThat(
            ex.getMessage(),
            equalTo(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore doesn't "
                    + "contain any PrivateKey entries where the associated certificate is a CA certificate"
            )
        );
    }

    public void testFailedManyCaInKeystore() throws Exception {
        final Path tempDir = createTempDir();
        final Path httpNoCaPath = tempDir.resolve("httpCa2.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa2.p12"), httpNoCaPath);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.http.ssl.truststore.secure_password", "password");
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "password");
        secureSettings.setString("bootstrap.password", "password");
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.http.ssl.enabled", true)
            .put("xpack.security.authc.api_key.enabled", true)
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.keystore.path", httpNoCaPath)
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        environment = new Environment(settings, tempDir);
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(environment, client);
        final URL baseURL = new URL("http://localhost:9200");
        final URL createAPIKeyURL = externalEnrollmentTokenGenerator.createAPIKeyUrl(baseURL);
        final URL getHttpInfoURL = externalEnrollmentTokenGenerator.getHttpInfoUrl(baseURL);

        final HttpResponse httpResponseOK = new HttpResponse(HttpURLConnection.HTTP_OK, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(createAPIKeyURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseOK);

        String createApiKeyResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("id", "DR6CzXkBDf8amV_48yYX")
                .field("name", "enrollment_token_API_key_VuaCfGcBCdbkQm")
                .field("expiration", "1622652381786")
                .field("api_key", "x3YqU_rqQwm-ESrkExcnOg")
                .endObject();
            createApiKeyResponseBody = Strings.toString(builder);
        }
        when(
            client.execute(
                eq("POST"),
                eq(createAPIKeyURL),
                eq(ElasticUser.NAME),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, createApiKeyResponseBody));

        final HttpResponse httpResponseNotOK = new HttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, new HashMap<>());
        when(
            client.execute(
                anyString(),
                eq(getHttpInfoURL),
                anyString(),
                any(SecureString.class),
                anyCheckedSupplier(),
                anyCheckedFunction()
            )
        ).thenReturn(httpResponseNotOK);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> externalEnrollmentTokenGenerator.createNodeEnrollmentToken("elastic", new SecureString("elastic".toCharArray()), baseURL)
                .getEncoded()
        );
        assertThat(
            ex.getMessage(),
            equalTo(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL "
                    + "configuration Keystore contains multiple PrivateKey entries where the associated certificate is a CA certificate"
            )
        );
    }

    public void testNoKeyStore() throws Exception {
        final Path tempDir = createTempDir();
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.enrollment.enabled", "true")
            .put("path.home", tempDir)
            .build();
        final Environment environment_no_keystore = new Environment(settings, tempDir);
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final URL baseURL = new URL("http://localhost:9200");
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(
            environment_no_keystore,
            client
        );

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> externalEnrollmentTokenGenerator.createNodeEnrollmentToken("elastic", new SecureString("elastic".toCharArray()), baseURL)
                .getEncoded()
        );
        assertThat(
            ex.getMessage(),
            Matchers.containsString("Elasticsearch node HTTP layer SSL configuration is not configured " + "with a keystore")
        );
    }

    public void testEnrollmentNotEnabled() throws Exception {
        final Path tempDir = createTempDir();
        final Path httpCaPath = tempDir.resolve("httpCa.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.http.ssl.truststore.secure_password", "password");
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "password");
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.http.ssl.enabled", true)
            .put("xpack.security.authc.api_key.enabled", true)
            .put("xpack.http.ssl.truststore.path", httpCaPath)
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.keystore.path", httpCaPath)
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        final Environment environment_not_enabled = new Environment(settings, tempDir);
        final CommandLineHttpClient client = mock(CommandLineHttpClient.class);
        final URL baseURL = new URL("http://localhost:9200");
        final ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = new ExternalEnrollmentTokenGenerator(
            environment_not_enabled,
            client
        );

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> externalEnrollmentTokenGenerator.createNodeEnrollmentToken("elastic", new SecureString("elastic".toCharArray()), baseURL)
                .getEncoded()
        );
        assertThat(
            ex.getMessage(),
            equalTo("[xpack.security.enrollment.enabled] must be set to `true` to " + "create an enrollment token")
        );
    }

    public void testGetFilteredAddresses() throws Exception {
        List<String> addresses = Arrays.asList(
            "[::1]:9200",
            "127.0.0.1:9200",
            "192.168.0.1:9201",
            "172.16.254.1:9202",
            "[2001:db8:0:1234:0:567:8:1]:9203"
        );
        List<String> filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(3));
        assertThat(
            filteredAddresses,
            Matchers.containsInAnyOrder("192.168.0.1:9201", "172.16.254.1:9202", "[2001:db8:0:1234:0:567:8:1]:9203")
        );
        assertThat(filteredAddresses.get(2), equalTo("[2001:db8:0:1234:0:567:8:1]:9203"));

        addresses = Arrays.asList("0.0.0.0:9200", "172.17.0.2:9200");
        filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(1));
        assertThat(filteredAddresses.get(0), equalTo("172.17.0.2:9200"));

        addresses = Arrays.asList("0.0.0.0:9200", "[::1]:9200", "127.0.0.1:9200");
        filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(2));
        assertThat(filteredAddresses.get(0), equalTo("127.0.0.1:9200"));
        assertThat(filteredAddresses.get(1), equalTo("[::1]:9200"));

        addresses = Arrays.asList("[::1]:9200", "127.0.0.1:9200");
        filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(2));
        assertThat(filteredAddresses.get(0), equalTo("127.0.0.1:9200"));
        assertThat(filteredAddresses.get(1), equalTo("[::1]:9200"));

        addresses = Arrays.asList("[::1]:9200", "127.0.0.1:9200", "[::1]:9200", "127.0.0.1:9200", "[::1]:9200", "127.0.0.1:9200");
        filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(2));
        assertThat(filteredAddresses.get(0), equalTo("127.0.0.1:9200"));
        assertThat(filteredAddresses.get(1), equalTo("[::1]:9200"));

        addresses = Arrays.asList("128.255.255.255", "[::1]:9200", "127.0.0.1:9200");
        filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(1));
        assertThat(filteredAddresses, Matchers.containsInAnyOrder("128.255.255.255"));

        addresses = Arrays.asList("8.8.8.8:9200", "192.168.0.1:9201", "172.16.254.1:9202", "[2001:db8:0:1234:0:567:8:1]:9203");
        filteredAddresses = getFilteredAddresses(addresses);
        assertThat(filteredAddresses, hasSize(4));
        assertThat(
            filteredAddresses,
            Matchers.containsInAnyOrder("8.8.8.8:9200", "192.168.0.1:9201", "172.16.254.1:9202", "[2001:db8:0:1234:0:567:8:1]:9203")
        );
        assertThat(filteredAddresses.get(3), equalTo("[2001:db8:0:1234:0:567:8:1]:9203"));

        final List<String> invalid_addresses = Arrays.asList("nldfnbndflbnl");
        UnknownHostException ex = expectThrows(UnknownHostException.class, () -> getFilteredAddresses(invalid_addresses));
        assertThat(ex.getMessage(), Matchers.containsString("nldfnbndflbnl"));
    }

    private Map<String, String> getDecoded(String token) throws IOException {
        final String jsonString = new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            final Map<String, Object> info = parser.map();
            assertNotEquals(info, null);
            return info.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
        }
    }

    private HttpResponse createHttpResponse(final int httpStatus, final String responseJson) throws IOException {
        final HttpResponse.HttpResponseBuilder builder = new HttpResponse.HttpResponseBuilder();
        builder.withHttpStatus(httpStatus);
        builder.withResponseBody(responseJson);
        return builder.build();
    }
}
