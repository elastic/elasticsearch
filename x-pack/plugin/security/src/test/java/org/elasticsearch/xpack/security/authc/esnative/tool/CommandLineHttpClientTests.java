/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettingsTests;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.esnative.tool.HttpResponse.HttpResponseBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

/**
 * This class tests {@link CommandLineHttpClient} For extensive tests related to
 * ssl settings can be found {@link SSLConfigurationSettingsTests}
 */
public class CommandLineHttpClientTests extends ESTestCase {

    private MockWebServer webServer;
    private Path certPath;
    private Path keyPath;

    @Before
    public void setup() throws Exception {
        certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");

        webServer = createMockWebServer();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{\"test\": \"complete\"}"));
        webServer.start();
    }

    @After
    public void shutdown() {
        webServer.close();
    }

    public void testCommandLineHttpClientCanExecuteAndReturnCorrectResultUsingSSLSettings() throws Exception {
        Settings settings = getHttpSslSettings()
            .put("xpack.security.http.ssl.certificate_authorities", certPath.toString())
            .put("xpack.security.http.ssl.verification_mode", VerificationMode.CERTIFICATE)
            .build();
        CommandLineHttpClient client = new CommandLineHttpClient(TestEnvironment.newEnvironment(settings));
        HttpResponse httpResponse = client.execute("GET", new URL("https://localhost:" + webServer.getPort() + "/test"), "u1",
                new SecureString(new char[]{'p'}), () -> null, is -> responseBuilder(is));

        assertNotNull("Should have http response", httpResponse);
        assertEquals("Http status code does not match", 200, httpResponse.getHttpStatus());
        assertEquals("Http response body does not match", "complete", httpResponse.getResponseBody().get("test"));
    }

    public void testGetDefaultURLFailsWithHelpfulMessage() {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("network.host", "_ec2:privateIpv4_")
            .build();
        CommandLineHttpClient client = new CommandLineHttpClient(TestEnvironment.newEnvironment(settings));
        assertThat(expectThrows(IllegalStateException.class, () -> client.getDefaultURL()).getMessage(),
            containsString("unable to determine default URL from settings, please use the -u option to explicitly provide the url"));
    }

    private MockWebServer createMockWebServer() {
        Settings settings = getHttpSslSettings().build();
        TestsSSLService sslService = new TestsSSLService(TestEnvironment.newEnvironment(settings));
        return new MockWebServer(sslService.sslContext("xpack.security.http.ssl."), false);
    }

    private Settings.Builder getHttpSslSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        return Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key", keyPath.toString())
            .put("xpack.security.http.ssl.certificate", certPath.toString())
            .setSecureSettings(secureSettings);
    }

    private HttpResponseBuilder responseBuilder(final InputStream is) throws IOException {
        final HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder();
        if (is != null) {
            byte[] bytes = toByteArray(is);
            httpResponseBuilder.withResponseBody(new String(bytes, StandardCharsets.UTF_8));
        }
        return httpResponseBuilder;
    }

    private byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] internalBuffer = new byte[1024];
        int read = is.read(internalBuffer);
        while (read != -1) {
            baos.write(internalBuffer, 0, read);
            read = is.read(internalBuffer);
        }
        return baos.toByteArray();
    }
}
