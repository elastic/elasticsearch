/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
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

/**
 * This class tests {@link CommandLineHttpClient} For extensive tests related to
 * ssl settings can be found {@link SSLConfigurationSettingsTests}
 */
public class CommandLineHttpClientTests extends ESTestCase {

    private MockWebServer webServer;
    private Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());

    @Before
    public void setup() throws Exception {
        webServer = createMockWebServer();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{\"test\": \"complete\"}"));
        webServer.start();
    }

    @After
    public void shutdown() throws Exception {
        webServer.close();
    }

    public void testCommandLineHttpClientCanExecuteAndReturnCorrectResultUsingSSLSettings() throws Exception {
        Path resource = getDataPath("/org/elasticsearch/xpack/security/keystore/testnode.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        Settings settings;
        if (randomBoolean()) {
            // with http ssl settings
            secureSettings.setString("xpack.security.http.ssl.truststore.secure_password", "testnode");
            settings = Settings.builder().put("xpack.security.http.ssl.truststore.path", resource.toString())
                    .put("xpack.security.http.ssl.verification_mode", VerificationMode.CERTIFICATE).setSecureSettings(secureSettings)
                    .build();
        } else {
            // with global settings
            secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
            settings = Settings.builder().put("xpack.ssl.truststore.path", resource.toString())
                    .put("xpack.ssl.verification_mode", VerificationMode.CERTIFICATE).setSecureSettings(secureSettings).build();
        }
        CommandLineHttpClient client = new CommandLineHttpClient(settings, environment);
        HttpResponse httpResponse = client.execute("GET", new URL("https://localhost:" + webServer.getPort() + "/test"), "u1",
                new SecureString(new char[]{'p'}), () -> null, is -> responseBuilder(is));

        assertNotNull("Should have http response", httpResponse);
        assertEquals("Http status code does not match", 200, httpResponse.getHttpStatus());
        assertEquals("Http response body does not match", "complete", httpResponse.getResponseBody().get("test"));
    }

    private MockWebServer createMockWebServer() {
        Path resource = getDataPath("/org/elasticsearch/xpack/security/keystore/testnode.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings =
                Settings.builder().put("xpack.ssl.keystore.path", resource.toString()).setSecureSettings(secureSettings).build();
        TestsSSLService sslService = new TestsSSLService(settings, environment);
        return new MockWebServer(sslService.sslContext(), false);
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
