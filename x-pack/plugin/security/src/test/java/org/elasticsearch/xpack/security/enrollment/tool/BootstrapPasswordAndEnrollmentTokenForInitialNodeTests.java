/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.enrollment.EnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.enrollment.EnrollmentToken;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BootstrapPasswordAndEnrollmentTokenForInitialNodeTests extends CommandTestCase {
    private CommandLineHttpClient client;
    private KeyStoreWrapper keyStoreWrapper;
    private EnrollmentTokenGenerator enrollmentTokenGenerator;
    private URL checkClusterHealthUrl;
    private URL setElasticUserPasswordUrl;
    private Path confDir;
    private Path tempDir;
    private Settings settings;

    @Override
    protected Command newCommand() {
        return new BootstrapPasswordAndEnrollmentTokenForInitialNode(environment -> client, environment -> keyStoreWrapper,
            environment -> enrollmentTokenGenerator) {
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected Environment readSecureSettings(Environment env, SecureString password) {
                return new Environment(settings, tempDir);
            }
            @Override
            protected Environment createEnv(Map<String, String> settings) {
                return new Environment(BootstrapPasswordAndEnrollmentTokenForInitialNodeTests.this.settings, confDir);
            }
        };
    }

    @BeforeClass
    public static void muteInFips(){
        assumeFalse("Enrollment is not supported in FIPS 140-2 as we are using PKCS#12 keystores", inFipsJvm());
    }

    @Before
    public void setup() throws Exception {
        this.keyStoreWrapper = mock(KeyStoreWrapper.class);
        when(keyStoreWrapper.isLoaded()).thenReturn(true);
        this.client = mock(CommandLineHttpClient.class);
        when(client.getDefaultURL()).thenReturn("https://localhost:9200");
        checkClusterHealthUrl = BootstrapPasswordAndEnrollmentTokenForInitialNode.checkClusterHealthUrl(client);
        setElasticUserPasswordUrl = BootstrapPasswordAndEnrollmentTokenForInitialNode.setElasticUserPasswordUrl(client);
        HttpResponse healthResponse =
            new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", randomFrom("yellow", "green")));
        when(client.execute(anyString(), eq(checkClusterHealthUrl), anyString(), any(SecureString.class), anyObject(), anyObject()))
            .thenReturn(healthResponse);
        HttpResponse setPasswordResponse =
            new HttpResponse(HttpURLConnection.HTTP_OK, Collections.emptyMap());
        when(client.execute(anyString(), eq(setElasticUserPasswordUrl), anyString(), any(SecureString.class), anyObject(), anyObject()))
            .thenReturn(setPasswordResponse);
        this.enrollmentTokenGenerator = mock(EnrollmentTokenGenerator.class);
        EnrollmentToken kibanaToken = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        EnrollmentToken nodeToken = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:4BhUk-mkFm-AwvRFg90KJ",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        when(enrollmentTokenGenerator.createKibanaEnrollmentToken(anyString(), any(SecureString.class)))
            .thenReturn(kibanaToken);
        when(enrollmentTokenGenerator.createNodeEnrollmentToken(anyString(), any(SecureString.class)))
            .thenReturn(nodeToken);
        tempDir = createTempDir();
        confDir = tempDir.resolve("config");
        final Path httpCaPath = tempDir.resolve("httpCa.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.http.ssl.truststore.secure_password", "password");
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "password");
        secureSettings.setString("keystore.seed", "password");
        settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.http.ssl.enabled", true)
            .put("xpack.security.authc.api_key.enabled", true)
            .put("xpack.http.ssl.truststore.path", "httpCa.p12")
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.keystore.path", "httpCa.p12")
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
    }

    public void testGenerateNewPasswordSuccess() throws Exception {
        terminal.addSecretInput("password");
        String includeNodeEnrollmentToken = randomBoolean() ? "--include-node-enrollment-token" : "";
        String output = execute(includeNodeEnrollmentToken);
        assertThat(output, containsString("elastic user password: Aljngvodjb94j8HSY803"));
        assertThat(output, containsString("CA fingerprint: ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428" +
            "f8a91362d"));
        assertThat(output, containsString("Kibana enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wL" +
            "jE6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWUyN" +
            "mI0ZDQyOGY4YTkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
        if (includeNodeEnrollmentToken.equals("--include-node-enrollment-token")) {
            assertThat(output, containsString("Node enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wLj" +
                "E6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWUy" +
                "NmI0ZDQyOGY4YTkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOjRCaFVrLW1rRm0tQXd2UkZnOTBLSiJ9"));
        } else {
            assertFalse(output.contains("Node enrollment token: "));
        }
    }

    public void testBootstrapPasswordSuccess() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final Path tempDir = createTempDir();
        secureSettings.setString("bootstrap.password", "password");
        settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        terminal.addSecretInput("password");
        String includeNodeEnrollmentToken = randomBoolean() ? "--include-node-enrollment-token" : "";
        String output = execute(includeNodeEnrollmentToken);
        assertFalse(terminal.getOutput().contains("elastic user password:"));
        assertThat(terminal.getOutput(), containsString("CA fingerprint: ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428" +
            "f8a91362d"));
        assertThat(output, containsString("Kibana enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wL" +
            "jE6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWUy" +
            "NmI0ZDQyOGY4YTkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
        if (includeNodeEnrollmentToken.equals("--include-node-enrollment-token")) {
            assertThat(output, containsString("Node enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wLj" +
                "E6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWU" +
                "yNmI0ZDQyOGY4YTkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOjRCaFVrLW1rRm0tQXd2UkZnOTBLSiJ9"));
        } else {
            assertFalse(output.contains("Node enrollment token: "));
        }
    }

    public void testClusterHealthIsRed() throws Exception {
        HttpResponse healthResponse =
            new HttpResponse(HttpURLConnection.HTTP_OK, Map.of("status", "red"));
        when(client.execute(anyString(), eq(checkClusterHealthUrl), anyString(), any(SecureString.class), anyObject(), anyObject()))
            .thenReturn(healthResponse);
        doCallRealMethod().when(client).checkClusterHealthWithRetriesWaitingForCluster(anyString(), anyObject(), anyInt());
        terminal.addSecretInput("password");
        final UserException ex = expectThrows(UserException.class, () -> execute(""));
        assertNull(ex.getMessage());
        assertThat(ex.exitCode, is(ExitCodes.UNAVAILABLE));
    }

    public void testFailedToSetPassword() throws Exception {
        HttpResponse setPasswordResponse =
            new HttpResponse(HttpURLConnection.HTTP_UNAUTHORIZED, Collections.emptyMap());
        when(client.execute(anyString(), eq(setElasticUserPasswordUrl), anyString(), any(SecureString.class), anyObject(), anyObject()))
            .thenReturn(setPasswordResponse);
        terminal.addSecretInput("password");
        final UserException ex = expectThrows(UserException.class, () -> execute(""));
        assertNull(ex.getMessage());
        assertThat(ex.exitCode, is(ExitCodes.UNAVAILABLE));
    }

    public void testNoKeystorePassword() {
        final UserException ex = expectThrows(UserException.class, () -> execute(""));
        assertNull(ex.getMessage());
        assertThat(ex.exitCode, is(ExitCodes.USAGE));
    }
}
