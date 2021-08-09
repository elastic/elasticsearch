/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import joptsimple.OptionSet;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.enrollment.EnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.enrollment.EnrollmentToken;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.mock.orig.Mockito.doReturn;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BootstrapPasswordAndEnrollmentTokenForInitialNodeTests extends ESTestCase {
    private Environment environment;

    @BeforeClass
    public static void muteInFips(){
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
        secureSettings.setString("keystore.seed", "password");
        final Settings settings = Settings.builder()
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
        environment = new Environment(settings, tempDir);
    }

    public void testGenerateNewPasswordSuccess() throws Exception {
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command_util = mock(BootstrapPasswordAndEnrollmentTokenForInitialNode
            .class);
        final CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(environment);
        final MockTerminal terminal = new MockTerminal();
        final CommandLineHttpClient client = new CommandLineHttpClient(environment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        final EnrollmentTokenGenerator enrollmentTokenGenerator = mock(EnrollmentTokenGenerator.class);
        final EnrollmentToken token = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command = new BootstrapPasswordAndEnrollmentTokenForInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) {
                return client_util;
            }
            @Override
            protected EnrollmentTokenGenerator getEnrollmentTokenGenerator(Environment env) {
                return enrollmentTokenGenerator;
            }
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected SecureString readBootstrapPassword(Environment env, SecureString password) {
                return new SecureString("password");
            }
        };
        final OptionSet option = command.getParser().parse("--keystore-password", "password", "--docker");
        when(enrollmentTokenGenerator.createKibanaEnrollmentToken(anyString(), any(SecureString.class))).thenReturn(token);
        when(enrollmentTokenGenerator.createNodeEnrollmentToken(anyString(), any(SecureString.class))).thenReturn(token);
        final String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "yellow")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        final String getChangeElasticUserPasswordBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .endObject();
            getChangeElasticUserPasswordBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("POST"), eq(command.changeElasticUserPasswordUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, getChangeElasticUserPasswordBody));
        command.execute(terminal, option, environment);
        assertThat(terminal.getOutput(), containsString("'elastic' user password: Aljngvodjb94j8HSY803"));
        assertThat(terminal.getOutput(), containsString("CA fingerprint: ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428" +
            "f8a91362d"));
        assertThat(terminal.getOutput(), containsString("Kibana enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wLjE" +
            "6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWUyNmI0ZDQyOGY4Y" +
            "TkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
        assertThat(terminal.getOutput(), containsString("Node enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wLjE" +
            "6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWUyNmI0ZDQyOGY4Y" +
            "TkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
    }

    public void testBootstrapPasswordSuccess() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final Path tempDir = createTempDir();
        secureSettings.setString("bootstrap.password", "password");
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.enrollment.enabled", "true")
            .setSecureSettings(secureSettings)
            .put("path.home", tempDir)
            .build();
        final Environment bootstrapPasswordEnvironment = new Environment(settings, tempDir);

        final BootstrapPasswordAndEnrollmentTokenForInitialNode command_util = mock(BootstrapPasswordAndEnrollmentTokenForInitialNode
            .class);
        final CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(bootstrapPasswordEnvironment);
        final MockTerminal terminal = new MockTerminal();
        final CommandLineHttpClient client = new CommandLineHttpClient(bootstrapPasswordEnvironment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        final EnrollmentTokenGenerator enrollmentTokenGenerator = mock(EnrollmentTokenGenerator.class);
        final EnrollmentToken token = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command = new BootstrapPasswordAndEnrollmentTokenForInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) {
                return client_util;
            }
            @Override
            protected EnrollmentTokenGenerator getEnrollmentTokenGenerator(Environment env) { return enrollmentTokenGenerator; }
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected SecureString readBootstrapPassword(Environment env, SecureString password) {
                return new SecureString("password");
            }
        };
        final OptionSet option = command.getParser().parse("--keystore-password", "password");
        when(enrollmentTokenGenerator.createKibanaEnrollmentToken(anyString(), any(SecureString.class))).thenReturn(token);
        final String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "yellow")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        final String getChangeElasticUserPasswordBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .endObject();
            getChangeElasticUserPasswordBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("POST"), eq(command.changeElasticUserPasswordUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, getChangeElasticUserPasswordBody));
        command.execute(terminal, option, bootstrapPasswordEnvironment);
        assertFalse(terminal.getOutput().contains("'elastic' user password:"));
        assertThat(terminal.getOutput(), containsString("CA fingerprint: ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428" +
            "f8a91362d"));
        assertThat(terminal.getOutput(), containsString("Kibana enrollment token: eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyJbMTkyLjE2OC4wLjE" +
            "6OTIwMSwgMTcyLjE2LjI1NC4xOjkyMDIiXSwiZmdyIjoiY2U0ODBkNTM3Mjg2MDU2NzRmY2ZkOGZmYjUxMDAwZDhhMzNiZjMyZGU3YzdmMWUyNmI0ZDQyOGY4Y" +
            "TkxMzYyZCIsImtleSI6IkRSNkN6WGtCRGY4YW1WXzQ4eVlYOngzWXFVX3JxUXdtLUVTcmtFeGNuT2cifQ=="));
        assertFalse(terminal.getOutput().contains("Node enrollment token: "));
    }

    public void testClusterHealthIsRed() throws Exception {
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command_util = mock(BootstrapPasswordAndEnrollmentTokenForInitialNode
            .class);
        final CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(environment);
        final Terminal terminal = new MockTerminal();
        final CommandLineHttpClient client = new CommandLineHttpClient(environment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        doCallRealMethod().when(client_util).checkClusterHealthWithRetriesWaitingForCluster(anyString(), anyObject(), anyInt());
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command = new BootstrapPasswordAndEnrollmentTokenForInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) { return client_util; }
            @Override
            protected SecureString readBootstrapPassword(Environment env, SecureString password) {
                return new SecureString("password");
            }
        };
        final OptionSet option = command.getParser().parse("--keystore-password", "password");
        final String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "red")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        final UserException ex = expectThrows(UserException.class, () ->
            command.execute(terminal, option, environment));
        assertNull(ex.getMessage());
    }

    public void testFailedToSetPassword() throws Exception {
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command_util = mock(BootstrapPasswordAndEnrollmentTokenForInitialNode
            .class);
        final CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(environment);
        final Terminal terminal = new MockTerminal();
        CommandLineHttpClient client = new CommandLineHttpClient(environment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        final EnrollmentTokenGenerator cet = mock(EnrollmentTokenGenerator.class);
        final EnrollmentToken token = new EnrollmentToken("DR6CzXkBDf8amV_48yYX:x3YqU_rqQwm-ESrkExcnOg",
            "ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", "8.0.0",
            Arrays.asList("[192.168.0.1:9201, 172.16.254.1:9202"));
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command = new BootstrapPasswordAndEnrollmentTokenForInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) {
                return client_util;
            }
            @Override
            protected EnrollmentTokenGenerator getEnrollmentTokenGenerator(Environment env) {
                return cet;
            }
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected SecureString readBootstrapPassword(Environment env, SecureString password) {
                return new SecureString("password");
            }
        };
        final OptionSet option = command.getParser().parse("--keystore-password", "password");
        when(cet.createKibanaEnrollmentToken(anyString(), any(SecureString.class))).thenReturn(token);
        final String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "yellow")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        final String getChangeElasticUserPasswordBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .endObject();
            getChangeElasticUserPasswordBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("POST"), eq(command.changeElasticUserPasswordUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_UNAUTHORIZED, getChangeElasticUserPasswordBody));
        final UserException ex = expectThrows(UserException.class, () ->
            command.execute(terminal, option, environment));
        assertNull(ex.getMessage());
    }

    public void testNoKeystorePassword() {
        final BootstrapPasswordAndEnrollmentTokenForInitialNode command = new BootstrapPasswordAndEnrollmentTokenForInitialNode();
        final Terminal terminal = mock(Terminal.class);
        final OptionSet option = command.getParser().parse(Strings.toStringArray(Collections.singletonList("")));
        final UserException ex = expectThrows(UserException.class, () ->
            command.execute(terminal, option, environment));
        assertNull(ex.getMessage());
    }

    private HttpResponse createHttpResponse(final int httpStatus, final String responseJson) throws IOException {
        final HttpResponse.HttpResponseBuilder builder = new HttpResponse.HttpResponseBuilder();
        builder.withHttpStatus(httpStatus);
        builder.withResponseBody(responseJson);
        return builder.build();
    }
}
