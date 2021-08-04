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
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.enrollment.CreateEnrollmentToken;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.elasticsearch.mock.orig.Mockito.doReturn;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PasswordAndEnrollmentInitialNodeTests extends ESTestCase {
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
        PasswordAndEnrollmentInitialNode command_util = mock(PasswordAndEnrollmentInitialNode.class);
        CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(environment);
        Terminal terminal = new MockTerminal();
        CommandLineHttpClient client = new CommandLineHttpClient(environment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        CreateEnrollmentToken createEnrollmentToken = mock(CreateEnrollmentToken.class);
        PasswordAndEnrollmentInitialNode command = new PasswordAndEnrollmentInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) {
                return client_util;
            }
            @Override
            protected CreateEnrollmentToken getCreateEnrollmentToken(Environment env) {
                return createEnrollmentToken;
            }
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected void ReadBootstrapPassword(Environment env, Terminal terminal) {
                bootstrapPassword = SecureSetting.secureString("bootstrap.password",
                    null).get(env.settings());
                credentialsPassword = SecureSetting.secureString("bootstrap.password",
                    KeyStoreWrapper.SEED_SETTING).get(env.settings());
            }
        };
        OptionSet option = command.getParser().parse("--explicitly-acknowledge-execution", "--docker");
        when(createEnrollmentToken.createKibanaEnrollmentToken(anyString(), any(SecureString.class))).thenReturn("eyJ2ZXIiOiI4LjAuMCIsIm" +
            "FkciI6WyIxOTIuMTY4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4M" +
            "GQ1MzcyODYwNTY3NGZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiwia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZc" +
            "VVfcnFRd20tRVNya0V4Y25PZyJ9");
        when(createEnrollmentToken.createNodeEnrollmentToken(anyString(), any(SecureString.class))).thenReturn("eyJ2ZXIiOiI4LjAuMCIsIm" +
            "FkciI6WyIxOTIuMTY4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4M" +
            "GQ1MzcyODYwNTY3NGZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiwia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZc" +
            "VVfcnFRd20tRVNya0V4Y25PZyJ9");
        String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "yellow")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        String getChangeElasticUserPasswordBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .endObject();
            getChangeElasticUserPasswordBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("POST"), eq(command.changeElasticUserPasswordUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, getChangeElasticUserPasswordBody));
        command.execute(terminal, option, environment);
        assertEquals(new SecureString("Aljngvodjb94j8HSY803"), command.getPassword());
        assertEquals("ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", command.getFingerprint());
        assertEquals("eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyIxOTIuMTY4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0" +
            "OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4MGQ1MzcyODYwNTY3NGZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiw" +
            "ia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZcVVfcnFRd20tRVNya0V4Y25PZyJ9", command.getKibanaToken());
        assertEquals("eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyIxOTIuMTY4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0" +
            "OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4MGQ1MzcyODYwNTY3NGZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiw" +
            "ia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZcVVfcnFRd20tRVNya0V4Y25PZyJ9", command.getNodeToken());
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

        PasswordAndEnrollmentInitialNode command_util = mock(PasswordAndEnrollmentInitialNode.class);
        CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(bootstrapPasswordEnvironment);
        Terminal terminal = new MockTerminal();
        CommandLineHttpClient client = new CommandLineHttpClient(bootstrapPasswordEnvironment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        CreateEnrollmentToken createEnrollmentToken = mock(CreateEnrollmentToken.class);
        PasswordAndEnrollmentInitialNode command = new PasswordAndEnrollmentInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) {
                return client_util;
            }
            @Override
            protected CreateEnrollmentToken getCreateEnrollmentToken(Environment env) { return createEnrollmentToken; }
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected void ReadBootstrapPassword(Environment env, Terminal terminal) {
                bootstrapPassword = SecureSetting.secureString("bootstrap.password",
                    null).get(env.settings());
                credentialsPassword = SecureSetting.secureString("bootstrap.password",
                    KeyStoreWrapper.SEED_SETTING).get(env.settings());
            }
        };
        OptionSet option = command.getParser().parse(Strings.toStringArray(Collections.singletonList(
            "--explicitly-acknowledge-execution")));
        when(createEnrollmentToken.createKibanaEnrollmentToken(anyString(), any(SecureString.class))).thenReturn("eyJ2ZXIiOiI4LjAuMCIsIm" +
            "FkciI6WyIxOTIuMTY4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4M" +
            "GQ1MzcyODYwNTY3NGZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiwia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZ" +
            "cVVfcnFRd20tRVNya0V4Y25PZyJ9");
        String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "yellow")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        String getChangeElasticUserPasswordBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .endObject();
            getChangeElasticUserPasswordBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("POST"), eq(command.changeElasticUserPasswordUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, getChangeElasticUserPasswordBody));
        command.execute(terminal, option, bootstrapPasswordEnvironment);
        assertEquals(new SecureString("password"), command.getPassword());
        assertEquals("ce480d53728605674fcfd8ffb51000d8a33bf32de7c7f1e26b4d428f8a91362d", command.getFingerprint());
        assertEquals("eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyIxOTIuMTY4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0" +
            "OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4MGQ1MzcyODYwNTY3NGZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiw" +
            "ia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZcVVfcnFRd20tRVNya0V4Y25PZyJ9", command.getKibanaToken());
    }

    public void testClusterHealthIsRed() throws Exception {
        PasswordAndEnrollmentInitialNode command_util = mock(PasswordAndEnrollmentInitialNode.class);
        CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(environment);
        Terminal terminal = new MockTerminal();
        CommandLineHttpClient client = new CommandLineHttpClient(environment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        PasswordAndEnrollmentInitialNode command = new PasswordAndEnrollmentInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) { return client_util; }
            @Override
            protected void ReadBootstrapPassword(Environment env, Terminal terminal) {
                bootstrapPassword = SecureSetting.secureString("bootstrap.password",
                    null).get(env.settings());
                credentialsPassword = SecureSetting.secureString("bootstrap.password",
                    KeyStoreWrapper.SEED_SETTING).get(env.settings());
            }
        };
        OptionSet option = command.getParser().parse(Strings.toStringArray(Collections.singletonList(
            "--explicitly-acknowledge-execution")));
        String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "red")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        UserException ex = expectThrows(UserException.class, () ->
            command.execute(terminal, option, environment));
        assertThat(ex.getMessage(), Matchers.containsString("Failed to determine the health of the cluster. Cluster health " +
            "is currently RED."));
    }

    public void testFailedToSetPassword() throws Exception {
        PasswordAndEnrollmentInitialNode command_util = mock(PasswordAndEnrollmentInitialNode.class);
        CommandLineHttpClient client_util = mock(CommandLineHttpClient.class);
        doReturn(client_util).when(command_util).getClient(environment);
        Terminal terminal = new MockTerminal();
        CommandLineHttpClient client = new CommandLineHttpClient(environment);
        doReturn(client.getDefaultURL()).when(client_util).getDefaultURL();
        CreateEnrollmentToken cet = mock(CreateEnrollmentToken.class);
        PasswordAndEnrollmentInitialNode command = new PasswordAndEnrollmentInitialNode() {
            @Override
            protected CommandLineHttpClient getClient(Environment env) {
                return client_util;
            }
            @Override
            protected CreateEnrollmentToken getCreateEnrollmentToken(Environment env) {
                return cet;
            }
            @Override
            protected char[] generatePassword(int passwordLength) {
                String password = "Aljngvodjb94j8HSY803";
                return password.toCharArray();
            }
            @Override
            protected void ReadBootstrapPassword(Environment env, Terminal terminal) {
                bootstrapPassword = SecureSetting.secureString("bootstrap.password",
                    null).get(env.settings());
                credentialsPassword = SecureSetting.secureString("bootstrap.password",
                    KeyStoreWrapper.SEED_SETTING).get(env.settings());
            }
        };
        OptionSet option = command.getParser().parse(Strings.toStringArray(Collections.singletonList(
            "--explicitly-acknowledge-execution")));
        when(cet.createKibanaEnrollmentToken(anyString(), any(SecureString.class))).thenReturn("eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyIxOTIuMTY" +
            "4LjAuMTo5MjAxIiwiMTcyLjE2LjI1NC4xOjkyMDIiLCJbMjAwMTpkYjg6MDoxMjM0OjA6NTY3Ojg6MV06OTIwMyJdLCJmZ3IiOiJjZTQ4MGQ1MzcyODYwNTY3NG" +
            "ZjZmQ4ZmZiNTEwMDBkOGEzM2JmMzJkZTdjN2YxZTI2YjRkNDI4ZjhhOTEzNjJkIiwia2V5IjoiRFI2Q3pYa0JEZjhhbVZfNDh5WVg6eDNZcVVfcnFRd20tRVNya" +
            "0V4Y25PZyJ9");
        String checkClusterHealthResponseBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .field("status", "yellow")
                .endObject();
            checkClusterHealthResponseBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("GET"), eq(command.checkClusterHealthUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_OK, checkClusterHealthResponseBody));

        String getChangeElasticUserPasswordBody;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.startObject()
                .endObject();
            getChangeElasticUserPasswordBody = Strings.toString(builder);
        }
        when(client_util.execute(eq("POST"), eq(command.changeElasticUserPasswordUrl(client)), anyString(), any(SecureString.class),
            any(), any())).thenReturn(createHttpResponse(HttpURLConnection.HTTP_UNAUTHORIZED, getChangeElasticUserPasswordBody));
        UserException ex = expectThrows(UserException.class, () ->
            command.execute(terminal, option, environment));
        assertThat(ex.getMessage(), Matchers.containsString("Failed to set password for user [elastic]"));
    }

    public void testNoExplicitAcknowledgment() {
        PasswordAndEnrollmentInitialNode command = new PasswordAndEnrollmentInitialNode();
        Terminal terminal = mock(Terminal.class);
        OptionSet option = command.getParser().parse(Strings.toStringArray(Collections.singletonList("")));
        UserException ex = expectThrows(UserException.class, () ->
            command.execute(terminal, option, environment));
        assertThat(ex.getMessage(), Matchers.containsString("This command is not intended for end users"));
    }

    private HttpResponse createHttpResponse(final int httpStatus, final String responseJson) throws IOException {
        HttpResponse.HttpResponseBuilder builder = new HttpResponse.HttpResponseBuilder();
        builder.withHttpStatus(httpStatus);
        builder.withResponseBody(responseJson);
        return builder.build();
    }
}
