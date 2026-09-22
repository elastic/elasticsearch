/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.exception.SdkClientException;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.datasource.s3.EsqlContainerCredentialsProvider.POD_IDENTITY_TOKEN_FILE_LOCATION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * Activation matrix and credential-exchange tests for {@link EsqlContainerCredentialsProvider}.
 */
@SuppressForbidden(reason = "test fixture uses HttpServer to emulate the EKS Pod Identity credentials endpoint")
public class EsqlContainerCredentialsProviderTests extends ESTestCase {

    private static final String TOKEN_CONTENTS = "test-pod-identity-token";

    private Environment environment;
    private ResourceWatcherService resourceWatcherService;
    private HttpServer credentialsServer;
    private final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();

    @Before
    public void initEnvironment() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        environment = TestEnvironment.newEnvironment(settings);
        Files.createDirectories(environment.configDir().resolve("esql-datasource-s3"));
        resourceWatcherService = mock(ResourceWatcherService.class);
    }

    @After
    public void stopCredentialsServer() {
        if (credentialsServer != null) {
            credentialsServer.stop(0);
            credentialsServer = null;
        }
    }

    public void testInactiveWhenTokenFileEnvUnset() throws IOException {
        try (
            EsqlContainerCredentialsProvider provider = new EsqlContainerCredentialsProvider(
                environment,
                resourceWatcherService,
                env(Map.of(SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable(), "http://127.0.0.1/creds"))
            )
        ) {
            assertFalse(provider.isActive());
            assertFalse(provider.isMisconfigured());
        }
    }

    public void testInactiveWhenCredentialsUriEnvUnset() throws IOException {
        try (
            EsqlContainerCredentialsProvider provider = new EsqlContainerCredentialsProvider(
                environment,
                resourceWatcherService,
                env(Map.of(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable(), "/var/run/secrets/token"))
            )
        ) {
            assertFalse(provider.isActive());
            assertFalse(provider.isMisconfigured());
        }
    }

    public void testAMissingTokenFileNamesTheFileToCreate() throws IOException {
        String before = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        try (
            EsqlContainerCredentialsProvider provider = new EsqlContainerCredentialsProvider(
                environment,
                resourceWatcherService,
                podIdentityEnv("http://127.0.0.1:1/creds")
            )
        ) {
            assertFalse(provider.isActive());
            assertTrue(provider.isMisconfigured());
            assertThat(provider.misconfigurationMessage(), containsString(POD_IDENTITY_TOKEN_FILE_LOCATION));
            assertThat(
                "must not write the JVM-wide token property when the entitled symlink is missing",
                System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property()),
                equalTo(before)
            );
        }
    }

    public void testPodIdentityCredentialsComeFromTheEntitledTokenFile() throws Exception {
        Path tokenFile = environment.configDir().resolve(POD_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFile, TOKEN_CONTENTS);
        startCredentialsServer();

        String before = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        try (
            EsqlContainerCredentialsProvider provider = new EsqlContainerCredentialsProvider(
                environment,
                resourceWatcherService,
                podIdentityEnv(credentialsUri())
            )
        ) {
            assertTrue(provider.isActive());
            assertFalse(provider.isMisconfigured());
            AwsCredentials credentials = provider.resolveCredentials();
            assertThat(credentials, instanceOf(AwsSessionCredentials.class));
            AwsSessionCredentials session = (AwsSessionCredentials) credentials;
            assertThat(session.accessKeyId(), equalTo("AKIATEST"));
            assertThat(session.secretAccessKey(), equalTo("secret"));
            assertThat(session.sessionToken(), equalTo("session-token"));
            assertThat(lastAuthorizationHeader.get(), equalTo(TOKEN_CONTENTS));
            assertThat(
                "must not write the JVM-wide token property",
                System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property()),
                equalTo(before)
            );
        }
    }

    public void testRejectsNonLoopbackHttpCredentialsUri() {
        SdkClientException e = expectThrows(
            SdkClientException.class,
            () -> EsqlContainerCredentialsProvider.validateCredentialsEndpoint(URI.create("http://example.com/creds"))
        );
        assertThat(e.getMessage(), containsString("invalid host"));
    }

    public void testAllowsLoopbackHttpCredentialsUri() {
        assertEquals(
            URI.create("http://127.0.0.1:18100/creds"),
            EsqlContainerCredentialsProvider.validateCredentialsEndpoint(URI.create("http://127.0.0.1:18100/creds"))
        );
    }

    public void testMalformedCredentialsUriDoesNotFailConstruction() throws IOException {
        Path tokenFile = environment.configDir().resolve(POD_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFile, TOKEN_CONTENTS);
        // URI.create throws on spaces; storing the raw string at construction must not.
        try (
            EsqlContainerCredentialsProvider provider = new EsqlContainerCredentialsProvider(
                environment,
                resourceWatcherService,
                podIdentityEnv("not a uri")
            )
        ) {
            assertTrue(provider.isActive());
            expectThrows(Exception.class, provider::resolveCredentials);
        }
    }

    private void startCredentialsServer() throws IOException {
        credentialsServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        credentialsServer.createContext("/creds", exchange -> {
            try (exchange) {
                lastAuthorizationHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
                String body = String.format(
                    Locale.ROOT,
                    "{\"AccessKeyId\":\"AKIATEST\",\"SecretAccessKey\":\"secret\",\"Token\":\"session-token\",\"Expiration\":\"%s\"}",
                    Instant.now().plus(15, ChronoUnit.MINUTES)
                );
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });
        credentialsServer.start();
    }

    private String credentialsUri() {
        InetSocketAddress addr = credentialsServer.getAddress();
        return "http://" + InetAddresses.toUriString(addr.getAddress()) + ":" + addr.getPort() + "/creds";
    }

    private Function<String, String> podIdentityEnv(String credentialsUri) {
        Map<String, String> map = new HashMap<>();
        map.put(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable(), "/var/run/secrets/token");
        map.put(SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable(), credentialsUri);
        return env(map);
    }

    private static Function<String, String> env(Map<String, String> values) {
        return values::get;
    }
}
