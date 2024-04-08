/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CustomWebIdentityTokenCredentialsProviderTests extends ESTestCase {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/FederatedWebIdentityRole";
    private static final String ROLE_NAME = "aws-sdk-java-1651084775908";
    private final TestThreadPool threadPool = new TestThreadPool("test");
    private final Settings settings = Settings.builder().put("resource.reload.interval.low", TimeValue.timeValueMillis(100)).build();
    private final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);

    @After
    public void shutdown() throws Exception {
        resourceWatcherService.close();
        threadPool.shutdown();
    }

    private static Environment getEnvironment() throws IOException {
        Path configDirectory = createTempDir("web-identity-token-test");
        Files.createDirectory(configDirectory.resolve("repository-s3"));
        Files.writeString(configDirectory.resolve("repository-s3/aws-web-identity-token-file"), "YXdzLXdlYi1pZGVudGl0eS10b2tlbi1maWxl");
        Environment environment = Mockito.mock(Environment.class);
        Mockito.when(environment.configFile()).thenReturn(configDirectory);
        return environment;
    }

    @SuppressForbidden(reason = "HTTP server is used for testing")
    private static HttpServer getHttpServer(Consumer<String> webIdentityTokenCheck) throws IOException {
        HttpServer httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);
        httpServer.createContext("/", exchange -> {
            try (exchange) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Map<String, String> params = Arrays.stream(body.split("&"))
                    .map(e -> e.split("="))
                    .collect(Collectors.toMap(e -> e[0], e -> URLDecoder.decode(e[1], StandardCharsets.UTF_8)));
                assertEquals(ROLE_NAME, params.get("RoleSessionName"));
                webIdentityTokenCheck.accept(params.get("WebIdentityToken"));

                exchange.getResponseHeaders().add("Content-Type", "text/xml; charset=UTF-8");
                byte[] response = Strings.format(
                    """
                        <AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                          <AssumeRoleWithWebIdentityResult>
                            <SubjectFromWebIdentityToken>amzn1.account.AF6RHO7KZU5XRVQJGXK6HB56KR2A</SubjectFromWebIdentityToken>
                            <Audience>client.5498841531868486423.1548@apps.example.com</Audience>
                            <AssumedRoleUser>
                              <Arn>%s</Arn>
                              <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:%s</AssumedRoleId>
                            </AssumedRoleUser>
                            <Credentials>
                              <SessionToken>sts_session_token</SessionToken>
                              <SecretAccessKey>secret_access_key</SecretAccessKey>
                              <Expiration>%s</Expiration>
                              <AccessKeyId>sts_access_key</AccessKeyId>
                            </Credentials>
                            <SourceIdentity>SourceIdentityValue</SourceIdentity>
                            <Provider>www.amazon.com</Provider>
                          </AssumeRoleWithWebIdentityResult>
                          <ResponseMetadata>
                            <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>
                          </ResponseMetadata>
                        </AssumeRoleWithWebIdentityResponse>
                        """,
                    ROLE_ARN,
                    ROLE_NAME,
                    ZonedDateTime.now().plusDays(1L).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
                ).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
            }
        });
        httpServer.start();
        return httpServer;
    }

    @SuppressForbidden(reason = "HTTP server is used for testing")
    private static Map<String, String> getSystemProperties(HttpServer httpServer) {
        return Map.of(
            "com.amazonaws.sdk.stsMetadataServiceEndpointOverride",
            "http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort()
        );
    }

    private static Map<String, String> environmentVariables() {
        return Map.of("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/eks.amazonaws.com/serviceaccount/token", "AWS_ROLE_ARN", ROLE_ARN);
    }

    private static void assertCredentials(AWSCredentials credentials) {
        Assert.assertFalse(credentials.getAWSAccessKeyId().isEmpty());
        Assert.assertFalse(credentials.getAWSSecretKey().isEmpty());
    }

    @SuppressForbidden(reason = "HTTP server is used for testing")
    public void testCreateWebIdentityTokenCredentialsProvider() throws Exception {
        HttpServer httpServer = getHttpServer(s -> assertEquals("YXdzLXdlYi1pZGVudGl0eS10b2tlbi1maWxl", s));

        Environment environment = getEnvironment();

        // No region is set, but the SDK shouldn't fail because of that
        Map<String, String> environmentVariables = environmentVariables();
        Map<String, String> systemProperties = getSystemProperties(httpServer);
        var webIdentityTokenCredentialsProvider = new S3Service.CustomWebIdentityTokenCredentialsProvider(
            environment,
            environmentVariables::get,
            systemProperties::getOrDefault,
            Clock.fixed(Instant.ofEpochMilli(1651084775908L), ZoneOffset.UTC),
            resourceWatcherService
        );
        try {
            AWSCredentials credentials = S3Service.buildCredentials(
                LogManager.getLogger(S3Service.class),
                S3ClientSettings.getClientSettings(Settings.EMPTY, randomAlphaOfLength(8)),
                webIdentityTokenCredentialsProvider
            ).getCredentials();

            assertCredentials(credentials);
        } finally {
            webIdentityTokenCredentialsProvider.shutdown();
            httpServer.stop(0);
        }
    }

    private static class DelegatingConsumer implements Consumer<String> {
        private Consumer<String> delegate;

        private DelegatingConsumer(Consumer<String> delegate) {
            this.delegate = delegate;
        }

        private void setDelegate(Consumer<String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void accept(String s) {
            delegate.accept(s);
        }
    }

    @SuppressForbidden(reason = "HTTP server is used for testing")
    public void testPickUpNewWebIdentityTokenWhenItsChanged() throws Exception {
        DelegatingConsumer webIdentityTokenCheck = new DelegatingConsumer(s -> assertEquals("YXdzLXdlYi1pZGVudGl0eS10b2tlbi1maWxl", s));

        HttpServer httpServer = getHttpServer(webIdentityTokenCheck);
        Environment environment = getEnvironment();
        Map<String, String> environmentVariables = environmentVariables();
        Map<String, String> systemProperties = getSystemProperties(httpServer);
        var webIdentityTokenCredentialsProvider = new S3Service.CustomWebIdentityTokenCredentialsProvider(
            environment,
            environmentVariables::get,
            systemProperties::getOrDefault,
            Clock.fixed(Instant.ofEpochMilli(1651084775908L), ZoneOffset.UTC),
            resourceWatcherService
        );
        try {
            AWSCredentialsProvider awsCredentialsProvider = S3Service.buildCredentials(
                LogManager.getLogger(S3Service.class),
                S3ClientSettings.getClientSettings(Settings.EMPTY, randomAlphaOfLength(8)),
                webIdentityTokenCredentialsProvider
            );
            assertCredentials(awsCredentialsProvider.getCredentials());

            var latch = new CountDownLatch(1);
            String newWebIdentityToken = "88f84342080d4671a511e10ae905b2b0";
            webIdentityTokenCheck.setDelegate(s -> {
                if (s.equals(newWebIdentityToken)) {
                    latch.countDown();
                }
            });
            Files.writeString(environment.configFile().resolve("repository-s3/aws-web-identity-token-file"), newWebIdentityToken);

            safeAwait(latch);
            assertCredentials(awsCredentialsProvider.getCredentials());
        } finally {
            webIdentityTokenCredentialsProvider.shutdown();
            httpServer.stop(0);
        }
    }

    public void testSupportRegionalizedEndpoints() throws Exception {
        Map<String, String> environmentVariables = Map.of(
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
            "AWS_ROLE_ARN",
            ROLE_ARN,
            "AWS_STS_REGIONAL_ENDPOINTS",
            "regional",
            "AWS_REGION",
            "us-west-2"
        );
        Map<String, String> systemProperties = Map.of();

        var webIdentityTokenCredentialsProvider = new S3Service.CustomWebIdentityTokenCredentialsProvider(
            getEnvironment(),
            environmentVariables::get,
            systemProperties::getOrDefault,
            Clock.systemUTC(),
            resourceWatcherService
        );
        // We can't verify that webIdentityTokenCredentialsProvider's STS client uses the "https://sts.us-west-2.amazonaws.com"
        // endpoint in a unit test. The client depends on hardcoded RegionalEndpointsOptionResolver that in turn depends
        // on the system environment that we can't change in the test. So we just verify we that we called `withRegion`
        // on stsClientBuilder which should internally correctly configure the endpoint when the STS client is built.
        assertEquals("us-west-2", webIdentityTokenCredentialsProvider.getStsRegion());

        webIdentityTokenCredentialsProvider.shutdown();
    }
}
