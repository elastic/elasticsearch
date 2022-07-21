/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSCredentials;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.mockito.Mockito;

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
import java.util.stream.Collectors;

public class CustomWebIdentityTokenCredentialsProviderTests extends ESTestCase {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/FederatedWebIdentityRole";
    private static final String ROLE_NAME = "aws-sdk-java-1651084775908";

    @SuppressForbidden(reason = "HTTP server is used for testing")
    public void testCreateWebIdentityTokenCredentialsProvider() throws Exception {
        HttpServer httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);
        httpServer.createContext("/", exchange -> {
            try (exchange) {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Map<String, String> params = Arrays.stream(body.split("&"))
                    .map(e -> e.split("="))
                    .collect(Collectors.toMap(e -> e[0], e -> URLDecoder.decode(e[1], StandardCharsets.UTF_8)));
                assertEquals(ROLE_NAME, params.get("RoleSessionName"));

                exchange.getResponseHeaders().add("Content-Type", "text/xml; charset=UTF-8");
                byte[] response = """
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
                    """.formatted(
                    ROLE_ARN,
                    ROLE_NAME,
                    ZonedDateTime.now().plusDays(1L).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
                ).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
            }
        });
        httpServer.start();

        Path configDirectory = Files.createTempDirectory("web-identity-token-test");
        Files.createDirectory(configDirectory.resolve("repository-s3"));
        Files.writeString(configDirectory.resolve("repository-s3/aws-web-identity-token-file"), "YXdzLXdlYi1pZGVudGl0eS10b2tlbi1maWxl");
        Environment environment = Mockito.mock(Environment.class);
        Mockito.when(environment.configFile()).thenReturn(configDirectory);

        // No region is set, but the SDK shouldn't fail because of that
        Map<String, String> environmentVariables = Map.of(
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
            "AWS_ROLE_ARN",
            ROLE_ARN
        );
        Map<String, String> systemProperties = Map.of(
            "com.amazonaws.sdk.stsMetadataServiceEndpointOverride",
            "http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort()
        );
        var webIdentityTokenCredentialsProvider = new S3Service.CustomWebIdentityTokenCredentialsProvider(
            environment,
            environmentVariables::get,
            systemProperties::getOrDefault,
            Clock.fixed(Instant.ofEpochMilli(1651084775908L), ZoneOffset.UTC)
        );
        try {
            AWSCredentials credentials = S3Service.buildCredentials(
                LogManager.getLogger(S3Service.class),
                S3ClientSettings.getClientSettings(Settings.EMPTY, randomAlphaOfLength(8)),
                webIdentityTokenCredentialsProvider
            ).getCredentials();

            Assert.assertEquals("sts_access_key", credentials.getAWSAccessKeyId());
            Assert.assertEquals("secret_access_key", credentials.getAWSSecretKey());
        } finally {
            webIdentityTokenCredentialsProvider.shutdown();
            httpServer.stop(0);
        }
    }
}
