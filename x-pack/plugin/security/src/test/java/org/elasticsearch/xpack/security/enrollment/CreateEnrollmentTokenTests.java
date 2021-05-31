/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Base64;
import java.util.Collections;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateEnrollmentTokenTests  extends ESTestCase {
    private ApiKeyGenerator apiKeyGenerator;
    private SecurityContext securityContext;
    private Environment environment;
    private NodeService nodeService;
    private SSLService sslService;
    private BoundTransportAddress dummyBoundTransportAddress;
    private SecureString key;
    private Instant now;
    private CreateEnrollmentToken createEnrollmentToken;

    @Before
    public void setupMocks() throws Exception {
        final Clock clock = Clock.systemUTC();
        now = clock.instant();
        environment = mock(Environment.class);
        final Path tempDir = createTempDir();
        final Path httpCaPath = tempDir.resolve("httpCa.p12");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/action/enrollment/httpCa.p12"), httpCaPath);
        when(environment.configFile()).thenReturn(tempDir);
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put( "xpack.security.authc.api_key.enabled", true)
            .put("keystore.path", "httpCa.p12")
            .put("keystore.password", "password")
            .build();
        sslService = mock(SSLService.class);
        final SSLConfiguration sslConfiguration = new SSLConfiguration(settings);
        when(sslService.getHttpTransportSSLConfiguration()).thenReturn(sslConfiguration);

        Authentication authentication =
            new Authentication(new User("joe", "manage_enrollment"),
                new Authentication.RealmRef("test", "test", "node"), null);
        securityContext = mock(SecurityContext.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);

        nodeService = mock(NodeService.class);
        dummyBoundTransportAddress = new BoundTransportAddress(
            new TransportAddress[]{buildNewFakeTransportAddress()}, buildNewFakeTransportAddress());
        NodeInfo nodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            new DiscoveryNode("test_node", buildNewFakeTransportAddress(), emptyMap(), emptySet(), VersionUtils.randomVersion(random())),
            null,
            null,
            null,
            null,
            null,
            null,
            new HttpInfo(dummyBoundTransportAddress, randomNonNegativeLong()),
            null,
            null,
            null,
            null);
        doReturn(nodeInfo).when(nodeService).info(false, false, false, false, false, false,
            true, false, false, false, false);

        final TransportService transportService = new TransportService(settings,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet());

        apiKeyGenerator = mock(ApiKeyGenerator.class);
        key = new SecureString(randomAlphaOfLength(18).toCharArray());
        final CreateApiKeyResponse createApiKeyResponse = new CreateApiKeyResponse(randomAlphaOfLengthBetween(6, 32),
            randomAlphaOfLength(12), key, now.plusMillis(CreateEnrollmentToken.ENROLL_API_KEY_EXPIRATION_SEC*1000));

        Mockito.doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(3));

            assertThat(args[0], equalTo(authentication));

            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) args[args.length - 1];
            listener.onResponse(createApiKeyResponse);

            return null;
        }).when(apiKeyGenerator).generateApiKey(any(Authentication.class), any(CreateApiKeyRequest.class), any(ActionListener.class));

        createEnrollmentToken = new CreateEnrollmentToken(apiKeyGenerator, securityContext, environment, nodeService, sslService);
    }

    public void testCreate() {
        try {
            String token = createEnrollmentToken.create();

            Map<String, String> info = getDecoded(token);
            assertEquals("0", info.get("v"));
            assertEquals(dummyBoundTransportAddress.publishAddress().toString(), info.get("adr"));
            assertEquals("598a35cd831ee6bb90e79aa80d6b073cda88b41d", info.get("fgr"));
            assertEquals(key.toString(), info.get("key"));
        } catch (Exception e) {
            logger.info("failed to create enrollment token ", e);
            fail("failed to create enrollment token");
        }
    }

    private Map<String, String> getDecoded(String token) throws IOException {
        String jsonString = new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            Map<String, Object> info = parser.map();
            assertNotEquals(info, null);
            return info.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
        }
    }
}
