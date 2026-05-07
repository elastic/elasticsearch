/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.TestTrustStore;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class MicrosoftGraphAuthzPluginIT extends ESRestTestCase {

    private static final String TENANT_ID = System.getProperty("test.ms_graph.tenant_id");
    private static final String CLIENT_ID = System.getProperty("test.ms_graph.client_id");
    private static final String CLIENT_SECRET = System.getProperty("test.ms_graph.client_secret");
    private static final String USERNAME = System.getProperty("test.ms_graph.username");
    private static final String EXPECTED_GROUP = System.getProperty("test.ms_graph.group_id");
    private static final Boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.ms_graph.fixture"));

    private static final List<MicrosoftGraphHttpFixture.TestUser> TEST_USERS = List.of(
        new MicrosoftGraphHttpFixture.TestUser(
            USERNAME,
            "Thor Odinson",
            "thor@oldap.test.elasticsearch.com",
            List.of("unmapped-group-1", "unmapped-group-2", "unmapped-group-3", EXPECTED_GROUP),
            List.of("microsoft_graph_user")
        ),
        new MicrosoftGraphHttpFixture.TestUser(
            "User2",
            "User 2",
            "user2@example.com",
            List.of(EXPECTED_GROUP),
            List.of("microsoft_graph_user")
        ),
        new MicrosoftGraphHttpFixture.TestUser("User3", "User 3", "user3@example.com", List.of(), List.of())
    );

    private static final MicrosoftGraphHttpFixture graphFixture = new MicrosoftGraphHttpFixture(
        TENANT_ID,
        CLIENT_ID,
        CLIENT_SECRET,
        TEST_USERS,
        3
    );

    public static ElasticsearchCluster cluster = initTestCluster();

    private static final TestTrustStore trustStore = new TestTrustStore(
        () -> MicrosoftGraphAuthzPluginIT.class.getClassLoader().getResourceAsStream("server/cert.pem")
    );

    @ClassRule
    public static TestRule ruleChain = USE_FIXTURE
        ? RuleChain.outerRule(graphFixture).around(trustStore).around(cluster)
        : RuleChain.outerRule(cluster);

    private static final String JWT_ISSUER = "test-issuer";
    private static final String JWT_AUDIENCE = "test-audience";
    private static final String JWT_HMAC_PASSPHRASE = "test-HMAC/secret passphrase-value-microsoft-graph-authz";

    private static ElasticsearchCluster initTestCluster() {
        final var clusterBuilder = ElasticsearchCluster.local()
            .module("analysis-common")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.http.ssl.enabled", "false")
            .plugin("microsoft-graph-authz")
            .keystore("bootstrap.password", "x-pack-test-password")
            .user("test_admin", "x-pack-test-password", User.ROOT_USER_ROLE, true)
            .user("rest_test", "rest_password", User.ROOT_USER_ROLE, true)
            .setting("xpack.security.authc.realms.jwt.jwt1.order", "1")
            .setting("xpack.security.authc.realms.jwt.jwt1.allowed_issuer", JWT_ISSUER)
            .setting("xpack.security.authc.realms.jwt.jwt1.allowed_audiences", JWT_AUDIENCE)
            .setting("xpack.security.authc.realms.jwt.jwt1.allowed_signature_algorithms", "HS256")
            .setting("xpack.security.authc.realms.jwt.jwt1.claims.principal", "sub")
            .setting("xpack.security.authc.realms.jwt.jwt1.client_authentication.type", "NONE")
            .setting("xpack.security.authc.realms.jwt.jwt1.authorization_realms", "microsoft_graph1")
            .keystore("xpack.security.authc.realms.jwt.jwt1.hmac_key", JWT_HMAC_PASSPHRASE)
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.order", "2")
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.client_id", CLIENT_ID)
            .keystore("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.client_secret", CLIENT_SECRET)
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.tenant_id", TENANT_ID)
            .setting("logger.org.elasticsearch.xpack.security.authc", "DEBUG")
            .setting("logger.org.elasticsearch.xpack.security.authz.microsoft", "TRACE")
            .setting("logger.com.microsoft", "TRACE")
            .setting("logger.com.azure", "TRACE");

        if (USE_FIXTURE) {
            clusterBuilder.setting(
                "xpack.security.authc.realms.microsoft_graph.microsoft_graph1.graph_host",
                () -> graphFixture.getBaseUrl() + "/v1.0"
            )
                .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.access_token_host", graphFixture::getBaseUrl)
                .systemProperty("javax.net.ssl.trustStore", () -> trustStore.getTrustStorePath().toString())
                .systemProperty("javax.net.ssl.trustStoreType", "jks")
                .systemProperty("tests.azure.credentials.disable_instance_discovery", "true");
        }

        return clusterBuilder.build();
    }

    @Before
    public void setupRoleMapping() throws IOException {
        Request request = new Request("PUT", "/_security/role_mapping/thor-kibana");
        request.setJsonEntity(
            Strings.toString(
                XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .array("roles", new String[] { "microsoft_graph_user" })
                    .field("enabled", true)
                    .startObject("rules")
                    .startArray("all")
                    .startObject()
                    .startObject("field")
                    .field("realm.name", "microsoft_graph1")
                    .endObject()
                    .endObject()
                    .startObject()
                    .startObject("field")
                    .field("groups", EXPECTED_GROUP)
                    .endObject()
                    .endObject()
                    .endArray() // "all"
                    .endObject() // "rules"
                    .endObject()
            )
        );
        adminClient().performRequest(request);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getProtocol() {
        return "http";
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("rest_test", new SecureString("rest_password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        super.configureClient(builder, settings);

        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setSocketTimeout(-1);
            return requestConfigBuilder;
        });
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    public void testAuthenticationSuccessful() throws Exception {
        final var listener = new PlainActionFuture<Map<String, Object>>();
        authenticateWithMicrosoftGraphAuthz(USERNAME, listener);
        final var resp = listener.get();
        assertThat(resp.get("username"), equalTo(USERNAME));
        assertThat(ObjectPath.<List<String>>evaluate(resp, "roles"), contains("microsoft_graph_user"));
        assertThat(ObjectPath.evaluate(resp, "authentication_realm.name"), equalTo("jwt1"));
    }

    public void testConcurrentAuthentication() throws Exception {
        assumeTrue("This needs the test server as the real account only has one user configured", USE_FIXTURE);
        final var concurrentLogins = 3;

        final var resultsListener = new PlainActionFuture<Collection<Map<String, Object>>>();
        final var groupedListener = new GroupedActionListener<>(TEST_USERS.size() * concurrentLogins, resultsListener);
        for (int i = 0; i < concurrentLogins; i++) {
            for (var user : TEST_USERS) {
                authenticateWithMicrosoftGraphAuthz(user.username(), groupedListener);
            }
        }
        final var allResponses = resultsListener.get();

        assertThat(allResponses.size(), equalTo(TEST_USERS.size() * concurrentLogins));
        for (var user : TEST_USERS) {
            var userResponses = allResponses.stream().filter(r -> r.get("username").equals(user.username())).toList();
            assertThat(userResponses.size(), equalTo(concurrentLogins));

            for (var response : userResponses) {
                assertThat(ObjectPath.evaluate(response, "roles"), equalTo(user.roles()));
                assertThat(ObjectPath.evaluate(response, "authentication_realm.name"), equalTo("jwt1"));
            }
        }
    }

    private static String buildSignedJwt(String principal) throws JOSEException {
        final Instant now = Instant.now();
        final JWTClaimsSet claims = new JWTClaimsSet.Builder().issuer(JWT_ISSUER)
            .audience(JWT_AUDIENCE)
            .subject(principal)
            .issueTime(Date.from(now))
            .expirationTime(Date.from(now.plusSeconds(600)))
            .build();
        final SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        final OctetSequenceKey hmacKey = new OctetSequenceKey.Builder(JWT_HMAC_PASSPHRASE.getBytes(StandardCharsets.UTF_8)).build();
        jwt.sign(new MACSigner(hmacKey));
        return jwt.serialize();
    }

    private void authenticateWithMicrosoftGraphAuthz(String principal, ActionListener<Map<String, Object>> listener) throws JOSEException {
        final String jwt = buildSignedJwt(principal);
        final var req = new Request("GET", "/_security/_authenticate");
        req.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + jwt));
        client().performRequestAsync(req, ActionTestUtils.wrapAsRestResponseListener(listener.map(ESRestTestCase::entityAsMap)));
    }

}
