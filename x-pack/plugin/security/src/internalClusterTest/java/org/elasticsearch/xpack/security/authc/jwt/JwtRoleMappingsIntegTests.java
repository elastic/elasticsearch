/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;
import static org.elasticsearch.xpack.security.authc.jwt.JwtRealmSingleNodeTests.getAuthenticateRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public final class JwtRoleMappingsIntegTests extends SecurityIntegTestCase {

    private final String jwt0SharedSecret = "jwt0_shared_secret";
    private final String jwt1SharedSecret = "jwt1_shared_secret";
    private final String jwtHmacKey = "test-HMAC/secret passphrase-value";
    private static boolean anonymousRole;

    @BeforeClass
    public static void beforeTests() {
        anonymousRole = randomBoolean();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MustachePlugin.class);
        return List.copyOf(plugins);
    }

    @Before
    private void clearRoleMappings() throws InterruptedException {
        publishRoleMappings(Set.of());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), randomBoolean())
            // 1st JWT realm
            .put("xpack.security.authc.realms.jwt.jwt0.order", 10)
            .put(
                randomBoolean()
                    ? Settings.builder().put("xpack.security.authc.realms.jwt.jwt0.token_type", "id_token").build()
                    : Settings.EMPTY
            )
            .put("xpack.security.authc.realms.jwt.jwt0.allowed_issuer", "my-issuer-01")
            .put("xpack.security.authc.realms.jwt.jwt0.allowed_audiences", "es-01")
            .put("xpack.security.authc.realms.jwt.jwt0.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt0.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt0.client_authentication.type", "shared_secret")
            .putList("xpack.security.authc.realms.jwt.jwt0.allowed_signature_algorithms", "HS256", "HS384")
            // 2nd JWT realm
            .put("xpack.security.authc.realms.jwt.jwt1.order", 20)
            .put("xpack.security.authc.realms.jwt.jwt1.token_type", "access_token")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_issuer", "my-issuer-02")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_subjects", "user-02")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_audiences", "es-02")
            .put("xpack.security.authc.realms.jwt.jwt1.fallback_claims.sub", "client_id")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.principal", "appId")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt1.client_authentication.type", "shared_secret")
            .putList("xpack.security.authc.realms.jwt.jwt1.allowed_signature_algorithms", "HS256", "HS384");
        if (anonymousRole) {
            builder.put("xpack.security.authc.anonymous.roles", "testAnonymousRole");
        }
        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt0.hmac_key", jwtHmacKey);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt0.client_authentication.shared_secret", jwt0SharedSecret);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt1.hmac_key", jwtHmacKey);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt1.client_authentication.shared_secret", jwt1SharedSecret);
        });
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testUsernameRoleMappingForJWT() throws Exception {
        String username1 = "me";
        String username2 = "someoneElse";
        String roleName = randomAlphaOfLength(8);
        // role mapping for username1
        ExpressionRoleMapping mapping1 = new ExpressionRoleMapping(
            "test-username-expression",
            new FieldExpression("username", List.of(new FieldExpression.FieldValue(username1))),
            List.of(roleName),
            List.of(),
            Map.of(),
            true
        );
        publishRoleMappings(Set.of(mapping1));
        // JWT "id_token" valid for jwt0
        // jwt for username1
        SignedJWT username1Jwt = getSignedJWT(
            new JWTClaimsSet.Builder().audience("es-01")
                .issuer("my-issuer-01")
                .subject(username1)
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                .build()
        );
        // jwt for username2
        // JWT "id_token" valid for jwt0
        SignedJWT username2Jwt = getSignedJWT(
            new JWTClaimsSet.Builder().audience("es-01")
                .issuer("my-issuer-01")
                .subject(username2)
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                .build()
        );
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(username1Jwt, jwt0SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            if (anonymousRole) {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo(roleName), equalTo("testAnonymousRole"))
                );
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo(roleName)));
            }
        }
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(username2Jwt, jwt0SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            if (anonymousRole) {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("testAnonymousRole")));
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), emptyIterable());
            }
        }
        // role mapping for username2
        if (randomBoolean()) {
            // overwrite the existing mapping for username1 to work for username2 instead
            ExpressionRoleMapping mapping2 = new ExpressionRoleMapping(
                "test-username-expression",
                new FieldExpression("username", List.of(new FieldExpression.FieldValue(username2))),
                List.of(roleName),
                List.of(),
                Map.of(),
                true
            );
            publishRoleMappings(Set.of(mapping2));
        } else {
            // inactivate existing mapping for username1
            if (randomBoolean()) {
                // disable
                mapping1 = new ExpressionRoleMapping(
                    "test-username-expression",
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue(username1))),
                    List.of(roleName),
                    List.of(),
                    Map.of(),
                    false
                );
            } else {
                // change incompatibly
                mapping1 = new ExpressionRoleMapping(
                    "test-username-expression",
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue("WRONG"))),
                    List.of(roleName),
                    List.of(),
                    Map.of(),
                    true
                );
            }
            // add the new mapping for username2
            ExpressionRoleMapping mapping2 = new ExpressionRoleMapping(
                "test-username-expression-2",
                new FieldExpression("username", List.of(new FieldExpression.FieldValue(username2))),
                List.of(roleName),
                List.of(),
                Map.of(),
                true
            );
            publishRoleMappings(Set.of(mapping1, mapping2));
        }
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(username1Jwt, jwt0SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            if (anonymousRole) {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("testAnonymousRole")));
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), emptyIterable());
            }
        }
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(username2Jwt, jwt0SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            if (anonymousRole) {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo(roleName), equalTo("testAnonymousRole"))
                );
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo(roleName)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testGroupsRoleMappingForJWT() throws Exception {
        // JWT "access_token" valid for jwt2
        SignedJWT signedJWT = getSignedJWT(
            new JWTClaimsSet.Builder().audience("es-02")
                .issuer("my-issuer-02")
                .subject("user-02")
                .claim("groups", List.of("adminGroup", "superUserGroup"))
                .claim("appId", "appIdSubject")
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(300)))
                .build()
        );
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(signedJWT, jwt1SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            // no role mapping
            if (anonymousRole) {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("testAnonymousRole")));
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), emptyIterable());
            }
        }
        RoleMapperExpression roleMapperExpression = new AnyExpression(
            List.of(
                new FieldExpression("groups", List.of(new FieldExpression.FieldValue("adminGroup"))),
                new AllExpression(
                    List.of(
                        new FieldExpression("groups", List.of(new FieldExpression.FieldValue("superUserGroup"))),
                        new FieldExpression("metadata.jwt_claim_iss", List.of(new FieldExpression.FieldValue("WRONG")))
                    )
                )
            )
        );
        ExpressionRoleMapping mapping = new ExpressionRoleMapping(
            "test-username-expression",
            roleMapperExpression,
            List.of("role1", "role2"),
            List.of(),
            Map.of(),
            true
        );
        publishRoleMappings(Set.of(mapping));
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(signedJWT, jwt1SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            // groups based role mapping
            if (anonymousRole) {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo("role1"), equalTo("role2"), equalTo("testAnonymousRole"))
                );
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("role1"), equalTo("role2")));
            }
        }
        // clear off all the role mappings
        publishRoleMappings(Set.of());
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(signedJWT, jwt1SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            // no role mapping
            if (anonymousRole) {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("testAnonymousRole")));
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), emptyIterable());
            }
        }
        // reinstate the same role mapping expression but with different roles
        publishRoleMappings(Set.of());
        ExpressionRoleMapping mapping2 = new ExpressionRoleMapping(
            "test-username-expression",
            roleMapperExpression,
            List.of("role3"),
            List.of(),
            Map.of(),
            true
        );
        publishRoleMappings(Set.of(mapping2));
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(signedJWT, jwt1SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            if (anonymousRole) {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo("testAnonymousRole"), equalTo("role3"))
                );
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("role3")));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testRoleTemplatesMetadataForJWT() throws Exception {
        SignedJWT jwt = getSignedJWT(
            new JWTClaimsSet.Builder().audience("es-02")
                .issuer("my-issuer-02")
                .subject("user-02")
                .claim("groups", List.of("adminGroup", "superUserGroup"))
                .claim("appId", "testAppId")
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(300)))
                .build()
        );
        RoleMapperExpression roleMapperExpression = new AnyExpression(
            List.of(
                new AllExpression(
                    List.of(
                        new FieldExpression(
                            "groups",
                            List.of(new FieldExpression.FieldValue("superUserGroup"), new FieldExpression.FieldValue("adminGroup"))
                        ),
                        new FieldExpression("metadata.jwt_claim_appId", List.of(new FieldExpression.FieldValue("testAppId")))
                    )
                )
            )
        );
        TemplateRoleName templateRoleName = new TemplateRoleName(new BytesArray("""
            {"source":"[\\"{{metadata.jwt_claim_iss}}\\",\\"{{#join}}metadata.jwt_claim_aud{{/join}}\\"]"}
            """), TemplateRoleName.Format.JSON);
        ExpressionRoleMapping mapping = new ExpressionRoleMapping(
            "test-username-expression",
            roleMapperExpression,
            List.of(),
            List.of(templateRoleName),
            Map.of(),
            true
        );
        publishRoleMappings(Set.of(mapping));
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(jwt, jwt1SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            // no role mapping
            if (anonymousRole) {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo("my-issuer-02"), equalTo("es-02"), equalTo("testAnonymousRole"))
                );
            } else {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo("my-issuer-02"), equalTo("es-02"))
                );
            }
        }
        ExpressionRoleMapping disabledMapping = new ExpressionRoleMapping(
            "test-username-expression",
            roleMapperExpression,
            List.of(),
            List.of(templateRoleName),
            Map.of(),
            false
        );
        ExpressionRoleMapping anotherMapping = new ExpressionRoleMapping(
            randomFrom("test-username-expression", "another-expression"), // name for the mapping is not important
            new FieldExpression("username", List.of(new FieldExpression.FieldValue("testAppId"))),
            List.of(),
            List.of(new TemplateRoleName(new BytesArray("""
                {"source":"{{realm.name}}"}"""), TemplateRoleName.Format.STRING)),
            Map.of(),
            true
        );
        // disabling or removing the mapping is equivalent
        if (randomBoolean()) {
            publishRoleMappings(Set.of(disabledMapping, anotherMapping));
        } else {
            publishRoleMappings(Set.of(anotherMapping));
        }
        {
            Response authenticateResponse = getRestClient().performRequest(getAuthenticateRequest(jwt, jwt1SharedSecret));
            assertEquals(200, authenticateResponse.getStatusLine().getStatusCode());
            Map<String, Object> authenticateResponseMap = entityAsMap(authenticateResponse);
            // no role mapping
            if (anonymousRole) {
                assertThat(
                    (List<String>) authenticateResponseMap.get("roles"),
                    containsInAnyOrder(equalTo("jwt1"), equalTo("testAnonymousRole"))
                );
            } else {
                assertThat((List<String>) authenticateResponseMap.get("roles"), containsInAnyOrder(equalTo("jwt1")));
            }
        }
    }

    private SignedJWT getSignedJWT(JWTClaimsSet claimsSet) throws Exception {
        return JwtRealmSingleNodeTests.getSignedJWT(claimsSet, jwtHmacKey.getBytes(StandardCharsets.UTF_8));
    }

    private void publishRoleMappings(Set<ExpressionRoleMapping> roleMappings) throws InterruptedException {
        RoleMappingMetadata roleMappingMetadata = new RoleMappingMetadata(roleMappings);
        List<ClusterService> clusterServices = new ArrayList<>();
        internalCluster().getInstances(ClusterService.class).forEach(clusterServices::add);
        CountDownLatch publishedClusterState = new CountDownLatch(clusterServices.size());
        for (ClusterService clusterService : clusterServices) {
            clusterService.addListener(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    RoleMappingMetadata publishedRoleMappingMetadata = RoleMappingMetadata.getFromClusterState(event.state());
                    if (roleMappingMetadata.equals(publishedRoleMappingMetadata)) {
                        clusterService.removeListener(this);
                        publishedClusterState.countDown();
                    }
                }
            });
        }
        ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterClusterService.submitUnbatchedStateUpdateTask("test-add-role-mapping", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return roleMappingMetadata.updateClusterState(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
                for (int i = 0; i < clusterServices.size(); i++) {
                    publishedClusterState.countDown();
                }
            }
        });
        boolean awaitSuccessful = publishedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);
    }
}
