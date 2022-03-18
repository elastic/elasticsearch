/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.security.authc.jwt.JwtRealm;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SecurityDomainIntegTests extends AbstractProfileIntegTestCase {

    private static final Map<String, Object> REALM_DOMAIN_MAP = Map.of(
        "name",
        "my_domain",
        "realms",
        List.of(Map.of("name", "index", "type", "native"), Map.of("name", "jwt_realm_1", "type", "jwt"))
    );

    private static final String HEADER_SECRET_JWT_REALM_1 = "client-shared-secret-string";
    /**
     * // header
     * {
     *   "alg": "HS256"
     * }
     * // payload
     * {
     *   "aud": "aud",
     *   "sub": "rac_user",
     *   "roles": "[rac_role]",
     *   "iss": "iss",
     *   "exp": 4070908800,
     *   "iat": 946684800
     * }
     * // signed with "hmac-oidc-key-string-for-hs256-algorithm"
     */
    private static final String HEADER_JWT_REALM_1 = "eyJhbGciOiJIUzI1NiJ9."
        + "eyJhdWQiOiJhdWQiLCJzdWIiOiJyYWNfdXNlciIsInJvbGVzIjoiW3JhY19yb2xlXSIsImlzcyI6ImlzcyIsImV4cCI6NDA3MDkwODgwMCwiaWF0Ijo5NDY2ODQ4MDB9"
        + ".vbEtAFY4e47ZovTcxlWRyU9BTRYB092kJm7SGsnd7KM";

    private static final String HEADER_SECRET_JWT_REALM_2 = "client-shared-secret-string-2";
    /**
     * // header
     * {
     *   "alg": "HS256"
     * }
     * // payload
     * {
     *   "aud": "aud",
     *   "sub": "rac_user",
     *   "roles": "[rac_role]",
     *   "iss": "other_iss",
     *   "exp": 4070908800,
     *   "iat": 946684800
     * }
     * // signed with "hmac-oidc-key-string-for-hs256-algorithm-2"
     */
    private static final String HEADER_JWT_REALM_2 = "eyJhbGciOiJIUzI1NiJ9."
        + "eyJhdWQiOiJhdWQiLCJzdWIiOiJyYWNfdXNlciIsInJvbGVzIjoiW3JhY19yb2xlXSIsImlzcyI6Im90aGVyX2lzcyIsImV4cCI6NDA3MDkwODgwMCwiaWF0Ijo5NDY2ODQ4MDB9"
        + ".4_c4sJ70xy-WssadlvAXL9OsR8V3qAZoY5bzPLjsong";

    @Before
    public void setNativeMapping() throws IOException {
        PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest();
        putRoleMappingRequest.setName("rac_role_mapping");
        putRoleMappingRequest.setRoles(List.of("rac_role"));
        putRoleMappingRequest.setRules(
            new ExpressionParser().parse(
                "rules",
                new XContentSource(new BytesArray("{ \"field\": { \"username\" : \"rac_user\" } }"), XContentType.JSON)
            )
        );
        client().execute(PutRoleMappingAction.INSTANCE, putRoleMappingRequest).actionGet();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // register two JWT realm
        builder.put("xpack.security.authc.realms.jwt.jwt_realm_1.order", "-1")
            .put("xpack.security.authc.realms.jwt.jwt_realm_1.allowed_signature_algorithms", "HS256")
            .put("xpack.security.authc.realms.jwt.jwt_realm_1.allowed_issuer", "iss")
            .put("xpack.security.authc.realms.jwt.jwt_realm_1.allowed_audiences", "aud")
            .put("xpack.security.authc.realms.jwt.jwt_realm_1.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt_realm_1.client_authentication.type", "shared_secret");
        builder.put("xpack.security.authc.realms.jwt.jwt_realm_2.order", "5")
            .put("xpack.security.authc.realms.jwt.jwt_realm_2.allowed_signature_algorithms", "HS256")
            .put("xpack.security.authc.realms.jwt.jwt_realm_2.allowed_issuer", "other_iss")
            .put("xpack.security.authc.realms.jwt.jwt_realm_2.allowed_audiences", "aud")
            .put("xpack.security.authc.realms.jwt.jwt_realm_2.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt_realm_2.client_authentication.type", "shared_secret");
        if (builder.getSecureSettings() == null) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        ((MockSecureSettings) builder.getSecureSettings()).setString(
            "xpack.security.authc.realms.jwt.jwt_realm_1.client_authentication.shared_secret",
            HEADER_SECRET_JWT_REALM_1
        );
        ((MockSecureSettings) builder.getSecureSettings()).setString(
            "xpack.security.authc.realms.jwt.jwt_realm_1.hmac_key",
            "hmac-oidc-key-string-for-hs256-algorithm"
        );
        ((MockSecureSettings) builder.getSecureSettings()).setString(
            "xpack.security.authc.realms.jwt.jwt_realm_2.client_authentication.shared_secret",
            HEADER_SECRET_JWT_REALM_2
        );
        ((MockSecureSettings) builder.getSecureSettings()).setString(
            "xpack.security.authc.realms.jwt.jwt_realm_2.hmac_key",
            "hmac-oidc-key-string-for-hs256-algorithm-2"
        );
        // index is in a domain with one of the jwt realms
        builder.put("xpack.security.authc.domains.my_domain.realms", "index,jwt_realm_1");
        if (randomBoolean()) {
            builder.put("xpack.security.authc.domains.other_domain.realms", "jwt_realm_2,file");
        }
        return builder.build();
    }

    public void testTokenRefreshUnderSameUsernameInDomain() {
        // "index"-realm user creates token for the "file"-realm user
        var createTokenResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD.clone()))
        )
            .execute(
                CreateTokenAction.INSTANCE,
                new CreateTokenRequest(
                    "password",
                    SecuritySettingsSource.TEST_USER_NAME,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.clone(),
                    null,
                    null,
                    null
                )
            )
            .actionGet();
        var refreshToken = createTokenResponse.getRefreshToken();
        assertNotNull(refreshToken);
        // same-domain "jwt" user refreshes token
        var refreshTokenResponse = client().filterWithHeader(
            Map.of(
                JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + HEADER_SECRET_JWT_REALM_1,
                JwtRealm.HEADER_END_USER_AUTHENTICATION,
                JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + HEADER_JWT_REALM_1
            )
        ).execute(RefreshTokenAction.INSTANCE, new CreateTokenRequest("refresh_token", null, null, null, null, refreshToken)).actionGet();
        assertNotNull(refreshTokenResponse.getRefreshToken());
        // "jwt" user creates token for the "file" user
        createTokenResponse = client().filterWithHeader(
            Map.of(
                JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + HEADER_SECRET_JWT_REALM_1,
                JwtRealm.HEADER_END_USER_AUTHENTICATION,
                JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + HEADER_JWT_REALM_1
            )
        )
            .execute(
                CreateTokenAction.INSTANCE,
                new CreateTokenRequest(
                    "password",
                    SecuritySettingsSource.TEST_USER_NAME,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.clone(),
                    null,
                    null,
                    null
                )
            )
            .actionGet();
        refreshToken = createTokenResponse.getRefreshToken();
        assertNotNull(refreshToken);
        // same-domain "index" user refreshes token
        refreshTokenResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD.clone()))
        ).execute(RefreshTokenAction.INSTANCE, new CreateTokenRequest("refresh_token", null, null, null, null, refreshToken)).actionGet();
        assertNotNull(refreshTokenResponse.getRefreshToken());
    }

    public void testTokenRefreshFailsForUsernameOutsideDomain() {
        // "index"-realm user creates token for the "file"-realm user
        var createTokenResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD.clone()))
        )
            .execute(
                CreateTokenAction.INSTANCE,
                new CreateTokenRequest(
                    "password",
                    SecuritySettingsSource.TEST_USER_NAME,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.clone(),
                    null,
                    null,
                    null
                )
            )
            .actionGet();
        var refreshToken = createTokenResponse.getRefreshToken();
        assertNotNull(refreshToken);
        // fail to refresh from outside realms
        var e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(
                Map.of(
                    "Authorization",
                    basicAuthHeaderValue(
                        SecuritySettingsSource.TEST_USER_NAME,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.clone()
                    )
                )
            )
                .execute(RefreshTokenAction.INSTANCE, new CreateTokenRequest("refresh_token", null, null, null, null, refreshToken))
                .actionGet()
        );
        assertThat(e.getDetailedMessage(), containsString("invalid_grant"));
        e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(
                Map.of(
                    JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                    JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + HEADER_SECRET_JWT_REALM_2,
                    JwtRealm.HEADER_END_USER_AUTHENTICATION,
                    JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + HEADER_JWT_REALM_2
                )
            )
                .execute(RefreshTokenAction.INSTANCE, new CreateTokenRequest("refresh_token", null, null, null, null, refreshToken))
                .actionGet()
        );
        assertThat(e.getDetailedMessage(), containsString("invalid_grant"));
    }

    public void testDomainCaptureForApiKey() {
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLengthBetween(3, 8), null, null);

        final CreateApiKeyResponse createApiKeyResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD.clone()))
        ).execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest).actionGet();

        final XContentTestUtils.JsonMapView getResponseView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(
                client().prepareGet(SECURITY_MAIN_ALIAS, createApiKeyResponse.getId()).execute().actionGet().getSourceAsBytes()
            )
        );

        // domain info is captured
        assertThat(getResponseView.get("creator.realm_domain"), equalTo(REALM_DOMAIN_MAP));

        // API key is usable
        client().filterWithHeader(
            Map.of(
                "Authorization",
                "ApiKey "
                    + Base64.getEncoder()
                        .encodeToString(
                            (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8)
                        )
            )
        ).admin().cluster().prepareHealth().execute().actionGet();
    }

    public void testDomainCaptureForServiceToken() {
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final CreateServiceAccountTokenRequest createServiceTokenRequest = new CreateServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            tokenName
        );

        final CreateServiceAccountTokenResponse createServiceTokenResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD))
        ).execute(CreateServiceAccountTokenAction.INSTANCE, createServiceTokenRequest).actionGet();

        final XContentTestUtils.JsonMapView responseView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(
                client().prepareGet(SECURITY_MAIN_ALIAS, "service_account_token-elastic/fleet-server/" + tokenName)
                    .execute()
                    .actionGet()
                    .getSourceAsBytes()
            )
        );

        assertThat(responseView.get("creator.realm_domain"), equalTo(REALM_DOMAIN_MAP));

        // The service token is usable
        client().filterWithHeader(Map.of("Authorization", "Bearer " + createServiceTokenResponse.getValue()))
            .admin()
            .cluster()
            .prepareHealth()
            .execute()
            .actionGet();
    }
}
