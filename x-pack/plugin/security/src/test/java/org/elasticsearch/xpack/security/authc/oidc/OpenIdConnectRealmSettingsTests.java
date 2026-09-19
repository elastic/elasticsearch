/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;

public class OpenIdConnectRealmSettingsTests extends ESTestCase {

    private static final String REALM_NAME = "oidc1-realm";
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadContext = new ThreadContext(globalSettings);
    }

    public void testAllSettings() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_USERINFO_ENDPOINT), "https://op.example.com/userinfo")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.POPULATE_USER_METADATA), randomBoolean())
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.GROUPS_CLAIM.getClaim()), "group1")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.DN_CLAIM.getClaim()), "uid=sub,ou=people,dc=example,dc=com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.NAME_CLAIM.getClaim()), "name")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.MAIL_CLAIM.getClaim()), "e@mail.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES), "openid")
            .put(
                getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_SIGNATURE_ALGORITHM),
                randomFrom(OpenIdConnectRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS)
            )
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_POST_LOGOUT_REDIRECT_URI), "https://my.rp.com/logout")
            .put(
                getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_METHOD),
                randomFrom(OpenIdConnectRealmSettings.CLIENT_AUTH_METHODS)
            )
            .put(
                getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_JWT_SIGNATURE_ALGORITHM),
                randomFrom(OpenIdConnectRealmSettings.SUPPORTED_CLIENT_AUTH_JWT_ALGORITHMS)
            )
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_CONNECT_TIMEOUT), "5s")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_CONNECTION_READ_TIMEOUT), "5s")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_SOCKET_TIMEOUT), "5s")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_MAX_CONNECTIONS), "5")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS), "5")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_HOST), "host")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_PORT), "8080")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME), "http")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.ALLOWED_CLOCK_SKEW), "10s");
        settingsBuilder.setSecureSettings(getSecureSettings());
        new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
    }

    public void testIncorrectResponseTypeThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "hybrid");
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE))
        );
    }

    public void testMissingAuthorizationEndpointThrowsError() {
        // op.authorization_endpoint is left unset, so the realm now attempts to resolve it from the OP's discovery
        // document at <op.issuer>/.well-known/openid-configuration; since op.issuer here doesn't serve one, that
        // fetch fails and surfaces as a SettingsException naming the (unreachable) discovery URL.
        final int closedPort = findClosedPort();
        final String issuer = "http://" + InetAddresses.toUriString(InetAddress.getLoopbackAddress()) + ":" + closedPort;
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), issuer)
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        final RealmConfig config = buildConfig(settingsBuilder.build());
        SettingsException exception = expectThrows(
            SettingsException.class,
            () -> new OpenIdConnectRealm(config, sslServiceForDiscovery(), null, null)
        );
        assertThat(exception.getMessage(), Matchers.containsString(issuer));
    }

    public void testInvalidAuthorizationEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "this is not a URI")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT))
        );
    }

    public void testMissingTokenEndpointThrowsErrorInCodeFlow() throws Exception {
        // op.token_endpoint is left unset, and op.authorization_endpoint/op.jwkset_path are also left unset so that
        // discovery is attempted; the served discovery document omits token_endpoint (optional per the OIDC
        // discovery spec), so it remains missing even after discovery, and the "code" response type still requires
        // it, producing the same "setting is required" SettingsException as before this feature existed.
        final HttpServer httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        try {
            final InetSocketAddress address = httpServer.getAddress();
            final String issuer = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
            httpServer.createContext("/.well-known/openid-configuration", exchange -> {
                final byte[] body = """
                    {
                      "issuer": "%s",
                      "authorization_endpoint": "%s/login",
                      "jwks_uri": "%s/jwks.json",
                      "response_types_supported": ["code"],
                      "subject_types_supported": ["public"],
                      "id_token_signing_alg_values_supported": ["RS256"]
                    }
                    """.formatted(issuer, issuer, issuer).getBytes(StandardCharsets.UTF_8);
                try {
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                } finally {
                    exchange.close();
                }
            });

            final Settings.Builder settingsBuilder = Settings.builder()
                .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), issuer)
                .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
                .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
                .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
                .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
            settingsBuilder.setSecureSettings(getSecureSettings());
            final RealmConfig config = buildConfig(settingsBuilder.build());
            SettingsException exception = expectThrows(
                SettingsException.class,
                () -> new OpenIdConnectRealm(config, sslServiceForDiscovery(), null, null)
            );
            assertThat(
                exception.getMessage(),
                Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT))
            );
        } finally {
            httpServer.stop(1);
        }
    }

    public void testMissingTokenEndpointIsAllowedInImplicitFlow() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "id_token token");
        settingsBuilder.setSecureSettings(getSecureSettings());
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        assertNotNull(realm);
    }

    public void testInvalidTokenEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "This is not a uri")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT))
        );
    }

    public void testMissingJwksUrlThrowsError() {
        // op.jwkset_path is left unset, so the realm attempts to resolve it from the OP's discovery document;
        // since op.issuer here doesn't serve one, that fetch fails with a SettingsException naming the discovery URL.
        final int closedPort = findClosedPort();
        final String issuer = "http://" + InetAddresses.toUriString(InetAddress.getLoopbackAddress()) + ":" + closedPort;
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), issuer)
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        final RealmConfig config = buildConfig(settingsBuilder.build());
        SettingsException exception = expectThrows(
            SettingsException.class,
            () -> new OpenIdConnectRealm(config, sslServiceForDiscovery(), null, null)
        );
        assertThat(exception.getMessage(), Matchers.containsString(issuer));
    }

    public void testMissingIssuerThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(exception.getMessage(), Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER)));
    }

    public void testMissingRedirectUriThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI))
        );
    }

    public void testMissingClientIdThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(exception.getMessage(), Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID)));
    }

    public void testMissingPrincipalClaimThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .putList(
                getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES),
                Arrays.asList("openid", "scope1", "scope2")
            );
        settingsBuilder.setSecureSettings(getSecureSettings());
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()))
        );
    }

    public void testPatternWithoutSettingThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.NAME_CLAIM.getPattern()), "^(.*)$")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .putList(
                getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES),
                Arrays.asList("openid", "scope1", "scope2")
            );
        settingsBuilder.setSecureSettings(getSecureSettings());
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.NAME_CLAIM.getClaim()))
        );
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.NAME_CLAIM.getPattern()))
        );
    }

    public void testMissingClientSecretThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_SECRET))
        );
    }

    public void testInvalidProxySchemeThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_HOST), "proxyhostname.org")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME), "invalid");
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            buildConfig(settingsBuilder.build()).getSetting(OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME))
        );
    }

    public void testInvalidProxyPortThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_HOST), "proxyhostname.org")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_PORT), 123456);
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            buildConfig(settingsBuilder.build()).getSetting(OpenIdConnectRealmSettings.HTTP_PROXY_PORT);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_PORT))
        );
    }

    public void testInvalidProxyHostThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_HOST), "proxy hostname.org")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_PORT), 8080);
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            buildConfig(settingsBuilder.build()).getSetting(OpenIdConnectRealmSettings.HTTP_PROXY_HOST);
        });
        assertThat(
            exception.getMessage(),
            Matchers.allOf(
                Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_HOST)),
                Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_PORT)),
                Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME))
            )
        );
    }

    public void testInvalidClientAuthenticationMethodThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_METHOD), "none")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_METHOD))
        );
    }

    public void testInvalidClientAuthenticationJwtAlgorithmThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_METHOD), "client_secret_jwt")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_JWT_SIGNATURE_ALGORITHM), "AB234");
        settingsBuilder.setSecureSettings(getSecureSettings());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()), null, null);
        });
        assertThat(
            exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_AUTH_JWT_SIGNATURE_ALGORITHM))
        );
    }

    private MockSecureSettings getSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_SECRET),
            randomAlphaOfLengthBetween(12, 18)
        );
        return secureSettings;
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("oidc", REALM_NAME);
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings)
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(realmIdentifier, settings, env, threadContext);
    }

    private int findClosedPort() {
        try (var socket = new java.net.ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * An {@link SSLService} whose realm SSL context is registered, for tests that need the realm's discovery
     * resolver to make a real (albeit failing or loopback-only) HTTP connection.
     */
    private SSLService sslServiceForDiscovery() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.oidc." + REALM_NAME + ".ssl.verification_mode", "certificate")
            .build();
        return new SSLService(TestEnvironment.newEnvironment(settings));
    }
}
