/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JwtRealmGenerateTests extends JwtRealmTestCase {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealmGenerateTests.class);

    /**
     * Generate a static JWT realm config, and request headers, which the realm can successfully authenticate.
     *
     * Print these artifacts, so they can be imported into this integration test project for verifying loading of JWT realm config:
     *  - x-pack/plugin/security/qa/smoke-test-all-realms/build.gradle
     *
     * If something needs tweaking, change the code here and re-run it. The new artifacts can be used to update that smoke test project.
     * @throws Exception Error if something goes wrong.
     */
    public void testCreateJwtSmokeTestRealm() throws Exception {
        // static HMAC key (password UTF-8 bytes, for OIDC HMAC Key encoding compatibility)
        final byte[] hmacKeyOidcBytes = "hmac-oidc-key-string-for-hs256-algorithm".getBytes(StandardCharsets.UTF_8);
        final OctetSequenceKey hmacJwkOidc = new OctetSequenceKey.Builder(hmacKeyOidcBytes).build();

        // Create JWT issuer with hard-coded HMAC signing key and HS256 algorithm, and a valid user from the smoke test project
        final JwtIssuer jwtIssuer = new JwtIssuer(
            "iss8",
            List.of("aud8"),
            JwtTestCase.randomJwks(Collections.emptyList()),
            JwtTestCase.randomJwks(Collections.emptyList()),
            new JwtIssuer.AlgJwkPair("HS256", hmacJwkOidc),
            Collections.singletonMap("security_test_user", new User("security_test_user", "security_test_role"))
        );

        // Create JWT realm settings with hard-coded name, order, issuer, audiences, algorithms, principal claim, secret, HMAC verify key
        final String realmName = "jwt8";
        final int realmOrder = 8;
        final Settings.Builder authcSettings = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), String.join(",", jwtIssuer.audiences))
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.getAllAlgorithms())
            )
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "sub")
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
                JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET
            );

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_KEY),
            new String(hmacKeyOidcBytes, StandardCharsets.UTF_8)
        );
        secureSettings.setString(
            RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            "client-shared-secret-string"
        );
        authcSettings.setSecureSettings(secureSettings);

        // Create JWT realm from the hard-coded settings
        final RealmConfig config = super.buildRealmConfig(JwtRealmSettings.TYPE, realmName, authcSettings.build(), realmOrder);
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(authcSettings.build()));
        final UserRoleMapper userRoleMapper = super.buildRoleMapper(jwtIssuer.users);
        final JwtRealm jwtRealm = new JwtRealm(config, sslService, userRoleMapper);
        jwtRealm.initialize(Collections.singletonList(jwtRealm), super.licenseState);

        // Register the JWT issuer and realm pair in this test class instance, so the realm is automatically closed
        super.jwtIssuerAndRealms = Collections.singletonList(new JwtIssuerAndRealm(jwtIssuer, jwtRealm));

        // Select the one JWT issuer and realm pair, and the one user from the issuer
        final JwtIssuerAndRealm jwtIssuerAndRealm = this.randomJwtIssuerRealmPair();
        final User user = this.randomUser(jwtIssuerAndRealm.issuer());

        // Sign a JWT for the one user, using the one HMAC key from the issuer, and claim settings from the realm
        final SecureString jwt;
        {   // Copied from randomJwt() and modified to hard-code exp far in the future
            final JwtIssuer.AlgJwkPair algJwkPair = randomFrom(jwtIssuerAndRealm.issuer().getAllAlgJwkPairs());
            LOGGER.info("JWK=[" + algJwkPair.jwk().getKeyType() + "/" + algJwkPair.jwk().size() + "], alg=[" + algJwkPair.alg() + "].");

            final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
                algJwkPair.alg(), // alg
                null, // jwtID
                jwtIssuerAndRealm.realm().allowedIssuer, // iss
                jwtIssuerAndRealm.realm().allowedAudiences, // aud
                user.principal(), // sub
                "sub", // principal claim name
                user.principal(), // principal claim value
                "roles", // group claim name
                List.of("security_test_role"), // group claim value
                null, // auth_time
                Date.from(ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()), // iat
                null, // nbf
                Date.from(ZonedDateTime.of(2099, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()), // exp
                null, // nonce
                Collections.emptyMap() // other claims
            );
            jwt = JwtValidateUtil.signJwt(algJwkPair.jwk(), unsignedJwt);
            assertThat(JwtValidateUtil.verifyJwt(algJwkPair.jwk(), SignedJWT.parse(jwt.toString())), is(equalTo(true)));
        }
        final SecureString clientSecret = jwtIssuerAndRealm.realm().clientAuthenticationSharedSecret;

        // Verify the JWT realm successfully authenticates the JWT and client secret
        final MinMax jwtAuthcRange = new MinMax(1, 1);
        super.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm.realm(), user, jwt, clientSecret, jwtAuthcRange);

        // Print the REST request headers and JWT realm settings. These static artifacts can be used in the integration smoke test.
        LOGGER.info(
            "\n===\nRequest Headers\n===\n"
                + JwtRealm.HEADER_CLIENT_AUTHENTICATION
                + ": "
                + clientSecret
                + "\n"
                + JwtRealm.HEADER_END_USER_AUTHENTICATION
                + ": "
                + jwt
                + "\n"
                + JwtRealmGenerateTests.printRealmSettings(config)
        );

        final SignedJWT signedJwt = SignedJWT.parse(new String(jwt.getChars()));
        LOGGER.info("JWT Contents\n===\nHeader: " + signedJwt.getHeader() + "\nClaims: " + signedJwt.getJWTClaimsSet() + "\n===");
    }

    private static String printRealmSettings(final RealmConfig config) {
        final StringBuilder sb = new StringBuilder("\n===\nelasticsearch.yml settings\n===\n");
        int numRegularSettings = 0;
        for (final Setting.AffixSetting<?> setting : JwtRealmSettings.getSettings()) {
            final String key = RealmSettings.getFullSettingKey(config, setting);
            if (key.startsWith("xpack.") && config.hasSetting(setting)) {
                final Object settingValue = config.getSetting(setting);
                if (settingValue instanceof SecureString == false) {
                    sb.append(key).append(": ").append(settingValue).append('\n');
                    numRegularSettings++;
                }
            }
        }
        if (numRegularSettings == 0) {
            sb.append("Not found.\n");
        }
        sb.append("\n===\nPKC JWKSet contents\n===\n");
        if (config.hasSetting(JwtRealmSettings.PKC_JWKSET_PATH) == false) {
            sb.append("Not found.\n");
        } else {
            final String key = RealmSettings.getFullSettingKey(config, JwtRealmSettings.PKC_JWKSET_PATH);
            final String pkcJwkSetPath = config.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
            try {
                if (JwtUtil.parseHttpsUri(pkcJwkSetPath) != null) {
                    sb.append("Found, but [").append(pkcJwkSetPath).append("] is not a local file.\n");
                } else {
                    final byte[] pkcJwkSetFileBytes = JwtUtil.readFileContents(key, pkcJwkSetPath, config.env());
                    sb.append(new String(pkcJwkSetFileBytes, StandardCharsets.UTF_8)).append('\n');
                }
            } catch (SettingsException se) {
                sb.append("Failed to load local file [").append(pkcJwkSetPath).append("].\n").append(se).append('\n');
            }
        }
        int numSecureSettings = 0;
        sb.append("\n===\nelasticsearch-keystore secure settings\n===\n");
        for (final Setting.AffixSetting<?> setting : JwtRealmSettings.getSettings()) {
            final String key = RealmSettings.getFullSettingKey(config, setting);
            if (key.startsWith("xpack.") && config.hasSetting(setting)) {
                final Object settingValue = config.getSetting(setting);
                if (settingValue instanceof SecureString) {
                    sb.append(key).append(": ").append(settingValue).append('\n');
                    numSecureSettings++;
                }
            }
        }
        if (numSecureSettings == 0) {
            sb.append("Not found.\n");
        }
        sb.append('\n');
        return sb.toString();
    }
}
