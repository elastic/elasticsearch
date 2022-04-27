/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Generate verified JWT configurations for integration tests or documentation.
 */
public class JwtRealmGenerateTests extends JwtRealmTestCase {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealmGenerateTests.class);

    private static final MinMax JWT_AUTHC_RANGE_1 = new MinMax(1, 1);
    private static final Date DATE_2000_1_1 = Date.from(ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());
    private static final Date DATE_2099_1_1 = Date.from(ZonedDateTime.of(2099, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());

    /**
     * Generate [jwt8] for [x-pack/plugin/security/qa/smoke-test-all-realms/build.gradle].
     * @throws Exception Error if something goes wrong.
     */
    public void testCreateJwtSmokeTestRealm() throws Exception {
        // Create HMAC key for algorithm HS256
        final String hmacKeyOidcString = "hmac-oidc-key-string-for-hs256-algorithm";
        final JwtIssuer.AlgJwkPair algJwkPairHmac = new JwtIssuer.AlgJwkPair(
            "HS256",
            new OctetSequenceKey.Builder(hmacKeyOidcString.getBytes(StandardCharsets.UTF_8)).build()
        );

        // Create issuer
        final JwtIssuer jwtIssuer = new JwtIssuer(
            "iss8", // iss
            List.of("aud8"), // aud
            Collections.emptyList(), // algJwkPairsPkc
            Collections.emptyList(), // algJwkPairsHmac
            algJwkPairHmac, // algJwkPairHmac
            Collections.singletonMap("security_test_user", new User("security_test_user", "security_test_role")), // users
            false // createHttpsServer
        );

        // Create realm settings
        final String realmName = "jwt8";
        final Settings.Builder configBuilder = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), String.join(",", jwtIssuer.audiences))
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.algorithmsAll)
            )
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "sub")
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
                JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET.value()
            );

        // Create realm secure settings
        final SecureString clientSecret = new SecureString("client-shared-secret-string".toCharArray());
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_KEY), hmacKeyOidcString);
        secureSettings.setString(
            RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            clientSecret.toString()
        );
        configBuilder.setSecureSettings(secureSettings);

        // Create realm
        final RealmConfig config = super.buildRealmConfig(JwtRealmSettings.TYPE, realmName, configBuilder.build(), 8);
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(configBuilder.build()));
        final UserRoleMapper userRoleMapper = super.buildRoleMapper(jwtIssuer.users);
        final JwtRealm jwtRealm = new JwtRealm(config, sslService, userRoleMapper);
        jwtRealm.initialize(Collections.singletonList(jwtRealm), super.licenseState);
        final JwtRealmNameAndSettingsBuilder realmNameAndSettingsBuilder = new JwtRealmNameAndSettingsBuilder(realmName, configBuilder);
        final JwtIssuerAndRealm jwtIssuerAndRealm = new JwtIssuerAndRealm(jwtIssuer, jwtRealm, realmNameAndSettingsBuilder);
        super.jwtIssuerAndRealms = Collections.singletonList(jwtIssuerAndRealm); // super.shutdown() closes issuer+realm if necessary

        // Create JWT
        final User user = this.randomUser(jwtIssuerAndRealm.issuer());
        final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
            randomBoolean() ? null : JOSEObjectType.JWT.toString(),
            algJwkPairHmac.alg(), // alg
            null, // jwtID
            jwtIssuerAndRealm.realm().allowedIssuer, // iss
            jwtIssuerAndRealm.realm().allowedAudiences, // aud
            user.principal(), // sub
            jwtIssuerAndRealm.realm().claimParserPrincipal.getClaimName(), // principal claim name
            user.principal(), // principal claim value
            jwtIssuerAndRealm.realm().claimParserGroups.getClaimName(), // group claim name
            List.of("security_test_role"), // group claim value
            null, // auth_time
            DATE_2000_1_1, // iat
            null, // nbf
            DATE_2099_1_1, // exp
            null, // nonce
            Collections.emptyMap() // other claims
        );
        final SecureString jwt = JwtValidateUtil.signJwt(algJwkPairHmac.jwk(), unsignedJwt);
        assertThat(JwtValidateUtil.verifyJwt(algJwkPairHmac.jwk(), SignedJWT.parse(jwt.toString())), is(equalTo(true)));

        // Verify authc+authz, then print all artifacts
        super.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm.realm(), user, jwt, clientSecret, JWT_AUTHC_RANGE_1);
        this.printArtifacts(jwtIssuer, config, clientSecret, jwt);
    }

    /**
     * Generate [jwt1] for [x-pack/plugin/security/qa/jwt-realm/build.gradle].
     * @throws Exception Error if something goes wrong.
     */
    public void testCreateJwtIntegrationTestRealm1() throws Exception {
        // Create RSA key for algorithm RS256
        final JWK jwk = new RSAKey.Builder((RSAKey) JwtTestCase.randomJwk("RS256")).keyID("test-rsa-key").build();
        final JwtIssuer.AlgJwkPair algJwkPairPkc = new JwtIssuer.AlgJwkPair("RS256", jwk);

        // Create issuer
        final JwtIssuer jwtIssuer = new JwtIssuer(
            "https://issuer.example.com/", // iss
            List.of("https://audience.example.com/"), // aud
            List.of(algJwkPairPkc), // algJwkPairsPkc
            Collections.emptyList(), // algJwkPairsHmac
            null, // algJwkPairHmac
            Collections.singletonMap("user1", new User("user1", "role1")), // users
            false // createHttpsServer
        );

        // Create realm settings (no secure settings)
        final String realmName = "jwt1";
        final Settings.Builder configBuilder = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), String.join(",", jwtIssuer.audiences))
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.algorithmsAll)
            )
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "sub")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_GROUPS.getClaim()), "roles")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_DN.getClaim()), "dn")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_NAME.getClaim()), "name")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_MAIL.getClaim()), "mail")
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
                JwtRealmSettings.ClientAuthenticationType.NONE.value()
            )
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.PKC_JWKSET_PATH),
                super.saveToTempFile("jwkset.", ".json", jwtIssuer.encodedJwkSetPkcPublic.getBytes(StandardCharsets.UTF_8))
            );

        // Create realm
        final RealmConfig config = super.buildRealmConfig(JwtRealmSettings.TYPE, realmName, configBuilder.build(), 2);
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(configBuilder.build()));
        final UserRoleMapper userRoleMapper = super.buildRoleMapper(jwtIssuer.users);
        final JwtRealm jwtRealm = new JwtRealm(config, sslService, userRoleMapper);
        jwtRealm.initialize(Collections.singletonList(jwtRealm), super.licenseState);
        final JwtRealmNameAndSettingsBuilder realmNameAndSettingsBuilder = new JwtRealmNameAndSettingsBuilder(realmName, configBuilder);
        final JwtIssuerAndRealm jwtIssuerAndRealm = new JwtIssuerAndRealm(jwtIssuer, jwtRealm, realmNameAndSettingsBuilder);
        super.jwtIssuerAndRealms = Collections.singletonList(jwtIssuerAndRealm); // super.shutdown() closes issuer+realm if necessary

        // Create JWT
        final User user = this.randomUser(jwtIssuerAndRealm.issuer());
        final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
            randomBoolean() ? null : JOSEObjectType.JWT.toString(),
            algJwkPairPkc.alg(), // alg
            null, // jwtID
            jwtIssuerAndRealm.realm().allowedIssuer, // iss
            jwtIssuerAndRealm.realm().allowedAudiences, // aud
            user.principal(), // sub
            jwtIssuerAndRealm.realm().claimParserPrincipal.getClaimName(), // principal claim name
            user.principal(), // principal claim value
            jwtIssuerAndRealm.realm().claimParserGroups.getClaimName(), // group claim name
            null, // group claim value
            null, // auth_time
            DATE_2000_1_1, // iat
            null, // nbf
            DATE_2099_1_1, // exp
            null, // nonce
            Collections.emptyMap() // other claims
        );
        final SecureString jwt = JwtValidateUtil.signJwt(algJwkPairPkc.jwk(), unsignedJwt);
        assertThat(JwtValidateUtil.verifyJwt(algJwkPairPkc.jwk(), SignedJWT.parse(jwt.toString())), is(equalTo(true)));

        // Verify authc+authz, then print all artifacts
        super.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm.realm(), user, jwt, null, JWT_AUTHC_RANGE_1);
        this.printArtifacts(jwtIssuer, config, null, jwt);
    }

    /**
     * Generate [jwt2] for [x-pack/plugin/security/qa/jwt-realm/build.gradle].
     * @throws Exception Error if something goes wrong.
     */
    public void testCreateJwtIntegrationTestRealm2() throws Exception {
        // Create HMAC key for algorithm HS256
        final String hmacKeyOidcString = "test-HMAC/secret passphrase-value";
        final JwtIssuer.AlgJwkPair algJwkPairHmac = new JwtIssuer.AlgJwkPair(
            "HS256",
            new OctetSequenceKey.Builder(hmacKeyOidcString.getBytes(StandardCharsets.UTF_8)).build()
        );

        // Create issuer
        final JwtIssuer jwtIssuer = new JwtIssuer(
            "my-issuer", // iss
            List.of("es01", "es02", "es03"), // aud
            Collections.emptyList(), // algJwkPairsPkc
            Collections.emptyList(), // algJwkPairsHmac
            algJwkPairHmac, // algJwkPairHmac
            Collections.singletonMap("user2", new User("user2", "role2")), // users
            false // createHttpsServer
        );

        // Create realm settings
        final String realmName = "jwt2";
        final String authzRealmName = "lookup_native";
        final Settings.Builder configBuilder = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), String.join(",", jwtIssuer.audiences))
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS), "HS256,HS384")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "email")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^(.*)@[^.]*[.]example[.]com$")
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_MAIL.getClaim()), "email")
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
                JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET.value()
            )
            .put(
                RealmSettings.getFullSettingKey(realmName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                authzRealmName
            );

        // Create realm secure settings
        final SecureString clientSecret = new SecureString("test-secret".toCharArray());
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_KEY), hmacKeyOidcString);
        secureSettings.setString(
            RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            clientSecret.toString()
        );
        configBuilder.setSecureSettings(secureSettings);

        // Create authz realm
        final RealmConfig authzConfig = this.buildRealmConfig("native", authzRealmName, Settings.EMPTY, 0);
        final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
        jwtIssuer.users.values().forEach(authzRealm::registerUser); // authz realm will do roles lookup

        // Create realm
        final RealmConfig config = super.buildRealmConfig(JwtRealmSettings.TYPE, realmName, configBuilder.build(), 3);
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(configBuilder.build()));
        final UserRoleMapper userRoleMapper = super.buildRoleMapper(Map.of()); // authc realm will not do role mapping
        final JwtRealm jwtRealm = new JwtRealm(config, sslService, userRoleMapper);
        jwtRealm.initialize(List.of(authzRealm, jwtRealm), super.licenseState);
        final JwtRealmNameAndSettingsBuilder realmNameAndSettingsBuilder = new JwtRealmNameAndSettingsBuilder(realmName, configBuilder);
        final JwtIssuerAndRealm jwtIssuerAndRealm = new JwtIssuerAndRealm(jwtIssuer, jwtRealm, realmNameAndSettingsBuilder);
        super.jwtIssuerAndRealms = Collections.singletonList(jwtIssuerAndRealm); // super.shutdown() closes issuer+realm if necessary

        // Create JWT
        final User user = this.randomUser(jwtIssuerAndRealm.issuer());
        final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
            JOSEObjectType.JWT.toString(),
            algJwkPairHmac.alg(), // alg
            null, // jwtID
            jwtIssuerAndRealm.realm().allowedIssuer, // iss
            jwtIssuerAndRealm.realm().allowedAudiences, // aud
            user.principal(), // sub
            jwtIssuerAndRealm.realm().claimParserPrincipal.getClaimName(), // principal claim name
            "user2@something.example.com", // principal claim value
            jwtIssuerAndRealm.realm().claimParserGroups.getClaimName(), // group claim name
            null, // group claim value
            null, // auth_time
            DATE_2000_1_1, // iat
            null, // nbf
            DATE_2099_1_1, // exp
            null, // nonce
            Collections.emptyMap() // other claims
        );
        final SecureString jwt = JwtValidateUtil.signJwt(algJwkPairHmac.jwk(), unsignedJwt);
        assertThat(JwtValidateUtil.verifyJwt(algJwkPairHmac.jwk(), SignedJWT.parse(jwt.toString())), is(equalTo(true)));

        // Verify authc+authz, then print all artifacts
        super.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm.realm(), user, jwt, clientSecret, JWT_AUTHC_RANGE_1);
        this.printArtifacts(jwtIssuer, config, clientSecret, jwt);
    }

    /**
     * Generate [jwt3] for [x-pack/plugin/security/qa/jwt-realm/build.gradle].
     * @throws Exception Error if something goes wrong.
     */
    public void testCreateJwtIntegrationTestRealm3() throws Exception {
        // Create HMAC keys
        final List<JwtIssuer.AlgJwkPair> hmacKeys = List.of(
            new JwtIssuer.AlgJwkPair("HS384", new OctetSequenceKey.Builder(randomByteArrayOfLength(48)).keyID("test-hmac-384").build()),
            new JwtIssuer.AlgJwkPair("HS512", new OctetSequenceKey.Builder(randomByteArrayOfLength(64)).keyID("test-hmac-512").build())
        );
        var selectedHmac = randomFrom(hmacKeys);

        // Create issuer
        final JwtIssuer jwtIssuer = new JwtIssuer(
            "jwt3-issuer", // iss
            List.of("jwt3-audience"), // aud
            Collections.emptyList(), // algJwkPairsPkc
            hmacKeys, // algJwkPairsHmac
            null, // algJwkPairHmac
            Collections.singletonMap("user3", new User("user3", "role3")), // users
            false // createHttpsServer
        );

        // Create realm settings
        final String realmName = "jwt3";
        final Settings.Builder configBuilder = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuer)
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_AUDIENCES), String.join(",", jwtIssuer.audiences))
            .put(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), "sub")
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.algorithmsAll)
            )
            .put(
                RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
                JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET.value()
            );

        // Create realm secure settings
        final SecureString clientSecret = new SecureString("test-secret".toCharArray());
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.HMAC_JWKSET), jwtIssuer.encodedJwkSetHmac);
        secureSettings.setString(
            RealmSettings.getFullSettingKey(realmName, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            clientSecret.toString()
        );
        configBuilder.setSecureSettings(secureSettings);

        // Create realm
        final RealmConfig config = super.buildRealmConfig(JwtRealmSettings.TYPE, realmName, configBuilder.build(), 4);
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(configBuilder.build()));
        final UserRoleMapper userRoleMapper = super.buildRoleMapper(jwtIssuer.users);
        final JwtRealm jwtRealm = new JwtRealm(config, sslService, userRoleMapper);
        jwtRealm.initialize(List.of(jwtRealm), super.licenseState);
        final JwtRealmNameAndSettingsBuilder realmNameAndSettingsBuilder = new JwtRealmNameAndSettingsBuilder(realmName, configBuilder);
        final JwtIssuerAndRealm jwtIssuerAndRealm = new JwtIssuerAndRealm(jwtIssuer, jwtRealm, realmNameAndSettingsBuilder);
        super.jwtIssuerAndRealms = Collections.singletonList(jwtIssuerAndRealm); // super.shutdown() closes issuer+realm if necessary

        // Create JWT
        final User user = this.randomUser(jwtIssuerAndRealm.issuer());
        final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
            JOSEObjectType.JWT.toString(),
            selectedHmac.alg(), // alg
            null, // jwtID
            jwtIssuerAndRealm.realm().allowedIssuer, // iss
            jwtIssuerAndRealm.realm().allowedAudiences, // aud
            user.principal(), // sub
            jwtIssuerAndRealm.realm().claimParserPrincipal.getClaimName(), // principal claim name
            randomFrom(jwtIssuer.users.keySet()), // principal claim value
            jwtIssuerAndRealm.realm().claimParserGroups.getClaimName(), // group claim name
            null, // group claim value
            null, // auth_time
            DATE_2000_1_1, // iat
            null, // nbf
            DATE_2099_1_1, // exp
            null, // nonce
            Collections.emptyMap() // other claims
        );
        final SecureString jwt = JwtValidateUtil.signJwt(selectedHmac.jwk(), unsignedJwt);
        assertThat(JwtValidateUtil.verifyJwt(selectedHmac.jwk(), SignedJWT.parse(jwt.toString())), is(equalTo(true)));

        // Verify authc+authz, then print all artifacts
        super.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm.realm(), user, jwt, clientSecret, JWT_AUTHC_RANGE_1);
        this.printArtifacts(jwtIssuer, config, clientSecret, jwt);
    }

    private void printArtifacts(
        final JwtIssuer jwtIssuer,
        final RealmConfig config,
        final SecureString clientSecret,
        final SecureString jwt
    ) throws Exception {
        final SignedJWT signedJwt = SignedJWT.parse(jwt.toString());
        LOGGER.info(
            JwtRealmGenerateTests.printIssuerSettings(jwtIssuer)
                + JwtRealmGenerateTests.printRealmSettings(config)
                + "\n===\nRequest Headers\n===\n"
                + (Strings.hasText(clientSecret) ? JwtRealm.HEADER_CLIENT_AUTHENTICATION + ": " + clientSecret + "\n" : "")
                + JwtRealm.HEADER_END_USER_AUTHENTICATION
                + ": "
                + jwt
                + "\n\n===\nJWT Contents\n===\nHeader: "
                + signedJwt.getHeader()
                + "\nClaims: "
                + signedJwt.getJWTClaimsSet()
                + "\nSignature: "
                + signedJwt.getSignature()
                + "\n==="
        );
    }

    private static String printIssuerSettings(final JwtIssuer jwtIssuer) {
        final StringBuilder sb = new StringBuilder("\n===\nIssuer settings\n===\n");
        sb.append("Issuer: ").append(jwtIssuer.issuer).append('\n');
        sb.append("Audiences: ").append(String.join(",", jwtIssuer.audiences)).append('\n');
        sb.append("Algorithms: ").append(String.join(",", jwtIssuer.algorithmsAll)).append("\n");
        if (jwtIssuer.algAndJwksPkc.isEmpty() == false) {
            sb.append("PKC JWKSet (Private): ").append(jwtIssuer.encodedJwkSetPkcPrivate).append("\n");
            sb.append("PKC JWKSet (Public): ").append(jwtIssuer.encodedJwkSetPkcPublic).append("\n");
        }
        if (jwtIssuer.algAndJwksHmac.isEmpty() == false) {
            sb.append("HMAC JWKSet: ").append(jwtIssuer.encodedJwkSetHmac).append("\n");
        }
        if (jwtIssuer.algAndJwkHmacOidc != null) {
            final String keyStr = new String(jwtIssuer.algAndJwkHmacOidc.jwk().toOctetSequenceKey().toByteArray(), StandardCharsets.UTF_8);
            sb.append("HMAC OIDC: ").append(keyStr).append('\n');
        }
        return sb.toString();
    }

    private static String printRealmSettings(final RealmConfig config) {
        final StringBuilder sb = new StringBuilder();
        for (final OutputStyle style : OutputStyle.values()) {
            final StringBuilder sb1 = new StringBuilder("\n===\nRealm settings [").append(style.fileName()).append("]\n===\n");
            final StringBuilder sb2 = new StringBuilder("\n===\nRealm secure settings [").append(style.fileName()).append("]\n===\n");
            int numRegularSettings = 0;
            int numSecureSettings = 0;
            for (final Setting.AffixSetting<?> setting : JwtRealmSettings.getSettings()) {
                final String key = RealmSettings.getFullSettingKey(config, setting);
                if (key.startsWith("xpack.") && config.hasSetting(setting)) {
                    final Object settingValue = config.getSetting(setting);
                    if (settingValue instanceof SecureString) {
                        switch (style) {
                            case YML -> sb2.append(key).append(": ").append(settingValue).append('\n');
                            case BUILD_GRADLE -> sb2.append("keystore '").append(key).append("', '").append(settingValue).append("'\n");
                        }
                        numSecureSettings++;
                    } else {
                        switch (style) {
                            case YML -> sb1.append(key).append(": ").append(settingValue).append('\n');
                            case BUILD_GRADLE -> sb1.append("setting '").append(key).append("', '").append(settingValue).append("'\n");
                        }
                        numRegularSettings++;
                    }
                }
            }
            if (numRegularSettings == 0) {
                sb1.append("None\n");
            }
            if (numSecureSettings == 0) {
                sb2.append("None\n");
            }
            sb.append(sb1).append(sb2);
        }
        sb.append("\n===\nRealm file contents\n===\n");
        if (config.hasSetting(JwtRealmSettings.PKC_JWKSET_PATH) == false) {
            sb.append("PKC JWKSet: Not found.\n");
        } else {
            final String key = RealmSettings.getFullSettingKey(config, JwtRealmSettings.PKC_JWKSET_PATH);
            final String pkcJwkSetPath = config.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
            try {
                if (JwtUtil.parseHttpsUri(pkcJwkSetPath) != null) {
                    sb.append("Found, but [").append(pkcJwkSetPath).append("] is not a local file.\n");
                } else {
                    final byte[] pkcJwkSetFileBytes = JwtUtil.readFileContents(key, pkcJwkSetPath, config.env());
                    sb.append("PKC JWKSet: ").append(new String(pkcJwkSetFileBytes, StandardCharsets.UTF_8)).append('\n');
                }
            } catch (SettingsException se) {
                sb.append("Failed to load local file [").append(pkcJwkSetPath).append("].\n").append(se).append('\n');
            }
        }
        return sb.toString();
    }

    enum OutputStyle {
        YML,
        BUILD_GRADLE;

        public String fileName() {
            return name().toLowerCase(Locale.ROOT).replace('_', '.');
        }
    }
}
