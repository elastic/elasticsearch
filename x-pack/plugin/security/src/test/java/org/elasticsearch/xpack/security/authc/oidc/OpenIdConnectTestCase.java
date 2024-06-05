/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.shaded.json.JSONStyle;
import com.nimbusds.jose.shaded.json.JSONValue;
import com.nimbusds.jose.shaded.json.reader.JsonWriterI;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Date;

import static java.time.Instant.now;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;

public abstract class OpenIdConnectTestCase extends ESTestCase {

    @BeforeClass
    public static void setupWriters() {
        // In test code, we sometimes create claims sets with claims that use the `Nonce` class; therefore, we register a writer
        // for them here; otherwise json-smart tries to use reflection which our security manage prohibits
        // This only applies to test, not prod code, since we don't create claim sets with "non-default" classes
        JSONValue.registerWriter(Nonce.class, new JsonWriterI<Nonce>() {
            @Override
            public <E extends Nonce> void writeJSONString(E e, Appendable appendable, JSONStyle jsonStyle) throws IOException {
                appendable.append(e.toJSONString());
            }
        });
    }

    protected static final String REALM_NAME = "oidc-realm";

    protected static Settings.Builder getBasicRealmSettings() {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(OpenIdConnectRealmSettings.TYPE, REALM_NAME);
        return Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.org/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.org/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ENDSESSION_ENDPOINT), "https://op.example.org/logout")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.org/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.elastic.co/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_POST_LOGOUT_REDIRECT_URI), "https://rp.elastic.co/succ_logout")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), randomFrom("code", "id_token"))
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.GROUPS_CLAIM.getClaim()), "groups")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.MAIL_CLAIM.getClaim()), "mail")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.NAME_CLAIM.getClaim()), "name")
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .setSecureSettings(getSecureSettings());
    }

    protected static MockSecureSettings getSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_SECRET),
            randomAlphaOfLengthBetween(12, 18)
        );
        return secureSettings;
    }

    protected JWT generateIdToken(String subject, String audience, String issuer) throws Exception {
        int hashSize = randomFrom(256, 384, 512);
        int keySize = randomFrom(2048, 4096);
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(keySize);
        KeyPair keyPair = gen.generateKeyPair();
        JWTClaimsSet idTokenClaims = new JWTClaimsSet.Builder().jwtID(randomAlphaOfLength(8))
            .audience(audience)
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issuer(issuer)
            .issueTime(Date.from(now().minusSeconds(4)))
            .notBeforeTime(Date.from(now().minusSeconds(4)))
            .claim("nonce", new Nonce())
            .subject(subject)
            .build();

        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.parse("RS" + hashSize)).build(), idTokenClaims);
        jwt.sign(new RSASSASigner(keyPair.getPrivate()));
        return jwt;
    }

    protected RealmConfig buildConfig(Settings realmSettings, ThreadContext threadContext) {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("oidc", REALM_NAME);
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings)
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(realmIdentifier, settings, env, threadContext);
    }

    public static void writeJwkSetToFile(Path file) throws IOException {
        Files.write(file, Arrays.asList("""
            {
              "keys": [
                {
                  "kty": "RSA",
                  "d": "lT2V49RNsu0eTroQDqFCiHY-CkPWdKfKAf66sJrWPNpSX8URa6pTCruFQMsb9ZSqQ8eIvqys9I9rq6Wpaxn1aGRahVzxp7nsBPZYwSY09L\
            RzhvAxJwWdwtF-ogrV5-p99W9mhEa0khot3myzzfWNnGzcf1IudqvkqE9zrlUJg-kvA3icbs6HgaZVAevb_mx-bgbtJdnUxyPGwXLyQ7g6hlntQR_vpzTnK\
            7XFU6fvkrojh7UPJkanKAH0gf3qPrB-Y2gQML7RSlKo-ZfJNHa83G4NRLHKuWTI6dSKJlqmS9zWGmyC3dx5kGjgqD6YgwtWlip8q-U839zxtz25yeslsQ",
                  "e": "AQAB",
                  "use": "sig",
                  "kid": "testkey",
                  "alg": "RS256",
                  "n": "lXBe4UngWJiUfbqbeOvwbH04kYLCpeH4k0o3ngScZDo6ydc_gBDEVwPLQpi8D930aIzr3XHP3RCj0hnpxUun7MNMhWxJZVOd1eg5uuO-nP\
            Ihkqr9iGKV5srJk0Dvw0wBaGZuXMBheY2ViNaKTR9EEtjNwU2d2-I5U3YlrnFR6nj-Pn_hWaiCbb_pSFM4w9QpoLDmuwMRanHY_YK7Td2WMICSGP\
            3IRGmbecRZCqgkWVZk396EMoMLNxi8WcErYknyY9r-QeJMruRkr27kgx78L7KZ9uBmu9oKXRQl15ZDYe7Bnt9E5wSdOCV9R9h5VRVUur-_129XkD\
            eAX-6re63_Mw"
                }
              ]
            }"""));
    }
}
