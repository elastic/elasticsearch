/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static java.time.Instant.now;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public abstract class JwtTestCase extends ESTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    protected String pathHome;
    protected Settings globalSettings;
    protected Environment env;
    protected ThreadContext threadContext;

    @Before
    public void beforeEachTest() {
        this.pathHome = createTempDir().toString();
        this.globalSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), this.pathHome).build();
        this.env = TestEnvironment.newEnvironment(this.globalSettings); // "path.home" sub-dirs: config,plugins,data,logs,bin,lib,modules
        this.threadContext = new ThreadContext(this.globalSettings);
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJWSVerifierTuple(final Object secretKeyOrKeyPair) throws JOSEException {
        assertThat(secretKeyOrKeyPair, is(notNullValue()));
        assertThat(secretKeyOrKeyPair, is(anyOf(isA(SecretKey.class), isA(KeyPair.class))));
        if (secretKeyOrKeyPair instanceof SecretKey hmacKey) {
            return new Tuple<>(new MACSigner(hmacKey), new MACVerifier(hmacKey));
        } else if (secretKeyOrKeyPair instanceof KeyPair keyPair) {
            if (keyPair.getPrivate()instanceof RSAPrivateKey rsaPrivateKey) {
                return new Tuple<>(new RSASSASigner(rsaPrivateKey), new RSASSAVerifier((RSAPublicKey) keyPair.getPublic()));
            } else if (keyPair.getPrivate()instanceof ECPrivateKey ecPrivateKey) {
                return new Tuple<>(new ECDSASigner(ecPrivateKey), new ECDSAVerifier((ECPublicKey) keyPair.getPublic()));
            }
        }
        return null;
    }

    public static Object generateSecretKeyOrKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        if (JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS.contains(signatureAlgorithm)) {
            return generateSecretKey(signatureAlgorithm); // SecretKeySpec
        } else if (JwtRealmSettings.SUPPORTED_PUBLIC_KEY_RSA_SIGNATURE_ALGORITHMS.contains(signatureAlgorithm)) {
            return generateRsaKeyPair(signatureAlgorithm); // KeyPair(RSAPublicKey,RSAPrivateKey)
        } else if (JwtRealmSettings.SUPPORTED_PUBLIC_KEY_EC_SIGNATURE_ALGORITHMS.contains(signatureAlgorithm)) {
            return generateEcKeyPair(signatureAlgorithm); // KeyPair(ECPublicKey,ECPrivateKey)
        }
        fail("Test does not support signing algorithm " + jwsAlgorithm);
        return null;
    }

    // Nimbus JOSE+JWT requires HMAC keys to match or exceed digest strength.
    public static SecretKey generateSecretKey(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        final int minRequiredSecretBytesLength = MACSigner.getMinRequiredSecretLength(jwsAlgorithm) / 8;
        final byte[] hmacKeyBytes = new byte[randomIntBetween(minRequiredSecretBytesLength, minRequiredSecretBytesLength * 3)];
        random().nextBytes(hmacKeyBytes);
        return new SecretKeySpec(hmacKeyBytes, "HMAC");
    }

    public static KeyPair generateKeyPair(final String signatureAlgorithm) throws JOSEException {
        if (JwtRealmSettings.SUPPORTED_PUBLIC_KEY_RSA_SIGNATURE_ALGORITHMS.contains(signatureAlgorithm)) {
            return generateRsaKeyPair(signatureAlgorithm);
        } else if (JwtRealmSettings.SUPPORTED_PUBLIC_KEY_EC_SIGNATURE_ALGORITHMS.contains(signatureAlgorithm)) {
            return generateRsaKeyPair(signatureAlgorithm);
        }
        return generateEcKeyPair(signatureAlgorithm);
    }

    // Nimbus JOSE+JWT requires RSA keys to match or exceed 2048-bit strength. No dependency on digest strength.
    public static KeyPair generateRsaKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        final Integer rsaSize = randomFrom(2048, 3072);
        final RSAKey privateKey = new RSAKeyGenerator(rsaSize).generate();
        return new KeyPair(privateKey.toPublicKey(), privateKey.toPrivateKey());
    }

    // Nimbus JOSE+JWT requires EC curves to match digest strength (as per RFC 7519).
    public static KeyPair generateEcKeyPair(final String signatureAlgorithm) throws JOSEException {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        final Curve ecCurve = randomFrom(Curve.forJWSAlgorithm(jwsAlgorithm)); // EC curves by specific SHA-2 lengths.
        final ECKey privateKey = new ECKeyGenerator(ecCurve).generate();
        return new KeyPair(privateKey.toPublicKey(), privateKey.toPrivateKey());
    }

    public static SignedJWT generateValidSignedJWT(final JWSSigner jwsSigner, final String signatureAlgorithm) throws Exception {
        final Tuple<JWSHeader, JWTClaimsSet> headerAndBody = createJwsHeaderAndJwtClaimsSet(
            jwsSigner,
            signatureAlgorithm,
            randomFrom("https://www.example.com/", "") + "iss1" + randomIntBetween(0, 99),
            randomFrom(List.of("rp_client1"), List.of("aud1", "aud2", "aud3")),
            randomFrom("sub", "uid", "name", "dn", "email", "custom"),
            "principal1",
            randomBoolean() ? null : randomFrom("groups", "roles", "other"),
            randomFrom(List.of(""), List.of("grp1"), List.of("rol1", "rol2", "rol3"), List.of("per1"))
        );
        return signSignedJwt(jwsSigner, headerAndBody.v1(), headerAndBody.v2());
    }

    public static SignedJWT signSignedJwt(final JWSSigner jwtSigner, final JWSHeader jwsHeader, final JWTClaimsSet jwtClaimsSet)
        throws JOSEException {
        final SignedJWT signedJwt = new SignedJWT(jwsHeader, jwtClaimsSet);
        signedJwt.sign(jwtSigner);
        return signedJwt;
    }

    public static boolean verifySignedJWT(final JWSVerifier jwtVerifier, final SignedJWT signedJwt) throws Exception {
        return signedJwt.verify(jwtVerifier);
    }

    public static Tuple<JWSHeader, JWTClaimsSet> createJwsHeaderAndJwtClaimsSet(
        final JWSSigner jwtSigner,
        final String signatureAlgorithm,
        final String issuer,
        final List<String> audiences,
        final String principalClaimName,
        final String principalClaimValue,
        final String groupsClaimName,
        final List<String> groupsClaimValue
    ) {
        final JWSAlgorithm jwsAlgorithm = JWSAlgorithm.parse(signatureAlgorithm);
        final JWSHeader jwtHeader = new JWSHeader.Builder(jwsAlgorithm).build();
        final JWTClaimsSet.Builder jwtClaimsSetBuilder = new JWTClaimsSet.Builder().jwtID(
            randomFrom((String) null, randomAlphaOfLengthBetween(1, 20))
        )
            .issueTime(randomFrom((Date) null, Date.from(now().minusSeconds(randomLongBetween(1, 60)))))
            .notBeforeTime(randomFrom((Date) null, Date.from(now().minusSeconds(randomLongBetween(1, 60)))))
            .expirationTime(randomFrom((Date) null, Date.from(now().plusSeconds(randomLongBetween(3600, 7200)))))
            .issuer(issuer)
            .audience(audiences)
            // .subject(subject)
            .claim("nonce", new Nonce());
        if ((Strings.hasText(principalClaimName)) && (principalClaimValue != null)) {
            jwtClaimsSetBuilder.claim(principalClaimName, principalClaimValue.toString());
        }
        if ((Strings.hasText(groupsClaimName)) && (groupsClaimValue != null)) {
            jwtClaimsSetBuilder.claim(groupsClaimName, groupsClaimValue.toString());
        }
        final JWTClaimsSet jwtClaimsSet = jwtClaimsSetBuilder.build();
        final Tuple<JWSHeader, JWTClaimsSet> headerAndBody = new Tuple<>(jwtHeader, jwtClaimsSet);
        return headerAndBody;
    }

    protected Settings.Builder generateRealmSettings(final String name) throws IOException {
        final boolean includeRsa = randomBoolean();
        final boolean includeEc = randomBoolean();
        final boolean includePublicKey = includeRsa || includeEc;
        final boolean includeHmac = randomBoolean() || (includePublicKey == false); // one of HMAC/RSA/EC must be true
        final boolean populateUserMetadata = randomBoolean();
        final Path jwtSetPathObj = PathUtils.get(this.pathHome);
        final String jwkSetPath = randomBoolean()
            ? "https://op.example.com/jwkset.json"
            : Files.createTempFile(jwtSetPathObj, "jwkset.", ".json").toString();

        if (jwkSetPath.equals("https://op.example.com/jwkset.json") == false) {
            Files.writeString(PathUtils.get(jwkSetPath), "Non-empty JWK Set Path contents");
        }
        final String clientAuthorizationType = randomFrom(JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPES);

        final List<String> allowedSignatureAlgorithmsList = new ArrayList<>();
        if (includeRsa) {
            allowedSignatureAlgorithmsList.add(randomFrom(JwtRealmSettings.SUPPORTED_PUBLIC_KEY_RSA_SIGNATURE_ALGORITHMS));
        }
        if (includeEc) {
            allowedSignatureAlgorithmsList.add(randomFrom(JwtRealmSettings.SUPPORTED_PUBLIC_KEY_EC_SIGNATURE_ALGORITHMS));
        }
        if (includeHmac) {
            allowedSignatureAlgorithmsList.add(randomFrom(JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS));
        }
        final String allowedSignatureAlgorithms = allowedSignatureAlgorithmsList.toString(); // Ex: "[HS256,RS384,ES512]"

        final Settings.Builder settingsBuilder = Settings.builder()
            // Issuer settings
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.ALLOWED_ISSUER),
                randomFrom(randomFrom("https://www.example.com/", "") + "iss1" + randomIntBetween(0, 99))
            )
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                allowedSignatureAlgorithms.substring(1, allowedSignatureAlgorithms.length() - 1)
            )
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
            )
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.JWKSET_PATH), includePublicKey ? jwkSetPath : "")
            // Audience settings
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.ALLOWED_AUDIENCES),
                randomFrom("rp_client1", "aud1", "aud2", "aud3")
            )
            // End-user settings
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()),
                randomFrom("sub", "uid", "name", "dn", "email", "custom")
            )
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()),
                randomBoolean() ? null : randomFrom("^(.*)$", "^([^@]+)@example\\.com$")
            )
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.CLAIMS_GROUPS.getClaim()), randomFrom("group", "roles", "other"))
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.CLAIMS_GROUPS.getPattern()),
                randomBoolean() ? null : randomFrom("^(.*)$", "^Group-(.*)$")
            )
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.POPULATE_USER_METADATA), populateUserMetadata)
            // Client settings for incoming connections
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE), clientAuthorizationType)
            // Delegated authorization settings
            .put(
                RealmSettings.getFullSettingKey(name, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                randomBoolean() ? "" : "authz1, authz2"
            )
            // Cache settings
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.CACHE_TTL),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(10, 120) + randomFrom("s", "m", "h")
            )
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.CACHE_MAX_USERS), randomIntBetween(1000, 10000))
            // HTTP settings for outgoing connections
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.HTTP_CONNECT_TIMEOUT),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
            )
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(5, 10) + randomFrom("s", "m", "h")
            )
            .put(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.HTTP_SOCKET_TIMEOUT),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(5, 10) + randomFrom("s", "m", "h")
            )
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.HTTP_MAX_CONNECTIONS), randomIntBetween(5, 20))
            .put(RealmSettings.getFullSettingKey(name, JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS), randomIntBetween(5, 20))
            // TLS settings for outgoing connections
            .put(RealmSettings.getFullSettingKey(name, SSLConfigurationSettings.TRUSTSTORE_TYPE.realm(JwtRealmSettings.TYPE)), "PKCS12")
            .put(RealmSettings.getFullSettingKey(name, SSLConfigurationSettings.TRUSTSTORE_PATH.realm(JwtRealmSettings.TYPE)), "ts2.p12")
            .put(RealmSettings.getFullSettingKey(name, SSLConfigurationSettings.TRUSTSTORE_ALGORITHM.realm(JwtRealmSettings.TYPE)), "PKIX")
            .put(RealmSettings.getFullSettingKey(name, SSLConfigurationSettings.CERT_AUTH_PATH.realm(JwtRealmSettings.TYPE)), "ca2.pem");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        if (includeHmac) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY),
                randomAlphaOfLengthBetween(10, 20)
            );
        }
        if (JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET.equals(clientAuthorizationType)) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(name, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET),
                randomAlphaOfLengthBetween(8, 12)
            );
        }
        secureSettings.setString(
            RealmSettings.getFullSettingKey(name, SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm(JwtRealmSettings.TYPE)),
            randomAlphaOfLengthBetween(10, 10)
        );

        settingsBuilder.setSecureSettings(secureSettings);
        return settingsBuilder;
    }

    protected RealmConfig buildRealmConfig(
        final String realmType,
        final String realmName,
        final Settings realmSettings,
        final Integer realmOrder
    ) {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(realmType, realmName);
        final Settings settings = Settings.builder()
            .put(this.globalSettings)
            // .put("path.home", this.pathHome)
            .put(realmSettings)
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), realmOrder)
            .build();
        return new RealmConfig(realmIdentifier, settings, this.env, this.threadContext);
    }

    protected static void writeJwkSetToFile(Path file) throws IOException {
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

    protected Answer<Class<Void>> getAnswer(AtomicReference<UserRoleMapper.UserData> userData) {
        return invocation -> {
            assert invocation.getArguments().length == 2;
            userData.set((UserRoleMapper.UserData) invocation.getArguments()[0]);
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(new HashSet<>(Arrays.asList("kibana_user", "role1")));
            return null;
        };
    }

    protected UserRoleMapper buildRoleMapper(final String principal, final Set<String> roles) {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            if (userData.getUsername().equals(principal)) {
                listener.onResponse(roles);
            } else {
                listener.onFailure(
                    new IllegalArgumentException("Expected principal '" + principal + "' but was '" + userData.getUsername() + "'")
                );
            }
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        return roleMapper;
    }

    public static class CapturingActionListener<T> implements ActionListener<T> {
        private T result = null;
        private Exception exception = null;

        @Override
        public void onResponse(final T result) {
            this.result = result;
        }

        @Override
        public void onFailure(final Exception exception) {
            this.exception = exception;
        }

        public T getResult() {
            return this.result;
        }

        public Exception getException() {
            return this.exception;
        }
    };

    public static Collection<String> random(final int min, final Collection<String> original) {
        if ((original == null) || (min < 0) || (min > original.size())) {
            throw new IllegalArgumentException("Invalid min=" + min + ", original=" + original);
        }
        return random(min, original.size(), original);
    }

    /**
     * Return collection of random count and elements from the original collection of strings.
     * @param min Minimum elements to retain.
     * @param max Maximum elements to retain.
     * @param original Original list of strings.
     * @return Random count of random elements from the original list.
     */
    public static Collection<String> random(final int min, final int max, final Collection<String> original) {
        if ((original == null) || (min < 0) || (min > original.size()) || (max < 0) || (max > original.size()) || (min > max)) {
            throw new IllegalArgumentException("Invalid min=" + min + ", max=" + max + ", original=" + original);
        }
        final List<String> elements = new ArrayList<>(original); // use list for stable order after shuffle
        Collections.shuffle(elements, random()); // randomize list contents

        final int minSelected = (min > 0) ? min : 0;
        final int maxSelected = (max < elements.size()) ? max : elements.size();
        final int retainCount = randomIntBetween(minSelected, maxSelected);
        final int removeCount = elements.size() - retainCount;

        for (int removed = 0; removed < removeCount; removed++) {
            elements.remove(elements.size() - 1); // remove last skips System.arraycopy()
        }
        return elements;
    }
}
