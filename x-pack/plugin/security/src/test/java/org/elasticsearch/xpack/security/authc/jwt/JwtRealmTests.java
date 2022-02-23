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
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtRealmTests extends JwtTestCase {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealmTests.class);

    record JwtIssuerAndRealm(JwtIssuer issuer, JwtRealm realm) {}

    record MinMax(int min, int max) {
        MinMax {
            assert min >= 0 && max >= min : "Invalid min=" + min + " max=" + max;
        }
    }

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private MockLicenseState licenseState;
    private List<JwtIssuerAndRealm> jwtIssuerAndRealms;

    @Before
    public void init() throws Exception {
        this.threadPool = new TestThreadPool("JWT realm tests");
        this.resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, this.threadPool);
        this.licenseState = mock(MockLicenseState.class);
        when(this.licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws Exception {
        if (this.jwtIssuerAndRealms != null) {
            this.jwtIssuerAndRealms.stream().filter(p -> p.realm != null).forEach(p -> p.realm.close());
        }
        this.resourceWatcherService.close();
        terminate(this.threadPool);
    }

    /**
     * Test with empty roles.
     * @throws Exception Unexpected test failure
     */
    public void testJwtAuthcRealmAuthenticateWithEmptyRoles() throws Exception {
        this.jwtIssuerAndRealms = this.generateJwtIssuerRealmPairs(
            new MinMax(1, 1), // realmsRange
            new MinMax(0, 1), // authzRange
            new MinMax(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size()), // algsRange
            new MinMax(1, 3), // audiencesRange
            new MinMax(1, 3), // usersRange
            new MinMax(0, 0), // rolesRange
            new MinMax(0, 1), // jwtCacheSizeRange
            new MinMax(0, 1) // userCacheSizeRange
        );
        final JwtIssuerAndRealm jwtIssuerAndRealm = this.randomJwtIssuerRealmPair();
        final User user = this.randomUser(jwtIssuerAndRealm.issuer);
        final SecureString jwt = this.randomJwt(jwtIssuerAndRealm, user);
        final SecureString clientSecret = jwtIssuerAndRealm.realm.clientAuthenticationSharedSecret;
        final MinMax jwtAuthcRange = new MinMax(2, 3);
        this.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm, user, jwt, clientSecret, jwtAuthcRange);
    }

    /**
     * Test with no authz realms.
     * @throws Exception Unexpected test failure
     */
    public void testJwtAuthcRealmAuthenticateWithoutAuthzRealms() throws Exception {
        this.jwtIssuerAndRealms = this.generateJwtIssuerRealmPairs(
            new MinMax(1, 3), // realmsRange
            new MinMax(0, 0), // authzRange
            new MinMax(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size()), // algsRange
            new MinMax(1, 3), // audiencesRange
            new MinMax(1, 3), // usersRange
            new MinMax(0, 3), // rolesRange
            new MinMax(0, 1), // jwtCacheSizeRange
            new MinMax(0, 1) // userCacheSizeRange
        );
        final JwtIssuerAndRealm jwtIssuerAndRealm = this.randomJwtIssuerRealmPair();
        final User user = this.randomUser(jwtIssuerAndRealm.issuer);
        final SecureString jwt = this.randomJwt(jwtIssuerAndRealm, user);
        final SecureString clientSecret = jwtIssuerAndRealm.realm.clientAuthenticationSharedSecret;
        final MinMax jwtAuthcRange = new MinMax(2, 3);
        this.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm, user, jwt, clientSecret, jwtAuthcRange);
    }

    /**
     * Test with authz realms.
     * @throws Exception Unexpected test failure
     */
    public void testJwtAuthcRealmAuthenticateWithAuthzRealms() throws Exception {
        this.jwtIssuerAndRealms = this.generateJwtIssuerRealmPairs(
            new MinMax(1, 3), // realmsRange
            new MinMax(0, 3), // authzRange
            new MinMax(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size()), // algsRange
            new MinMax(1, 3), // audiencesRange
            new MinMax(1, 3), // usersRange
            new MinMax(0, 3), // rolesRange
            new MinMax(0, 1), // jwtCacheSizeRange
            new MinMax(0, 1) // userCacheSizeRange
        );
        final JwtIssuerAndRealm jwtIssuerAndRealm = this.randomJwtIssuerRealmPair();
        final User user = this.randomUser(jwtIssuerAndRealm.issuer);
        final SecureString jwt = this.randomJwt(jwtIssuerAndRealm, user);
        final SecureString clientSecret = jwtIssuerAndRealm.realm.clientAuthenticationSharedSecret;
        final MinMax jwtAuthcRange = new MinMax(2, 3);
        this.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm, user, jwt, clientSecret, jwtAuthcRange);
    }

    /**
     * Test token parse failures and authentication failures.
     * @throws Exception Unexpected test failure
     */
    public void testJwtValidationSuccessAndFailure() throws Exception {
        this.jwtIssuerAndRealms = this.generateJwtIssuerRealmPairs(
            new MinMax(1, 1), // realmsRange
            new MinMax(0, 0), // authzRange
            new MinMax(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size()), // algsRange
            new MinMax(1, 1), // audiencesRange
            new MinMax(1, 1), // usersRange
            new MinMax(1, 1), // rolesRange
            new MinMax(0, 1), // jwtCacheSizeRange
            new MinMax(0, 1) // userCacheSizeRange
        );
        final JwtIssuerAndRealm jwtIssuerAndRealm = this.randomJwtIssuerRealmPair();
        final User user = this.randomUser(jwtIssuerAndRealm.issuer);
        final SecureString jwt = this.randomJwt(jwtIssuerAndRealm, user);
        final SecureString clientSecret = jwtIssuerAndRealm.realm.clientAuthenticationSharedSecret;
        final MinMax jwtAuthcRange = new MinMax(2, 3);

        // Indirectly verify authentication works before performing any failure scenarios
        this.multipleRealmsAuthenticateJwtHelper(jwtIssuerAndRealm, user, jwt, clientSecret, jwtAuthcRange);

        {   // Directly verify SUCCESS scenario for token() and authenticate() validation, before checking any failure tests.
            final ThreadContext requestThreadContext = this.createThreadContext(jwt, clientSecret);
            final JwtAuthenticationToken token = (JwtAuthenticationToken) jwtIssuerAndRealm.realm.token(requestThreadContext);
            final PlainActionFuture<AuthenticationResult<User>> plainActionFuture = PlainActionFuture.newFuture();
            jwtIssuerAndRealm.realm.authenticate(token, plainActionFuture);
            assertThat(plainActionFuture.get(), is(notNullValue()));
            assertThat(plainActionFuture.get().isAuthenticated(), is(true));
        }

        // Directly verify FAILURE scenarios for token() parsing and authenticate() validation.

        // Null JWT
        final ThreadContext tc1 = this.createThreadContext(null, clientSecret);
        final Exception e1 = expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc1));
        assertThat(e1.getMessage(), equalTo("JWT bearer token must be non-null"));

        // Empty JWT string
        final ThreadContext tc2 = this.createThreadContext("", clientSecret);
        final Exception e2 = expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc2));
        assertThat(e2.getMessage(), equalTo("JWT bearer token must be non-empty"));

        // Non-empty whitespace JWT string
        final ThreadContext tc3 = this.createThreadContext("", clientSecret);
        final Exception e3 = expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc3));
        assertThat(e3.getMessage(), equalTo("JWT bearer token must be non-empty"));

        // Blank client secret
        final ThreadContext tc4 = this.createThreadContext(jwt, "");
        final Exception e4 = expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc4));
        assertThat(e4.getMessage(), equalTo("Client shared secret must be non-empty"));

        // Non-empty whitespace JWT client secret
        final ThreadContext tc5 = this.createThreadContext(jwt, " ");
        final Exception e5 = expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc5));
        assertThat(e5.getMessage(), equalTo("Client shared secret must be non-empty"));

        // JWT parse exception
        final ThreadContext tc6 = this.createThreadContext("Head.Body.Sig", clientSecret);
        final Exception e6 = expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc6));
        assertThat(e6.getMessage(), equalTo("Failed to parse JWT bearer token"));

        // Parse JWT into three parts, for rejecting testing of tampered JWT contents
        final SignedJWT parsedJwt = SignedJWT.parse(jwt.toString());
        final JWSHeader validHeader = parsedJwt.getHeader();
        final JWTClaimsSet validClaimsSet = parsedJwt.getJWTClaimsSet();
        final Base64URL validSignature = parsedJwt.getSignature();

        {   // Verify rejection of unsigned JWT
            final SecureString unsignedJwt = new SecureString(new PlainJWT(validClaimsSet).serialize().toCharArray());
            final ThreadContext tc = this.createThreadContext(unsignedJwt, clientSecret);
            expectThrows(IllegalArgumentException.class, () -> jwtIssuerAndRealm.realm.token(tc));
        }

        {   // Verify rejection of a tampered header (flip HMAC=>RSA or RSA/EC=>HMAC)
            final String mixupAlg; // Check if there are any algorithms available in the realm for attempting a flip test
            if (JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(validHeader.getAlgorithm().getName())) {
                if (jwtIssuerAndRealm.realm.jwksAlgsPkc.algs().isEmpty()) {
                    mixupAlg = null; // cannot flip HMAC to PKC (no PKC algs available)
                } else {
                    mixupAlg = randomFrom(jwtIssuerAndRealm.realm.jwksAlgsPkc.algs()); // flip HMAC to PKC
                }
            } else {
                if (jwtIssuerAndRealm.realm.jwksAlgsHmac.algs().isEmpty()) {
                    mixupAlg = null; // cannot flip PKC to HMAC (no HMAC algs available)
                } else {
                    mixupAlg = randomFrom(jwtIssuerAndRealm.realm.jwksAlgsHmac.algs()); // flip HMAC to PKC
                }
            }
            // This check can only be executed if there is a flip algorithm available in the realm
            if (Strings.hasText(mixupAlg)) {
                final JWSHeader tamperedHeader = new JWSHeader.Builder(JWSAlgorithm.parse(mixupAlg)).build();
                final SecureString jwtTamperedHeader = super.buildJWT(tamperedHeader, validClaimsSet, validSignature);
                this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, jwtTamperedHeader, clientSecret);
            }
        }

        {   // Verify rejection of a tampered claim set
            final JWTClaimsSet tamperedClaimsSet = new JWTClaimsSet.Builder(validClaimsSet).claim("gr0up", "superuser").build();
            final SecureString jwtTamperedClaimsSet = buildJWT(validHeader, tamperedClaimsSet, validSignature);
            this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, jwtTamperedClaimsSet, clientSecret);
        }

        {   // Verify rejection of a tampered signature
            final SecureString jwtWithTruncatedSignature = new SecureString(jwt.toString().substring(0, jwt.length() - 1).toCharArray());
            this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, jwtWithTruncatedSignature, clientSecret);
        }

        // Get read to re-sign JWTs for time claim failure tests
        final JwtIssuer.AlgJwkPair algJwkPair = randomFrom(jwtIssuerAndRealm.issuer.getAllAlgJwkPairs());
        final JWSHeader jwtHeader = new JWSHeader.Builder(JWSAlgorithm.parse(algJwkPair.alg())).build();
        final JWSSigner jwsSigner = JwtValidateUtil.createJwsSigner(algJwkPair.jwk());
        final Instant now = Instant.now();
        final Date past = Date.from(now.minusSeconds(86400));
        final Date future = Date.from(now.plusSeconds(86400));

        {   // Verify rejection of JWT auth_time > now
            final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder(validClaimsSet).claim("auth_time", future).build();
            final String jwtIatFuture = JwtValidateUtil.signJwt(jwsSigner, new SignedJWT(jwtHeader, claimsSet)).serialize();
            this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, new SecureString(jwtIatFuture.toCharArray()), clientSecret);
        }

        {   // Verify rejection of JWT iat > now
            final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder(validClaimsSet).issueTime(future).build();
            final String jwtIatFuture = JwtValidateUtil.signJwt(jwsSigner, new SignedJWT(jwtHeader, claimsSet)).serialize();
            this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, new SecureString(jwtIatFuture.toCharArray()), clientSecret);
        }

        {   // Verify rejection of JWT nbf > now
            final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder(validClaimsSet).notBeforeTime(future).build();
            final String jwtIatFuture = JwtValidateUtil.signJwt(jwsSigner, new SignedJWT(jwtHeader, claimsSet)).serialize();
            this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, new SecureString(jwtIatFuture.toCharArray()), clientSecret);
        }

        {   // Verify rejection of JWT now > exp
            final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder(validClaimsSet).expirationTime(past).build();
            final String jwtExpPast = JwtValidateUtil.signJwt(jwsSigner, new SignedJWT(jwtHeader, claimsSet)).serialize();
            this.verifyAuthenticateFailureHelper(jwtIssuerAndRealm, new SecureString(jwtExpPast.toCharArray()), clientSecret);
        }
    }

    private void verifyAuthenticateFailureHelper(
        final JwtIssuerAndRealm jwtIssuerAndRealm,
        final SecureString jwt,
        final SecureString clientSecret
    ) throws InterruptedException, ExecutionException {
        final ThreadContext tc = this.createThreadContext(jwt, clientSecret);
        final JwtAuthenticationToken token = (JwtAuthenticationToken) jwtIssuerAndRealm.realm.token(tc);
        final PlainActionFuture<AuthenticationResult<User>> plainActionFuture = PlainActionFuture.newFuture();
        jwtIssuerAndRealm.realm.authenticate(token, plainActionFuture);
        assertThat(plainActionFuture.get(), is(notNullValue()));
        assertThat(plainActionFuture.get().isAuthenticated(), is(false));
    }

    private List<JwtIssuerAndRealm> generateJwtIssuerRealmPairs(
        MinMax realmsRange,
        MinMax authzRange,
        MinMax algsRange,
        MinMax audiencesRange,
        MinMax usersRange,
        MinMax rolesRange,
        MinMax jwtCacheSizeRange,
        MinMax userCacheSizeRange
    ) throws Exception {
        assertThat(realmsRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(authzRange.min(), is(greaterThanOrEqualTo(0)));
        assertThat(algsRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(audiencesRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(usersRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(rolesRange.min(), is(greaterThanOrEqualTo(0)));
        assertThat(jwtCacheSizeRange.min(), is(greaterThanOrEqualTo(0)));
        assertThat(userCacheSizeRange.min(), is(greaterThanOrEqualTo(0)));

        // Create JWT authc realms and mocked authz realms. Initialize each JWT realm, and test ensureInitialized() before and after.
        final int realmsCount = randomIntBetween(realmsRange.min(), realmsRange.max());
        final List<Realm> allRealms = new ArrayList<>(); // authc and authz realms
        this.jwtIssuerAndRealms = new ArrayList<>(realmsCount);
        for (int i = 0; i < realmsCount; i++) {
            final int authzCount = randomIntBetween(authzRange.min(), authzRange.max());
            final int algsCount = randomIntBetween(algsRange.min(), algsRange.max());
            final int audiencesCount = randomIntBetween(audiencesRange.min(), audiencesRange.max());
            final int usersCount = randomIntBetween(usersRange.min(), usersRange.max());
            final int rolesCount = randomIntBetween(rolesRange.min(), rolesRange.max());
            final int jwtCacheSize = randomIntBetween(jwtCacheSizeRange.min(), jwtCacheSizeRange.max());
            final int usersCacheSize = randomIntBetween(userCacheSizeRange.min(), userCacheSizeRange.max());

            final JwtIssuer jwtIssuer = this.createJwtIssuer(i, algsCount, audiencesCount, usersCount, rolesCount);
            final JwtRealm jwtRealm = this.createJwtRealm(allRealms, jwtIssuer, authzCount, jwtCacheSize, usersCacheSize);
            this.jwtIssuerAndRealms.add(new JwtIssuerAndRealm(jwtIssuer, jwtRealm));

            // verify exception before initialize()
            final Exception exception = expectThrows(IllegalStateException.class, jwtRealm::ensureInitialized);
            assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));
        }
        allRealms.forEach(realm -> realm.initialize(allRealms, this.licenseState)); // JWT realms and authz realms
        this.jwtIssuerAndRealms.forEach(p -> p.realm.ensureInitialized()); // verify no exception after initialize()
        return this.jwtIssuerAndRealms;
    }

    private JwtIssuer createJwtIssuer(final int i, final int algsCount, final int audiencesCount, final int userCount, final int roleCount)
        throws JOSEException {
        final String issuer = "iss" + (i + 1) + "_" + randomIntBetween(0, 9999);

        // Allow algorithm repeats, to cover testing of multiple JWKs for same algorithm
        final List<String> algs = randomOfMinMaxNonUnique(algsCount, algsCount, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final List<String> algsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();
        final List<String> algsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
        final List<JwtIssuer.AlgJwkPair> algJwkPairsPkc = JwtTestCase.randomJwks(algsPkc);
        // Key setting vs JWKSet setting are mutually exclusive, do not populate both
        final List<JwtIssuer.AlgJwkPair> algJwkPairsHmac = new ArrayList<>(JwtTestCase.randomJwks(algsHmac)); // allow remove/add below
        final JwtIssuer.AlgJwkPair algJwkPairHmacOidc;
        if ((algJwkPairsHmac.size() == 0) || (randomBoolean())) {
            algJwkPairHmacOidc = null; // list(0||1||N) => Key=null and JWKSet(N)
        } else {
            // Change one of the HMAC random bytes keys to an OIDC UTF8 key. Put it in either the Key setting or JWKSet setting.
            final JwtIssuer.AlgJwkPair algJwkPairRandomBytes = algJwkPairsHmac.get(0);
            final OctetSequenceKey jwkHmacRandomBytes = JwtTestCase.conditionJwkHmacForOidc((OctetSequenceKey) algJwkPairRandomBytes.jwk());
            final JwtIssuer.AlgJwkPair algJwkPairUtf8Bytes = new JwtIssuer.AlgJwkPair(algJwkPairRandomBytes.alg(), jwkHmacRandomBytes);
            if ((algJwkPairsHmac.size() == 1) && (randomBoolean())) {
                algJwkPairHmacOidc = algJwkPairUtf8Bytes; // list(1) => Key=OIDC and JWKSet(0)
                algJwkPairsHmac.remove(0);
            } else {
                algJwkPairHmacOidc = null; // list(N) => Key=null and JWKSet(OIDC+N-1)
                algJwkPairsHmac.set(0, algJwkPairUtf8Bytes);
            }
        }

        final List<String> audiences = IntStream.range(0, audiencesCount).mapToObj(j -> issuer + "_aud" + (j + 1)).toList();
        final Map<String, User> users = JwtTestCase.generateTestUsersWithRoles(userCount, roleCount);
        return new JwtIssuer(issuer, audiences, algJwkPairsPkc, algJwkPairsHmac, algJwkPairHmacOidc, users);
    }

    private JwtRealm createJwtRealm(
        final List<Realm> allRealms, // JWT realms and authz realms
        final JwtIssuer jwtIssuer,
        final int authzCount,
        final int jwtCacheSize,
        final int usersCacheSize
    ) throws Exception {
        final String authcRealmName = "realm_" + jwtIssuer.issuer;
        final String[] authzRealmNames = IntStream.range(0, authzCount).mapToObj(z -> authcRealmName + "_authz" + z).toArray(String[]::new);

        final String clientAuthenticationType = randomFrom(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPES);
        final Settings.Builder authcSettings = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuer)
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.getAllAlgorithms())
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.PKC_JWKSET_PATH),
                saveJwkSetToTempFile(jwtIssuer.getJwkSetPkc(), true)
            )
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_AUDIENCES), randomFrom(jwtIssuer.audiences))
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE), clientAuthenticationType)
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()),
                randomBoolean() ? "sub" : authcRealmName + "_sub"
            )
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^(.*)$")
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.POPULATE_USER_METADATA), randomBoolean())
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                String.join(",", authzRealmNames)
            );
        if (randomBoolean()) {
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getClaim()),
                authcRealmName + "_groups"
            );
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$");
        }
        // JWT authc realm secure settings
        final MockSecureSettings secureSettings = new MockSecureSettings();
        if (jwtIssuer.algAndJwksHmac.isEmpty() == false) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.HMAC_JWKSET),
                JwtUtil.serializeJwkSet(jwtIssuer.getJwkSetHmac(), false)
            );
        }
        if (jwtIssuer.algAndJwkHmacOidc != null) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.HMAC_KEY),
                new String(jwtIssuer.algAndJwkHmacOidc.jwk().toOctetSequenceKey().toByteArray(), StandardCharsets.UTF_8)
            );
        }
        if (clientAuthenticationType.equals(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET)) {
            final String clientAuthenticationSharedSecret = Base64.getUrlEncoder().encodeToString(randomByteArrayOfLength(32));
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
                clientAuthenticationSharedSecret
            );
        }
        authcSettings.setSecureSettings(secureSettings);

        final RealmConfig authcConfig = super.buildRealmConfig(
            JwtRealmSettings.TYPE,
            authcRealmName,
            authcSettings.build(),
            (allRealms.size() + 1)
        );
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(authcSettings.build()));
        final UserRoleMapper userRoleMapper = super.buildRoleMapper((authzRealmNames.length >= 1) ? Map.of() : jwtIssuer.users);

        // If authz names is not set, register the users here in the JWT authc realm.
        final JwtRealm jwtRealm = new JwtRealm(authcConfig, sslService, userRoleMapper);
        allRealms.add(jwtRealm);

        // If authz names is set, register the users here in one of the authz realms.
        if (authzRealmNames.length >= 1) {
            final int selected = randomIntBetween(0, authzRealmNames.length - 1);
            for (int i = 0; i < authzRealmNames.length; i++) {
                final RealmConfig authzConfig = this.buildRealmConfig("authz", authzRealmNames[i], Settings.EMPTY, allRealms.size() + 1);
                final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
                if (i == selected) {
                    jwtIssuer.users.values().forEach(authzRealm::registerUser);
                }
                allRealms.add(authzRealm);
            }
        }
        return jwtRealm;
    }

    private JwtIssuerAndRealm randomJwtIssuerRealmPair() {
        // Select random JWT issuer and JWT realm pair, and log the realm settings
        assertThat(this.jwtIssuerAndRealms, is(notNullValue()));
        assertThat(this.jwtIssuerAndRealms, is(not(empty())));
        final JwtIssuerAndRealm jwtIssuerAndRealm = randomFrom(this.jwtIssuerAndRealms);
        final JwtRealm jwtRealm = jwtIssuerAndRealm.realm;
        assertThat(jwtRealm, is(notNullValue()));
        assertThat(jwtRealm.allowedIssuer, is(equalTo(jwtIssuerAndRealm.issuer.issuer))); // assert equal, don't print both
        assertThat(jwtIssuerAndRealm.issuer.audiences.stream().anyMatch(jwtRealm.allowedAudiences::contains), is(true));
        LOGGER.info(
            "REALM["
                + jwtRealm.name()
                + ","
                + jwtRealm.order()
                + "/"
                + this.jwtIssuerAndRealms.size()
                + "], iss=["
                + jwtIssuerAndRealm.issuer
                + "], iss.aud="
                + jwtIssuerAndRealm.issuer.audiences
                + ", realm.aud="
                + jwtRealm.allowedAudiences
                + ", HMAC alg="
                + jwtRealm.jwksAlgsHmac.algs()
                + ", PKC alg="
                + jwtRealm.jwksAlgsPkc.algs()
                + ", client=["
                + jwtRealm.clientAuthenticationType
                + "], meta=["
                + jwtRealm.populateUserMetadata
                + "], authz=["
                + jwtRealm.delegatedAuthorizationSupport.hasDelegation()
                + "], jwkSetPath=["
                + jwtRealm.jwkSetPath
                + "], claimPrincipal=["
                + jwtRealm.claimParserPrincipal.getClaimName()
                + "], claimGroups=["
                + jwtRealm.claimParserGroups.getClaimName()
                + "], clientAuthenticationSharedSecret=["
                + jwtRealm.clientAuthenticationSharedSecret
                + "], authz=["
                + jwtRealm.delegatedAuthorizationSupport
                + "]"
        );
        return jwtIssuerAndRealm;
    }

    private User randomUser(JwtIssuer jwtIssuer) {
        final User user = randomFrom(jwtIssuer.users.values());
        LOGGER.info("USER[" + user.principal() + "]: roles=[" + String.join(",", user.roles()) + "].");
        return user;
    }

    private SecureString randomJwt(final JwtIssuerAndRealm jwtIssuerAndRealm, User user) throws Exception {
        final JwtIssuer.AlgJwkPair algJwkPair = randomFrom(jwtIssuerAndRealm.issuer.getAllAlgJwkPairs());
        LOGGER.info("JWK=[" + algJwkPair.jwk().getKeyType() + "/" + algJwkPair.jwk().size() + "], alg=[" + algJwkPair.alg() + "].");

        // JWT needs user settings (i.e. principal, roles), and realm settings (ex: iss, aud, principal/groups claim parsers).
        final SignedJWT signedJWT = this.buildSignedJWT(jwtIssuerAndRealm.realm, user, algJwkPair.alg(), algJwkPair.jwk());
        return new SecureString(signedJWT.serialize().toCharArray());
    }

    private SignedJWT buildSignedJWT(final JwtRealm jwtRealm, final User user, final String signatureAlgorithm, final JWK jwk)
        throws Exception {
        final SignedJWT unsignedJwt = JwkValidateUtilTests.randomValidJwsHeaderAndJwtClaimsSet(
            signatureAlgorithm,
            jwtRealm.allowedIssuer,
            jwtRealm.allowedAudiences,
            jwtRealm.claimParserPrincipal.getClaimName(),
            user.principal(),
            jwtRealm.claimParserGroups.getClaimName(),
            List.of(user.roles()),
            Map.of("metadata", randomAlphaOfLength(10))
        );
        final JWSSigner jwsSigner = JwtValidateUtil.createJwsSigner(jwk);
        final SignedJWT signedJWT = JwtValidateUtil.signJwt(jwsSigner, unsignedJwt);
        final JWSVerifier jwkVerifier = JwtValidateUtil.createJwsVerifier(jwk);
        assertThat(JwtValidateUtil.verifyJWT(jwkVerifier, signedJWT), is(equalTo(true)));
        return signedJWT;
    }

    private void multipleRealmsAuthenticateJwtHelper(
        final JwtIssuerAndRealm jwtIssuerAndRealm,
        final User user,
        final SecureString jwt,
        final SecureString sharedSecret,
        final MinMax jwtAuthcRange
    ) throws Exception {
        assertThat(jwtAuthcRange.min(), is(greaterThanOrEqualTo(1)));

        // Select one JWT authc Issuer/Realm pair. Select one test user, to use inside the authc test loop.
        final List<JwtRealm> allJwtRealms = this.jwtIssuerAndRealms.stream().map(p -> p.realm).toList();

        // Select different test JWKs from the JWT realm, and generate test JWTs for the test user. Run the JWT through the chain.
        final int jwtAuthcRepeats = randomIntBetween(jwtAuthcRange.min(), jwtAuthcRange.max());
        for (int authcRun = 1; authcRun <= jwtAuthcRepeats; authcRun++) {
            // Create request with headers set
            LOGGER.info("RUN[" + authcRun + "/" + jwtAuthcRepeats + "], jwt=[" + jwt + "], secret=[" + sharedSecret + "].");
            final ThreadContext requestThreadContext = this.createThreadContext(jwt, sharedSecret);

            // Loop through all authc/authz realms. Confirm a JWT authc realm recognizes and extracts the request headers.
            JwtAuthenticationToken jwtAuthenticationToken = null;
            for (final JwtRealm candidateJwtRealm : allJwtRealms) {
                final AuthenticationToken authenticationToken = candidateJwtRealm.token(requestThreadContext);
                if (authenticationToken != null) {
                    assertThat(authenticationToken, isA(JwtAuthenticationToken.class));
                    jwtAuthenticationToken = (JwtAuthenticationToken) authenticationToken;
                    break;
                }
            }
            assertThat(jwtAuthenticationToken, is(notNullValue()));
            final String tokenPrincipal = jwtAuthenticationToken.principal();
            final SecureString tokenJwt = jwtAuthenticationToken.getEndUserSignedJwt();
            final SecureString tokenSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();
            assertThat(tokenPrincipal, is(notNullValue()));
            if (tokenJwt.equals(jwt) == false) {
                assertThat(tokenJwt, is(equalTo(jwt)));
            }
            assertThat(tokenJwt, is(equalTo(jwt)));
            if (tokenSecret != null) {
                if (tokenSecret.equals(sharedSecret) == false) {
                    assertThat(tokenSecret, is(equalTo(sharedSecret)));
                }
                assertThat(tokenSecret, is(equalTo(sharedSecret)));
            }
            LOGGER.info("TOKEN[" + tokenPrincipal + "]: jwt=[" + tokenJwt + "], secret=[" + tokenSecret + "].");

            // Loop through all authc/authz realms. Confirm authenticatedUser is returned with expected principal and roles.
            User authenticatedUser = null;
            final List<String> realmAuthenticationResults = new ArrayList<>();
            final List<String> realmUsageStats = new ArrayList<>();
            final List<Exception> realmFailureExceptions = new ArrayList<>(allJwtRealms.size());
            try {
                for (final JwtRealm candidateJwtRealm : allJwtRealms) {
                    final PlainActionFuture<AuthenticationResult<User>> authenticateFuture = PlainActionFuture.newFuture();
                    try {
                        candidateJwtRealm.authenticate(jwtAuthenticationToken, authenticateFuture);
                        final AuthenticationResult<User> authenticationResult = authenticateFuture.actionGet();
                        final Exception authenticationResultException = authenticationResult.getException();
                        final String realmResult = "  realms=["
                            + allJwtRealms.size()
                            + "], expected=["
                            + jwtIssuerAndRealm.realm.name()
                            + ","
                            + jwtIssuerAndRealm.realm.order()
                            + "], current["
                            + candidateJwtRealm.name()
                            + ","
                            + candidateJwtRealm.order()
                            + "], status=["
                            + authenticationResult.getStatus()
                            + "], authenticated=["
                            + authenticationResult.isAuthenticated()
                            + "], msg=["
                            + authenticationResult.getMessage()
                            + "], meta=["
                            + authenticationResult.getMetadata()
                            + "], user=["
                            + authenticationResult.getValue()
                            + "].";
                        realmAuthenticationResults.add(realmResult);
                        realmFailureExceptions.add(new Exception(realmResult, authenticationResultException));
                        switch (authenticationResult.getStatus()) {
                            case SUCCESS:
                                assertThat(candidateJwtRealm.name(), is(equalTo(jwtIssuerAndRealm.realm.name())));
                                assertThat(authenticationResult.isAuthenticated(), is(equalTo(true)));
                                assertThat(authenticationResult.getException(), is(nullValue()));
                                assertThat(authenticationResult.getMessage(), is(nullValue()));
                                assertThat(authenticationResult.getMetadata(), is(anEmptyMap()));
                                authenticatedUser = authenticationResult.getValue();
                                assertThat(authenticatedUser, is(notNullValue()));
                                break;
                            case CONTINUE:
                                assertThat(candidateJwtRealm.name(), is(not(equalTo(jwtIssuerAndRealm.realm.name()))));
                                assertThat(authenticationResult.isAuthenticated(), is(equalTo(false)));
                                continue;
                            case TERMINATE:
                                assertThat(candidateJwtRealm.name(), is(not(equalTo(jwtIssuerAndRealm.realm.name()))));
                                assertThat(authenticationResult.isAuthenticated(), is(equalTo(false)));
                                break;
                            default:
                                fail("Unexpected AuthenticationResult.Status=[" + authenticationResult.getStatus() + "]");
                                break;
                        }
                        break; // Only SUCCESS falls through to here, break out of the loop
                    } catch (Exception e) {
                        realmFailureExceptions.add(new Exception("Caught Exception.", e));
                    } finally {
                        final PlainActionFuture<Map<String, Object>> usageStatsFuture = PlainActionFuture.newFuture();
                        candidateJwtRealm.usageStats(usageStatsFuture);
                        realmUsageStats.add(
                            "   realm["
                                + candidateJwtRealm.name()
                                + ","
                                + candidateJwtRealm.order()
                                + "/"
                                + allJwtRealms.size()
                                + "], stats=["
                                + usageStatsFuture.actionGet()
                                + "]"
                        );
                    }
                }
                // Loop ended. Confirm authenticatedUser is returned with expected principal and roles.
                assertThat("Expected realm " + jwtIssuerAndRealm.realm.name() + " to authenticate.", authenticatedUser, is(notNullValue()));
                assertThat(user.principal(), equalTo(authenticatedUser.principal()));
                assertThat(new TreeSet<>(Arrays.asList(user.roles())), equalTo(new TreeSet<>(Arrays.asList(authenticatedUser.roles()))));
                if (jwtIssuerAndRealm.realm.delegatedAuthorizationSupport.hasDelegation()) {
                    assertThat(user.metadata(), is(equalTo(authenticatedUser.metadata()))); // delegated authz returns user's
                                                                                            // metadata
                } else if (jwtIssuerAndRealm.realm.populateUserMetadata) {
                    assertThat(authenticatedUser.metadata(), is(not(anEmptyMap()))); // role mapping with flag true returns non-empty
                } else {
                    assertThat(authenticatedUser.metadata(), is(anEmptyMap())); // role mapping with flag false returns empty
                }
            } catch (Throwable t) {
                final Exception authcFailed = new Exception("Authentication test failed.");
                realmFailureExceptions.forEach(authcFailed::addSuppressed); // realm exceptions
                authcFailed.addSuppressed(t); // final throwable (ex: assertThat)
                LOGGER.error("Unexpected exception.", authcFailed);
                throw authcFailed;
            } finally {
                LOGGER.info("STATS: expected=[" + jwtIssuerAndRealm.realm.name() + "]\n" + String.join("\n", realmUsageStats));
                if (authenticatedUser != null) {
                    LOGGER.info(
                        "RESULT: expected=[" + jwtIssuerAndRealm.realm.name() + "]\n" + String.join("\n", realmAuthenticationResults)
                    );
                }
            }
        }
        LOGGER.info("Test succeeded");
    }

    private ThreadContext createThreadContext(final CharSequence jwt, final CharSequence sharedSecret) {
        final ThreadContext requestThreadContext = new ThreadContext(this.globalSettings);
        if (jwt != null) {
            requestThreadContext.putHeader(
                JwtRealm.HEADER_END_USER_AUTHENTICATION,
                JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + jwt
            );
        }
        if (sharedSecret != null) {
            requestThreadContext.putHeader(
                JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET + " " + sharedSecret
            );
        }
        return requestThreadContext;
    }

    private String saveJwkSetToTempFile(final JWKSet jwksetPkc, final boolean publicKeysOnly) throws IOException {
        final String serializedJwkSet = JwtUtil.serializeJwkSet(jwksetPkc, publicKeysOnly);
        if (serializedJwkSet == null) {
            return null;
        }
        final Path path = Files.createTempFile(PathUtils.get(this.pathHome), "jwkset.", ".json");
        Files.writeString(path, serializedJwkSet);
        return path.toString();
    }

}
