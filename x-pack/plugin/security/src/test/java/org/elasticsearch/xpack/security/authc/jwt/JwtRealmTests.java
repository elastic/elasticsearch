/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.JWTClaimsSet;
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
import org.elasticsearch.core.Tuple;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
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

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private MockLicenseState licenseState;

    @Before
    public void init() throws Exception {
        this.threadPool = new TestThreadPool("JWT realm tests");
        this.resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, this.threadPool);
        this.licenseState = mock(MockLicenseState.class);
        when(this.licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        this.resourceWatcherService.close();
        terminate(this.threadPool);
    }

    public void testJwtAuthcRealmAuthenticateWithEmptyRoles() throws Exception {
        final int repeats = 10;
        for (int repeat = 0; repeat < repeats; repeat++) {
            final Tuple<Integer, Integer> authcRealmsRange = new Tuple<>(1, 1);
            final Tuple<Integer, Integer> authzRealmsRange = new Tuple<>(0, 0);
            final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 0);
            final Tuple<Integer, Integer> authcRepeatsRange = new Tuple<>(2, 3);
            this.realmTestHelper(authcRealmsRange, authzRealmsRange, audiencesRange, rolesRange, authcRepeatsRange);
        }
    }

    public void testJwtAuthcRealmAuthenticateWithoutAuthzRealms() throws Exception {
        final int repeats = 10;
        for (int repeat = 0; repeat < repeats; repeat++) {
            final Tuple<Integer, Integer> authcRealmsRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> authzRealmsRange = new Tuple<>(0, 0);
            final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 3);
            final Tuple<Integer, Integer> authcRepeatsRange = new Tuple<>(2, 3);
            this.realmTestHelper(authcRealmsRange, authzRealmsRange, audiencesRange, rolesRange, authcRepeatsRange);
        }
    }

    public void testJwtAuthcRealmAuthenticateWithAuthzRealms() throws Exception {
        final int repeats = 10;
        for (int repeat = 0; repeat < repeats; repeat++) {
            final Tuple<Integer, Integer> authcRealmsRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> authzRealmsRange = new Tuple<>(0, 3);
            final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 3);
            final Tuple<Integer, Integer> authcRepeatsRange = new Tuple<>(2, 3);
            this.realmTestHelper(authcRealmsRange, authzRealmsRange, audiencesRange, rolesRange, authcRepeatsRange);
        }
    }

    public void realmTestHelper(
        final Tuple<Integer, Integer> authcRealmsRange,
        final Tuple<Integer, Integer> authzRealmsRange,
        final Tuple<Integer, Integer> audiencesRange,
        final Tuple<Integer, Integer> rolesRange,
        final Tuple<Integer, Integer> authcRepeatsRange
    ) throws Exception {
        assertThat(authcRealmsRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(audiencesRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(authcRepeatsRange.v1(), is(greaterThanOrEqualTo(1)));

        final int realmCount = randomIntBetween(authcRealmsRange.v1(), authcRealmsRange.v2());
        final List<Realm> allRealms = new ArrayList<>();
        try {
            // Create JWT authc realms and mocked authz realms. Initialize each JWT realm, and test ensureInitialized() before and after.
            for (int i = 0; i < realmCount; i++) {
                final int audCount = randomIntBetween(audiencesRange.v1(), audiencesRange.v2());
                final int algCount = randomIntBetween(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size() - 1);
                final String issuer = "iss" + (i + 1) + randomIntBetween(0, 9999);
                final List<String> audiences = IntStream.range(0, audCount)
                    .mapToObj(j -> "aud" + (j + 1) + randomIntBetween(0, 999))
                    .toList();
                final List<String> signatureAlgorithms = randomSubsetOf(algCount, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
                final JwtRealm jwtRealm = addJwtRealm(allRealms, issuer, audiences, signatureAlgorithms, authzRealmsRange, rolesRange);
                // verify exception before initialization
                final Exception exception = expectThrows(IllegalStateException.class, jwtRealm::ensureInitialized);
                assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));
            }
            allRealms.forEach(r -> r.initialize(allRealms, this.licenseState));
            allRealms.forEach(r -> {
                if (r instanceof JwtRealm j) {
                    j.ensureInitialized();
                }
            });

            // Select one of the JWT authc Realms.
            final JwtRealm jwtRealm = (JwtRealm) randomFrom(allRealms.stream().filter(e -> e instanceof JwtRealm).toList());
            LOGGER.info(
                "REALM["
                    + jwtRealm.name()
                    + ","
                    + jwtRealm.order()
                    + "/"
                    + realmCount
                    + "], iss=["
                    + jwtRealm.allowedIssuer
                    + "], aud="
                    + jwtRealm.allowedAudiences
                    + "], alg="
                    + jwtRealm.allowedSignatureAlgorithms
                    + ", client=["
                    + jwtRealm.clientAuthorizationType
                    + "], meta=["
                    + jwtRealm.populateUserMetadata
                    + "], jwkSetPath=["
                    + jwtRealm.jwkSetPath
                    + "], claimPrincipal=["
                    + jwtRealm.claimParserPrincipal.getClaimName()
                    + "], claimGroups=["
                    + jwtRealm.claimParserGroups.getClaimName()
                    + "], clientAuthorizationSharedSecret=["
                    + jwtRealm.clientAuthorizationSharedSecret
                    + "], authz=["
                    + jwtRealm.delegatedAuthorizationSupport
                    + "]"
            );

            // Select one of the test users from the JWT realm, to use inside the authc test loop.
            final User expectedUser = randomFrom(jwtRealm.testIssuerUsers.values());
            LOGGER.info("USER[" + expectedUser.principal() + "]: roles=[" + String.join(",", expectedUser.roles()) + "].");

            // Select different test JWKs from the JWT realm, and generate test JWTs for the test user. Run the JWT through the chain.
            final int authcRepeats = randomIntBetween(authcRepeatsRange.v1(), authcRepeatsRange.v2());
            for (int authcRun = 1; authcRun <= authcRepeats; authcRun++) {
                final int indexOfJwkAndJwsAlgorithm = randomIntBetween(0, jwtRealm.testIssuerJwks.size() - 1);
                final JWK jwk = jwtRealm.testIssuerJwks.get(indexOfJwkAndJwsAlgorithm);
                final String allowedSignatureAlgorithm = jwtRealm.allowedSignatureAlgorithms.get(indexOfJwkAndJwsAlgorithm);
                final String jwkDesc = jwk.getKeyType() + "/" + jwk.size();
                LOGGER.info("RUN[" + authcRun + "/" + authcRepeats + "]: jwk=[" + jwkDesc + "], alg=[" + allowedSignatureAlgorithm + "].");
                final JWSSigner jwsSigner = JwtUtil.createJwsSigner(jwk);
                final Tuple<JWSHeader, JWTClaimsSet> jwsHeaderAndJwtClaimsSet = JwtUtilTests.randomValidJwsHeaderAndJwtClaimsSet(
                    allowedSignatureAlgorithm,
                    jwtRealm.allowedIssuer,
                    jwtRealm.allowedAudiences,
                    jwtRealm.claimParserPrincipal.getClaimName(),
                    expectedUser.principal(),
                    jwtRealm.claimParserGroups.getClaimName(),
                    List.of(expectedUser.roles()),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                );
                final SignedJWT signedJWT = JwtUtil.signSignedJwt(jwsSigner, jwsHeaderAndJwtClaimsSet.v1(), jwsHeaderAndJwtClaimsSet.v2());
                final JWSVerifier jwkVerifier = JwtUtil.createJwsVerifier(jwk);
                assertThat(JwtUtil.verifySignedJWT(jwkVerifier, signedJWT), is(equalTo(true)));
                final SecureString headerJwt = new SecureString(signedJWT.serialize().toCharArray());
                final SecureString headerSecret = jwtRealm.clientAuthorizationSharedSecret;
                LOGGER.info("HEADERS: jwt=[" + headerJwt + "], secret=[" + headerSecret + "].");

                final ThreadContext requestThreadContext = new ThreadContext(this.globalSettings);
                requestThreadContext.putHeader(
                    JwtRealmSettings.HEADER_END_USER_AUTHORIZATION,
                    JwtRealmSettings.HEADER_END_USER_AUTHORIZATION_SCHEME + " " + headerJwt
                );
                if (headerSecret != null) {
                    requestThreadContext.putHeader(
                        JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION,
                        JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET + " " + headerSecret
                    );
                }

                // Loop through all authc/authz realms. Confirm a JWT authc realm recognizes and extracts the request headers.
                JwtAuthenticationToken jwtAuthenticationToken = null;
                for (final Realm realm : allRealms) {
                    final AuthenticationToken authenticationToken = realm.token(requestThreadContext);
                    if (authenticationToken != null) {
                        assertThat(authenticationToken, isA(JwtAuthenticationToken.class));
                        jwtAuthenticationToken = (JwtAuthenticationToken) authenticationToken;
                        break;
                    }
                }
                assertThat(jwtAuthenticationToken, is(notNullValue()));
                final String tokenPrincipal = jwtAuthenticationToken.principal();
                final SecureString tokenJwt = jwtAuthenticationToken.getEndUserSignedJwt();
                final SecureString tokenSecret = jwtAuthenticationToken.getClientAuthorizationSharedSecret();
                assertThat(tokenPrincipal, is(notNullValue()));
                if (tokenJwt.equals(headerJwt) == false) {
                    assertThat(tokenJwt, is(equalTo(headerJwt)));
                }
                assertThat(tokenJwt, is(equalTo(headerJwt)));
                if (tokenSecret != null) {
                    if (tokenSecret.equals(headerSecret) == false) {
                        assertThat(tokenSecret, is(equalTo(headerSecret)));
                    }
                    assertThat(tokenSecret, is(equalTo(headerSecret)));
                }
                LOGGER.info("TOKEN[" + tokenPrincipal + "]: jwt=[" + tokenJwt + "], secret=[" + tokenSecret + "].");

                // Loop through all authc/authz realms. Confirm authenticatedUser is returned with expected principal and roles.
                User authenticatedUser = null;
                final List<String> realmResults = new ArrayList<>();
                final Exception realmExceptions = new Exception("Authentication failed.");
                final List<String> realmUsageStats = new ArrayList<>();
                try {
                    for (final Realm realm : allRealms) {
                        final PlainActionFuture<AuthenticationResult<User>> authenticateFuture = PlainActionFuture.newFuture();
                        try {
                            realm.authenticate(jwtAuthenticationToken, authenticateFuture);
                            final AuthenticationResult<User> authenticationResult = authenticateFuture.actionGet();
                            final Exception authenticationResultException = authenticationResult.getException();
                            final String realmResult = "   realm["
                                + realm.name()
                                + ","
                                + jwtRealm.order()
                                + "/"
                                + realmCount
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
                            realmResults.add(realmResult);
                            realmExceptions.addSuppressed(new Exception(realmResult, authenticationResultException));
                            switch (authenticationResult.getStatus()) {
                                case SUCCESS:
                                    assertThat(authenticationResult.isAuthenticated(), is(equalTo(true)));
                                    assertThat(authenticationResult.getException(), is(nullValue()));
                                    assertThat(authenticationResult.getStatus(), is(equalTo(AuthenticationResult.Status.SUCCESS)));
                                    assertThat(authenticationResult.getMessage(), is(nullValue()));
                                    assertThat(authenticationResult.getMetadata(), is(anEmptyMap()));
                                    authenticatedUser = authenticationResult.getValue();
                                    assertThat(authenticatedUser, is(notNullValue()));
                                    break;
                                case CONTINUE:
                                    continue;
                                case TERMINATE:
                                default:
                                    fail("Unexpected AuthenticationResult.Status=[" + authenticationResult.getStatus() + "]");
                                    break;
                            }
                            break; // Only SUCCESS falls through to here, break out of the loop
                        } catch (Exception e) {
                            realmExceptions.addSuppressed(new Exception("Caught Exception.", e));
                        } finally {
                            final PlainActionFuture<Map<String, Object>> usageStatsFuture = PlainActionFuture.newFuture();
                            realm.usageStats(usageStatsFuture);
                            realmUsageStats.add(
                                "   realm["
                                    + realm.name()
                                    + ","
                                    + jwtRealm.order()
                                    + "/"
                                    + realmCount
                                    + "], stats=["
                                    + usageStatsFuture.actionGet()
                                    + "]"
                            );
                        }
                    }
                    // Loop ended. Confirm authenticatedUser is returned with expected principal and roles.
                    assertThat("Expected realm " + jwtRealm.name() + " to authenticate.", authenticatedUser, is(notNullValue()));
                    assertThat(expectedUser.principal(), equalTo(authenticatedUser.principal()));
                    assertThat(expectedUser.roles(), equalTo(authenticatedUser.roles()));
                    if (jwtRealm.populateUserMetadata) {
                        assertThat(authenticatedUser.metadata(), is(not(anEmptyMap())));
                    }
                } catch (Throwable t) {
                    realmExceptions.addSuppressed(t);
                    LOGGER.error("Unexpected exception.", realmExceptions);
                    throw t;
                } finally {
                    LOGGER.info("STATS: expected=[" + jwtRealm.name() + "]\n" + String.join("\n", realmUsageStats));
                    if (authenticatedUser != null) {
                        LOGGER.info("RESULT: expected=[" + jwtRealm.name() + "]\n" + String.join("\n", realmResults));
                    }
                }
            }
            LOGGER.info("Test succeeded");
        } finally {
            for (final Realm realm : allRealms) {
                if (realm instanceof JwtRealm jwtRealm) {
                    jwtRealm.close();
                }
            }
        }
    }

    private JwtRealm addJwtRealm(
        final List<Realm> allRealms,
        final String issuer,
        final List<String> audiences,
        final List<String> signatureAlgorithmList,
        final Tuple<Integer, Integer> authzRealmsRange,
        final Tuple<Integer, Integer> rolesRange
    ) throws Exception {
        assertThat(authzRealmsRange.v1(), is(greaterThanOrEqualTo(0)));
        assertThat(rolesRange.v1(), is(greaterThanOrEqualTo(0)));

        // Generate test JWKs and test users (with roles)
        final String authcRealmName = "jwt" + (allRealms.size() + 1) + randomIntBetween(0, 9);
        final List<JWK> testJwks = JwtTestCase.toRandomJwks(JwtUtil.toJwsAlgorithms(signatureAlgorithmList));
        final Map<String, User> testUsers = JwtTestCase.generateTestUsersWithRoles(
            randomIntBetween(5, 10),
            randomIntBetween(rolesRange.v1(), rolesRange.v2())
        );

        // If no authz realms, resolve roles via UserRoleMapper. If some authz realms, resolve via authz User lookup.
        final String[] authzRealmNames = IntStream.range(0, randomIntBetween(authzRealmsRange.v1(), authzRealmsRange.v2()))
            .mapToObj(i -> authcRealmName + "_authz" + i)
            .toArray(String[]::new);

        // These values are used for conditional settings below
        final String clientAuthorizationType = randomFrom(JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPES);
        final String claimPrincipal = randomBoolean() ? "sub" : authcRealmName + "_sub";
        final String claimGroups = randomBoolean() ? null : authcRealmName + "_groups";

        // Split the test JWKs into HMAC JWKSet vs PKC JWKSet. Save a copy of the JWKSet PKC to a file (public keys only).
        final List<JWK> jwksHmac = testJwks.stream().filter(e -> (e instanceof OctetSequenceKey)).toList();
        final List<JWK> jwksPkc = testJwks.stream().filter(e -> (e instanceof OctetSequenceKey) == false).toList();
        final String jwkSetSerializedHmac = JwtUtil.serializeJwkSet(new JWKSet(jwksHmac), false);
        final String jwkSetSerializedPkc = JwtUtil.serializeJwkSet(new JWKSet(jwksPkc), true);
        final Path jwkSetPathPkc = jwksPkc.isEmpty() ? null : Files.createTempFile(PathUtils.get(this.pathHome), "jwkset.", ".json");
        if (jwkSetPathPkc != null) {
            Files.writeString(jwkSetPathPkc, jwkSetSerializedPkc);
        }

        // JWT authc realm basic settings
        final Settings.Builder authcSettings = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_ISSUER), issuer)
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", signatureAlgorithmList)
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWKSET_PKC_PATH),
                (jwkSetPathPkc == null ? null : jwkSetPathPkc.toString())
            )
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_AUDIENCES), randomFrom(audiences))
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE), clientAuthorizationType)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), claimPrincipal)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^(.*)$")
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.POPULATE_USER_METADATA), randomBoolean())
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                String.join(",", authzRealmNames)
            );
        if (Strings.hasText(claimGroups)) {
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getClaim()), claimGroups);
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$");
        }
        // JWT authc realm secure settings
        final MockSecureSettings secureSettings = new MockSecureSettings();
        if (jwksHmac.isEmpty() == false) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWKSET_HMAC_CONTENTS),
                jwkSetSerializedHmac
            );
        }
        // secureSettings.setString(
        // RealmSettings.getFullSettingKey(authcRealmName, SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm(JwtRealmSettings.TYPE)),
        // randomAlphaOfLengthBetween(10, 10)
        // );
        if (clientAuthorizationType.equals(JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET)) {
            final String clientAuthorizationSharedSecret = Base64.getUrlEncoder().encodeToString(randomByteArrayOfLength(32));
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET),
                clientAuthorizationSharedSecret
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
        final UserRoleMapper userRoleMapper = super.buildRoleMapper((authzRealmNames.length >= 1) ? Map.of() : testUsers);

        // If authz names is not set, register the users here in the JWT authc realm.
        final JwtRealm jwtRealm = new JwtRealm(authcConfig, this.threadPool, sslService, userRoleMapper, this.resourceWatcherService);
        jwtRealm.testIssuerJwks = testJwks; // test JWKs for generating JWTs (includes private keys, unlike jwkSetPath w/ public keys only)
        jwtRealm.testIssuerUsers = testUsers; // test Users for generating JWTs (includes roles, authc role mapping or authz roles lookup)
        allRealms.add(jwtRealm);

        // If authz names is set, register the users here in one of the authz realms.
        if (authzRealmNames.length >= 1) {
            final int selected = randomIntBetween(0, authzRealmNames.length - 1);
            for (int i = 0; i < authzRealmNames.length; i++) {
                final RealmConfig authzConfig = this.buildRealmConfig("authz", authzRealmNames[i], Settings.EMPTY, allRealms.size() + 1);
                final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
                if (i == selected) {
                    testUsers.values().forEach(authzRealm::registerUser);
                }
                allRealms.add(authzRealm);
            }
        }
        return jwtRealm;
    }
}
