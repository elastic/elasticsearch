/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
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

import java.io.IOException;
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
        final Tuple<Integer, Integer> authcRange = new Tuple<>(1, 1);
        final Tuple<Integer, Integer> authzRange = new Tuple<>(0, 1);
        final Tuple<Integer, Integer> algRange = new Tuple<>(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size());
        final Tuple<Integer, Integer> audRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> userRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> roleRange = new Tuple<>(0, 0);
        final Tuple<Integer, Integer> jwtRange = new Tuple<>(2, 3);
        final Tuple<Integer, Integer> authcCacheRange = new Tuple<>(0, 1);
        final Tuple<Integer, Integer> authzCacheRange = new Tuple<>(0, 1);
        this.realmTestHelper(authcRange, authzRange, algRange, audRange, userRange, roleRange, jwtRange, authcCacheRange, authzCacheRange);
    }

    public void testJwtAuthcRealmAuthenticateWithoutAuthzRealms() throws Exception {
        final Tuple<Integer, Integer> authcRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> authzRange = new Tuple<>(0, 0);
        final Tuple<Integer, Integer> algRange = new Tuple<>(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size());
        final Tuple<Integer, Integer> audRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> userRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> roleRange = new Tuple<>(0, 3);
        final Tuple<Integer, Integer> jwtRange = new Tuple<>(2, 3);
        final Tuple<Integer, Integer> authcCacheRange = new Tuple<>(0, 1);
        final Tuple<Integer, Integer> authzCacheRange = new Tuple<>(0, 1);
        this.realmTestHelper(authcRange, authzRange, algRange, audRange, userRange, roleRange, jwtRange, authcCacheRange, authzCacheRange);
    }

    public void testJwtAuthcRealmAuthenticateWithAuthzRealms() throws Exception {
        final Tuple<Integer, Integer> authcRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> authzRange = new Tuple<>(0, 3);
        final Tuple<Integer, Integer> algRange = new Tuple<>(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size());
        final Tuple<Integer, Integer> audRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> userRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> roleRange = new Tuple<>(0, 3);
        final Tuple<Integer, Integer> jwtRange = new Tuple<>(2, 3);
        final Tuple<Integer, Integer> authcCacheRange = new Tuple<>(0, 1);
        final Tuple<Integer, Integer> authzCacheRange = new Tuple<>(0, 1);
        this.realmTestHelper(authcRange, authzRange, algRange, audRange, userRange, roleRange, jwtRange, authcCacheRange, authzCacheRange);
    }

    public void realmTestHelper(
        final Tuple<Integer, Integer> authcRange,
        final Tuple<Integer, Integer> authzRange,
        final Tuple<Integer, Integer> algRange,
        final Tuple<Integer, Integer> audRange,
        final Tuple<Integer, Integer> userRange,
        final Tuple<Integer, Integer> roleRange,
        final Tuple<Integer, Integer> jwtRange,
        final Tuple<Integer, Integer> authcCacheRange,
        final Tuple<Integer, Integer> authzCacheRange
    ) throws Exception {
        assertThat(authcRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(authzRange.v1(), is(greaterThanOrEqualTo(0)));
        assertThat(algRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(audRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(userRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(roleRange.v1(), is(greaterThanOrEqualTo(0)));
        assertThat(jwtRange.v1(), is(greaterThanOrEqualTo(1)));
        assertThat(authcCacheRange.v1(), is(greaterThanOrEqualTo(0)));
        assertThat(authzCacheRange.v1(), is(greaterThanOrEqualTo(0)));

        final int realmCount = randomIntBetween(authcRange.v1(), authcRange.v2());
        final List<Realm> allRealms = new ArrayList<>();
        final List<Tuple<JwtIssuer, JwtRealm>> jwtIssuerAndRealms = new ArrayList<>();
        try {
            // Create JWT authc realms and mocked authz realms. Initialize each JWT realm, and test ensureInitialized() before and after.
            for (int i = 0; i < realmCount; i++) {
                final int authzCount = randomIntBetween(authzRange.v1(), authzRange.v2());
                final int algCount = randomIntBetween(algRange.v1(), algRange.v2());
                final int audCount = randomIntBetween(audRange.v1(), audRange.v2());
                final int userCount = randomIntBetween(userRange.v1(), userRange.v2());
                final int roleCount = randomIntBetween(roleRange.v1(), roleRange.v2());
                final int authcCacheSize = randomIntBetween(authcCacheRange.v1(), authcCacheRange.v2());
                final int authzCacheSize = randomIntBetween(authzCacheRange.v1(), authzCacheRange.v2());

                final JwtIssuer jwtIssuer = createJwtIssuer(i, algCount, audCount, userCount, roleCount);
                final JwtRealm jwtRealm = createJwtRealm(allRealms, authzCount, jwtIssuer, authcCacheSize, authzCacheSize);
                jwtIssuerAndRealms.add(new Tuple<>(jwtIssuer, jwtRealm));

                // verify exception before initialization
                final Exception exception = expectThrows(IllegalStateException.class, jwtRealm::ensureInitialized);
                assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));
            }
            allRealms.forEach(r -> r.initialize(allRealms, this.licenseState));
            jwtIssuerAndRealms.forEach(e -> e.v2().ensureInitialized());

            // Select one of the JWT authc Realms.
            final Tuple<JwtIssuer, JwtRealm> jwtIssuerAndRealm = randomFrom(jwtIssuerAndRealms);
            final JwtIssuer jwtIssuer = jwtIssuerAndRealm.v1();
            final JwtRealm jwtRealm = jwtIssuerAndRealm.v2();
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
                    + ", HMAC alg="
                    + jwtRealm.algorithmsHmac
                    + ", PKC alg="
                    + jwtRealm.algorithmsPkc
                    + ", client=["
                    + jwtRealm.clientAuthenticationType
                    + "], meta=["
                    + jwtRealm.populateUserMetadata
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

            // Select one of the test users from the JWT realm, to use inside the authc test loop.
            final User expectedUser = randomFrom(jwtIssuer.getUsers().values());
            LOGGER.info("USER[" + expectedUser.principal() + "]: roles=[" + String.join(",", expectedUser.roles()) + "].");

            // Select different test JWKs from the JWT realm, and generate test JWTs for the test user. Run the JWT through the chain.
            final int authcRepeats = randomIntBetween(jwtRange.v1(), jwtRange.v2());
            for (int authcRun = 1; authcRun <= authcRepeats; authcRun++) {
                final String signatureAlgorithm = randomFrom(jwtIssuer.getSignatureAlgorithmAndJwks().keySet());
                final JWK jwk = randomFrom(jwtIssuer.getSignatureAlgorithmAndJwks().get(signatureAlgorithm));
                final String jwkDesc = jwk.getKeyType() + "/" + jwk.size();
                LOGGER.info("RUN[" + authcRun + "/" + authcRepeats + "]: jwk=[" + jwkDesc + "], alg=[" + signatureAlgorithm + "].");

                // Generate different JWTs for the selected user. Use a different JWK/alg and iat each time.
                final JWSSigner jwsSigner = JwtValidateUtil.createJwsSigner(jwk);
                final Tuple<JWSHeader, JWTClaimsSet> jwsHeaderAndJwtClaimsSet = JwkValidateUtilTests.randomValidJwsHeaderAndJwtClaimsSet(
                    signatureAlgorithm,
                    jwtRealm.allowedIssuer,
                    jwtRealm.allowedAudiences,
                    jwtRealm.claimParserPrincipal.getClaimName(),
                    expectedUser.principal(),
                    jwtRealm.claimParserGroups.getClaimName(),
                    List.of(expectedUser.roles()),
                    Map.of("metadata", randomAlphaOfLength(10))
                );
                final SignedJWT signedJWT = JwtValidateUtil.signJwt(
                    jwsSigner,
                    jwsHeaderAndJwtClaimsSet.v1(),
                    jwsHeaderAndJwtClaimsSet.v2()
                );
                final JWSVerifier jwkVerifier = JwtValidateUtil.createJwsVerifier(jwk);
                assertThat(JwtValidateUtil.verifyJWT(jwkVerifier, signedJWT), is(equalTo(true)));

                // Create request with headers set
                final SecureString headerJwt = new SecureString(signedJWT.serialize().toCharArray());
                final SecureString headerSecret = jwtRealm.clientAuthenticationSharedSecret;
                LOGGER.info("HEADERS: jwt=[" + headerJwt + "], secret=[" + headerSecret + "].");
                final ThreadContext requestThreadContext = new ThreadContext(this.globalSettings);
                requestThreadContext.putHeader(
                    JwtRealm.HEADER_END_USER_AUTHENTICATION,
                    JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + headerJwt
                );
                if (headerSecret != null) {
                    requestThreadContext.putHeader(
                        JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                        JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET + " " + headerSecret
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
                final SecureString tokenSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();
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
                final List<String> realmAuthenticationResults = new ArrayList<>();
                final List<String> realmUsageStats = new ArrayList<>();
                final List<Exception> realmFailureExceptions = new ArrayList<>(allRealms.size());
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
                            realmAuthenticationResults.add(realmResult);
                            realmFailureExceptions.add(new Exception(realmResult, authenticationResultException));
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
                            realmFailureExceptions.add(new Exception("Caught Exception.", e));
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
                    final Exception authcFailed = new Exception("Authentication failed.");
                    realmFailureExceptions.forEach(authcFailed::addSuppressed); // realm exceptions
                    authcFailed.addSuppressed(t); // final throwable (ex: assertThat)
                    LOGGER.error("Unexpected exception.", authcFailed);
                    throw authcFailed;
                } finally {
                    LOGGER.info("STATS: expected=[" + jwtRealm.name() + "]\n" + String.join("\n", realmUsageStats));
                    if (authenticatedUser != null) {
                        LOGGER.info("RESULT: expected=[" + jwtRealm.name() + "]\n" + String.join("\n", realmAuthenticationResults));
                    }
                }
            }
            LOGGER.info("Test succeeded");
        } finally {
            for (final Tuple<JwtIssuer, JwtRealm> jwtIssuerAndRealm : jwtIssuerAndRealms) {
                LOGGER.info("Closing realm [" + jwtIssuerAndRealm.v2().name() + "].");
                jwtIssuerAndRealm.v2().close();
            }
        }
    }

    private JwtIssuer createJwtIssuer(final int i, final int algCount, final int audCount, final int userCount, final int roleCount)
        throws JOSEException {
        final String issuer = "iss" + (i + 1) + randomIntBetween(0, 9999);
        final List<String> signatureAlgorithms = randomOf(algCount, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final Map<String, List<JWK>> algAndJwks = JwtTestCase.randomJwks(signatureAlgorithms);
        final List<String> audiences = IntStream.range(0, audCount).mapToObj(j -> issuer + "_aud" + (j + 1)).toList();
        final Map<String, User> users = JwtTestCase.generateTestUsersWithRoles(userCount, roleCount);
        return new JwtIssuer(issuer, audiences, algAndJwks, users);
    }

    private JwtRealm createJwtRealm(
        final List<Realm> allRealms,
        final int authzCount,
        final JwtIssuer jwtIssuer,
        final int authcCacheSize,
        final int authzCacheSize
    ) throws Exception {
        final String authcRealmName = "jwt" + (allRealms.size() + 1) + randomIntBetween(0, 9);
        final String[] authzRealmNames = IntStream.range(0, authzCount).mapToObj(z -> authcRealmName + "_authz" + z).toArray(String[]::new);

        final String clientAuthenticationType = randomFrom(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPES);
        final Settings.Builder authcSettings = Settings.builder()
            .put(this.globalSettings)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.getIssuer())
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.getSignatureAlgorithmAndJwks().keySet())
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWKSET_PKC_PATH),
                saveJwkSetToTempFile(jwtIssuer.getJwkSetPkc(), true)
            )
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_AUDIENCES), randomFrom(jwtIssuer.getAudiences()))
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
            )
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWT_VALIDATION_CACHE_MAX_USERS), authcCacheSize)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ROLES_LOOKUP_CACHE_MAX_USERS), authzCacheSize);
        if (randomBoolean()) {
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getClaim()),
                authcRealmName + "_groups"
            );
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$");
        }
        // JWT authc realm secure settings
        final MockSecureSettings secureSettings = new MockSecureSettings();
        if (jwtIssuer.getJwkSetHmac().getKeys().isEmpty() == false) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWKSET_HMAC_CONTENTS),
                JwtUtil.serializeJwkSet(jwtIssuer.getJwkSetHmac(), false)
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
        final UserRoleMapper userRoleMapper = super.buildRoleMapper((authzRealmNames.length >= 1) ? Map.of() : jwtIssuer.getUsers());

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
                    jwtIssuer.getUsers().values().forEach(authzRealm::registerUser);
                }
                allRealms.add(authzRealm);
            }
        }
        return jwtRealm;
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
