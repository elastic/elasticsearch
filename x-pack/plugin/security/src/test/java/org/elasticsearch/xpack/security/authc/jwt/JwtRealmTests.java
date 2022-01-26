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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
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
        final Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
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
        final int repeats = 1;
        for (int repeat = 0; repeat < repeats; repeat++) {
            final Tuple<Integer, Integer> authcRealmsRange = new Tuple<>(1, 1);
            final Tuple<Integer, Integer> authzRealmsRange = new Tuple<>(0, 0);
            final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 0);
            final Tuple<Integer, Integer> authcRunsRange = new Tuple<>(2, 3);
            this.realmTestHelper(authcRealmsRange, authzRealmsRange, audiencesRange, rolesRange, authcRunsRange);
        }
    }

    public void testJwtAuthcRealmAuthenticateWithoutAuthzRealms() throws Exception {
        final int repeats = 1;
        for (int repeat = 0; repeat < repeats; repeat++) {
            final Tuple<Integer, Integer> authcRealmsRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> authzRealmsRange = new Tuple<>(0, 0);
            final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 3);
            final Tuple<Integer, Integer> authcRunsRange = new Tuple<>(2, 3);
            this.realmTestHelper(authcRealmsRange, authzRealmsRange, audiencesRange, rolesRange, authcRunsRange);
        }
    }

    public void testJwtAuthcRealmAuthenticateWithAuthzRealms() throws Exception {
        final int repeats = 1;
        for (int repeat = 0; repeat < repeats; repeat++) {
            final Tuple<Integer, Integer> authcRealmsRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> authzRealmsRange = new Tuple<>(0, 3);
            final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
            final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 3);
            final Tuple<Integer, Integer> authcRunsRange = new Tuple<>(2, 3);
            this.realmTestHelper(authcRealmsRange, authzRealmsRange, audiencesRange, rolesRange, authcRunsRange);
        }
    }

    public void realmTestHelper(
        final Tuple<Integer, Integer> authcRealmsRange,
        final Tuple<Integer, Integer> authzRealmsRange,
        final Tuple<Integer, Integer> audiencesRange,
        final Tuple<Integer, Integer> rolesRange,
        final Tuple<Integer, Integer> authcRunsRange
    ) throws Exception {
        final int authcCount = randomIntBetween(authcRealmsRange.v1(), authcRealmsRange.v2());
        final int selectedAuthcIndex = randomIntBetween(0, authcCount - 1);
        final int authcRuns = randomIntBetween(authcRunsRange.v1(), authcRunsRange.v2());
        LOGGER.info("authcCount=[" + authcCount + "], selectedAuthcIndex=[" + selectedAuthcIndex + "], authcRuns=[" + authcRuns + "]");

        // Details from a random selected JWT authc realm. These unique values will be used to create a JWT authentication token.
        final SetOnce<Realm> selectedJwtRealm = new SetOnce<>();
        final SetOnce<String> selectedClientAuthorizationSharedSecret = new SetOnce<>();
        final SetOnce<List<String>> selectedAllowedSignatureAlgorithms = new SetOnce<>();
        final SetOnce<List<JWK>> selectedJwks = new SetOnce<>();
        final SetOnce<String> selectedIssuer = new SetOnce<>();
        final SetOnce<List<String>> selectedAudiences = new SetOnce<>();
        final SetOnce<String> selectedPrincipalClaim = new SetOnce<>();
        final SetOnce<String> selectedPrincipal = new SetOnce<>();
        final SetOnce<String> selectedGroupsClaim = new SetOnce<>();
        final SetOnce<String[]> selectedRoleNames = new SetOnce<>();
        final SetOnce<String> selectedDnClaim = new SetOnce<>();
        final SetOnce<String> selectedDn = new SetOnce<>();
        final SetOnce<String> selectedFullNameClaim = new SetOnce<>();
        final SetOnce<String> selectedFullName = new SetOnce<>();
        final SetOnce<String> selectedEmailClaim = new SetOnce<>();
        final SetOnce<String> selectedEmail = new SetOnce<>();
        final SetOnce<Boolean> selectedPopulateUserMetadata = new SetOnce<>();

        final List<Tuple<Realm, Object>> jwtRealmsAndIssuers = new ArrayList<>();
        final List<Realm> allRealms = new ArrayList<>(); // JWT authc realms and authz realms

        // Create 0..N-1 JWT authc realms. Pick a random one to use for generating a JWT and optional client secret.
        for (final int authcIndex : IntStream.range(0, authcCount).toArray()) {
            // If authzCount == 0, authc realm will resolve the roles via UserRoleMapper
            // If authzCount >= 1, authz realm will resolve the roles via User lookup
            final int authzCount = randomIntBetween(authzRealmsRange.v1(), authzRealmsRange.v2());
            final int audienceCount = randomIntBetween(audiencesRange.v1(), audiencesRange.v2());
            final int rolesCount = randomIntBetween(rolesRange.v1(), rolesRange.v2());
            final int jwksCount = randomIntBetween(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS.size() - 1);

            // Generate random, unique settings per JWT authc realm. This helps with tracing and debugging.
            final String authcName = "jwt" + authcIndex + randomIntBetween(0, 9);
            final String[] authzNames = IntStream.range(0, authzCount).mapToObj(i -> authcName + "_authz" + i).toArray(String[]::new);
            LOGGER.debug("Creating JWT authc realm: authcName=[" + authcName + "], authzNames=[" + Arrays.toString(authzNames) + "]");
            final String clientAuthorizationType = randomFrom(JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPES);
            final String clientAuthorizationSharedSecret = clientAuthorizationType.equals(
                JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
            ) ? Base64.getUrlEncoder().encodeToString(randomByteArrayOfLength(32)) : null;
            final String issuer = authcName + "_iss";
            final List<String> allowedAudiences = IntStream.range(0, audienceCount)
                .mapToObj(i -> authcName + "_aud" + i)
                .collect(Collectors.toList());
            final String claimPrincipal = randomBoolean() ? "sub" : authcName + "_claimPrincipal";
            final String principal = authcName + "_principal";
            final String claimGroups = randomBoolean() ? null : authcName + "_claimGroups";
            final String[] roleNames = IntStream.range(0, rolesCount).mapToObj(i -> authcName + "_role" + i).toArray(String[]::new);
            final String claimDn = randomBoolean() ? null : authcName + "_claimDn";
            final String dn = authcName + "_dn";
            final String fullName = authcName + "_fullName";
            final String claimFullName = randomBoolean() ? null : authcName + "_claimFullName";
            final String claimEmail = randomBoolean() ? null : authcName + "_claimEmail";
            final String email = authcName + "_email@example.com";
            final Boolean populateUserMetadata = randomBoolean();

            // pick random signature algorithms (random size 1..N, and random order), generate random JWKs, and split them into HMAC vs PKC
            final List<String> allowedSignatureAlgorithms = randomSubsetOf(jwksCount, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
            final List<JWK> jwks = JwtTestCase.toRandomJwks(JwtUtil.toJwsAlgorithms(allowedSignatureAlgorithms));
            final List<JWK> jwksHmac = jwks.stream().filter(e -> (e instanceof OctetSequenceKey)).toList();
            final List<JWK> jwksPkc = jwks.stream().filter(e -> (e instanceof OctetSequenceKey) == false).toList();
            final String jwkSetSerializedHmac = JwtUtil.serializeJwkSet(new JWKSet(jwksHmac), false);
            final String jwkSetSerializedPkc = JwtUtil.serializeJwkSet(new JWKSet(jwksPkc), true);

            // JWT authc realm basic settings
            final Path jwkSetPathPkc = jwksPkc.isEmpty() ? null : Files.createTempFile(PathUtils.get(this.pathHome), "jwkset.", ".json");
            if (jwkSetPathPkc != null) {
                Files.writeString(jwkSetPathPkc, jwkSetSerializedPkc);
            }
            final Settings.Builder authcSettings = Settings.builder()
                .put(this.globalSettings)
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_ISSUER), issuer)
                .put(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                    String.join(",", allowedSignatureAlgorithms)
                )
                .put(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                    randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
                )
                .put(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.JWKSET_PKC_PATH),
                    (jwkSetPathPkc == null ? null : jwkSetPathPkc.toString())
                )
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_AUDIENCES), randomFrom(allowedAudiences))
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()), claimPrincipal)
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^(.*)$")
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.POPULATE_USER_METADATA), populateUserMetadata)
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE), clientAuthorizationType)
                .put(
                    RealmSettings.getFullSettingKey(authcName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                    String.join(",", authzNames)
                );
            if (Strings.hasText(claimGroups)) {
                authcSettings.put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLAIMS_GROUPS.getClaim()), claimGroups);
                authcSettings.put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$");
            }
            // JWT authc realm secure settings
            final MockSecureSettings secureSettings = new MockSecureSettings();
            if (jwksHmac.isEmpty() == false) {
                secureSettings.setString(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.JWKSET_HMAC_CONTENTS),
                    jwkSetSerializedHmac
                );
            }
            // secureSettings.setString(
            // RealmSettings.getFullSettingKey(authcName, SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm(JwtRealmSettings.TYPE)),
            // randomAlphaOfLengthBetween(10, 10)
            // );
            if (clientAuthorizationSharedSecret != null) {
                secureSettings.setString(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET),
                    clientAuthorizationSharedSecret
                );
            }
            authcSettings.setSecureSettings(secureSettings);
            // Create JWT authc realm and add to list of all authc and authz realms
            final RealmConfig authcConfig = super.buildRealmConfig(
                JwtRealmSettings.TYPE,
                authcName,
                authcSettings.build(),
                jwtRealmsAndIssuers.size()
            );
            final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(authcSettings.build()));

            final UserRoleMapper authcUserRoleMapper = super.buildRoleMapper(principal, Set.of(roleNames));
            final JwtRealm authcRealm = new JwtRealm(
                authcConfig,
                this.threadPool,
                sslService,
                authcUserRoleMapper,
                this.resourceWatcherService
            );
            allRealms.add(authcRealm);
            jwtRealmsAndIssuers.add(new Tuple<>(authcRealm, null));

            // Add 0..N other authz realms (if any).
            if (authzCount >= 1) {
                final int selectedAuthzIndex = randomIntBetween(0, authzCount - 1);
                for (final int authzIndex : IntStream.range(0, authzCount).toArray()) {
                    final RealmConfig authzConfig = this.buildRealmConfig(
                        "mock",
                        authzNames[authzIndex],
                        Settings.EMPTY,
                        jwtRealmsAndIssuers.size()
                    );
                    final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
                    // For the current JWT authc realm, select only one authz realm to resolve the roles for the authc principal
                    if ((authcIndex == selectedAuthcIndex) && (authzIndex == selectedAuthzIndex)) {
                        final Map<String, Object> userMetadata = Collections.singletonMap("is_lookup", true);
                        authzRealm.registerUser(new User(principal, roleNames, fullName, email, userMetadata, true));
                    }
                    allRealms.add(authzRealm);
                }
            }

            final boolean isSelected = (authcIndex == selectedAuthcIndex);
            LOGGER.info(
                "Created JWT authc realm:"
                    + " isSelected=["
                    + isSelected
                    + "], authcName=["
                    + authcName
                    + "], authzNames=["
                    + Arrays.toString(authzNames)
                    + "], issuer=["
                    + issuer
                    + "], allowedAudiences=["
                    + allowedAudiences
                    + "], claimPrincipal=["
                    + claimPrincipal
                    + "], claimGroups=["
                    + claimGroups
                    + "], clientAuthorizationType=["
                    + clientAuthorizationType
                    + "], clientAuthorizationSharedSecret=["
                    + clientAuthorizationSharedSecret
                    + "], populateUserMetadata=["
                    + populateUserMetadata
                    + "], roleNames=["
                    + Arrays.toString(roleNames)
                    + "], allowedSignatureAlgorithms=["
                    + allowedSignatureAlgorithms
                    + "], jwkSetPathPkc=["
                    + jwkSetPathPkc
                    + "]"
            );

            // select one of the generated JWT authc realm details to use for generating and testing a JWT authentication token
            if (isSelected) {
                selectedJwtRealm.set(authcRealm);
                selectedIssuer.set(issuer);
                selectedAudiences.set(allowedAudiences);
                selectedPrincipalClaim.set(claimPrincipal);
                selectedPrincipal.set(principal);
                selectedGroupsClaim.set(claimGroups);
                selectedRoleNames.set(roleNames);
                selectedDnClaim.set(claimDn);
                selectedDn.set(dn);
                selectedFullNameClaim.set(claimFullName);
                selectedFullName.set(fullName);
                selectedEmailClaim.set(claimEmail);
                selectedEmail.set(email);
                selectedPopulateUserMetadata.set(populateUserMetadata);
                selectedClientAuthorizationSharedSecret.set(clientAuthorizationSharedSecret);
                selectedAllowedSignatureAlgorithms.set(allowedSignatureAlgorithms);
                selectedJwks.set(jwks);
            }
        }
        assertThat(selectedJwtRealm.get(), is(notNullValue())); // debug check: ensure a JWT authc realm was selected

        // initialize all authc and authz realms, only the JWT authc realms actually need to do anything
        LOGGER.info("Initializing realms");
        for (final Realm realm : allRealms) {
            if (realm instanceof JwtRealm jwtRealm) {
                final Exception exception = expectThrows(IllegalStateException.class, () -> jwtRealm.ensureInitialized());
                assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));
            }
            realm.initialize(allRealms, this.licenseState); // JWT authc realms link to authz realms
        }

        // From selected realm, select a random JWK/JWK signature algorithm for signing a JWT. Confirm the JWT realm can authc it.
        for (int authcRun = 1; authcRun <= authcRuns; authcRun++) {
            final int index = randomIntBetween(0, selectedJwks.get().size() - 1);
            final JWK jwk = selectedJwks.get().get(index);
            final String allowedSignatureAlgorithm = selectedAllowedSignatureAlgorithms.get().get(index);
            LOGGER.info(
                "authcRun ["
                    + authcRun
                    + "], index ["
                    + index
                    + "], JWK ["
                    + jwk.getKeyType()
                    + "], JWS algorithm ["
                    + allowedSignatureAlgorithm
                    + "]."
            );
            final JWSSigner jwsSigner = JwtUtil.createJwsSigner(jwk);
            final Tuple<JWSHeader, JWTClaimsSet> jwsHeaderAndJwtClaimsSet = JwtUtilTests.randomValidJwsHeaderAndJwtClaimsSet(
                allowedSignatureAlgorithm,
                selectedIssuer.get(),
                selectedAudiences.get(),
                selectedPrincipalClaim.get(),
                selectedPrincipal.get(),
                selectedGroupsClaim.get(),
                List.of(selectedRoleNames.get()),
                selectedDnClaim.get(),
                selectedDn.get(),
                selectedFullNameClaim.get(),
                selectedFullName.get(),
                selectedEmailClaim.get(),
                selectedEmail.get()
            );
            final SignedJWT signedJWT = JwtUtil.signSignedJwt(jwsSigner, jwsHeaderAndJwtClaimsSet.v1(), jwsHeaderAndJwtClaimsSet.v2());
            final JWSVerifier jwkVerifier = JwtUtil.createJwsVerifier(jwk);
            assertThat(JwtUtil.verifySignedJWT(jwkVerifier, signedJWT), is(equalTo(true)));

            // Use a new ThreadContext per authcRunsRange loop body. Add the signed JWT and optional client secret.
            LOGGER.info("Using JWT [" + signedJWT.serialize() + "], client secret [" + selectedClientAuthorizationSharedSecret.get() + "]");
            final ThreadContext requestThreadContext = new ThreadContext(this.globalSettings);
            requestThreadContext.putHeader(
                JwtRealmSettings.HEADER_END_USER_AUTHORIZATION,
                JwtRealmSettings.HEADER_END_USER_AUTHORIZATION_SCHEME + " " + signedJWT.serialize()
            );
            if (selectedClientAuthorizationSharedSecret.get() != null) {
                requestThreadContext.putHeader(
                    JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION,
                    JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET + " " + selectedClientAuthorizationSharedSecret.get()
                );
            }

            // Loop through all authc/authz realms. Confirm a JWT authc realm recognizes and extracts the request headers.
            AuthenticationToken authenticationToken = null;
            for (final Realm realm : allRealms) {
                authenticationToken = realm.token(requestThreadContext);
                if (authenticationToken != null) {
                    break;
                }
            }
            assertThat(authenticationToken, is(notNullValue()));
            assertThat(authenticationToken, isA(JwtAuthenticationToken.class));
            LOGGER.info("Realm chain returned a JWT authentication token [" + authenticationToken + "]");

            // Loop through all authc/authz realms. Confirm authenticatedUser is returned with expected principal and roles.
            User authenticatedUser = null;
            final List<String> realmResults = new ArrayList<>();
            final Exception realmExceptions = new Exception("Authentication failed.");
            final List<String> realmUsageStats = new ArrayList<>();
            try {
                for (final Realm realm : allRealms) {
                    final PlainActionFuture<AuthenticationResult<User>> authenticateFuture = PlainActionFuture.newFuture();
                    try {
                        realm.authenticate(authenticationToken, authenticateFuture);
                        final AuthenticationResult<User> authenticationResult = authenticateFuture.actionGet();
                        authenticationResult.getMessage();
                        final Exception authenticationResultException = authenticationResult.getException();
                        if (authenticationResult != null) {
                            final String realmResult = "AuthenticationResult status=["
                                + authenticationResult.getStatus()
                                + "], authenticated=["
                                + authenticationResult.isAuthenticated()
                                + "], message=["
                                + authenticationResult.getMessage()
                                + "], metadata=["
                                + authenticationResult.getMetadata()
                                + "], user=["
                                + authenticationResult.getValue()
                                + "]";
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
                            if (authenticatedUser != null) {
                                break; // Break out of the loop
                            }
                        } else {
                            fail("Exception and AuthenticationResult are null. Expected one of them to be non-null.");
                        }
                    } catch (Exception e) {
                        realmExceptions.addSuppressed(new Exception("Caught Exception.", e));
                    } finally {
                        final PlainActionFuture<Map<String, Object>> usageStatsFuture = PlainActionFuture.newFuture();
                        realm.usageStats(usageStatsFuture);
                        realmUsageStats.add("Usage stats=[" + usageStatsFuture.actionGet() + "]");
                    }
                }
                // Loop ended. Confirm authenticatedUser is returned with expected principal and roles.
                assertThat("AuthenticatedUser is null. Expected a realm to authenticate.", authenticatedUser, is(notNullValue()));
                assertThat(authenticatedUser.principal(), equalTo(selectedPrincipal.get()));
                assertThat(
                    new TreeSet<>(Arrays.asList(authenticatedUser.roles())),
                    equalTo(new TreeSet<>(Arrays.asList(selectedRoleNames.get())))
                );
                if (selectedPopulateUserMetadata.get()) {
                    assertThat(authenticatedUser.metadata(), is(not(anEmptyMap())));
                }
            } catch (Throwable t) {
                realmExceptions.addSuppressed(t);
                LOGGER.error("Expected exception.", realmExceptions);
                throw t;
            } finally {
                if (authenticatedUser != null) {
                    LOGGER.info("RESULTS: authcRun=[" + authcRun + "]\n" + String.join("\n", realmResults));
                }
                LOGGER.info("STATS: authcRun=[" + authcRun + "]\n" + String.join("\n", realmUsageStats));
            }
        }
        LOGGER.info("Test succeeded");
    }
}
