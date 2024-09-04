/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.cache.Cache;
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
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.ClientAuthenticationType;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class JwtRealmTestCase extends JwtTestCase {
    record JwtRealmSettingsBuilder(String name, Settings.Builder settingsBuilder) {}

    record JwtIssuerAndRealm(JwtIssuer issuer, JwtRealm realm, JwtRealmSettingsBuilder realmSettingsBuilder) {}

    protected ThreadPool threadPool;
    protected ResourceWatcherService resourceWatcherService;
    protected MockLicenseState licenseState;
    protected List<JwtIssuerAndRealm> jwtIssuerAndRealms;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("JWT realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws Exception {
        if (jwtIssuerAndRealms != null) {
            jwtIssuerAndRealms.forEach(jwtIssuerAndRealm -> {
                jwtIssuerAndRealm.realm.close(); // Close HTTPS client (if any)
                jwtIssuerAndRealm.issuer.close(); // Close HTTPS server (if any)
            });
        }
        resourceWatcherService.close();
        terminate(threadPool);
    }

    protected void verifyAuthenticateFailureHelper(
        final JwtIssuerAndRealm jwtIssuerAndRealm,
        final SecureString jwt,
        final SecureString clientSecret
    ) throws InterruptedException, ExecutionException {
        final ThreadContext tc = createThreadContext(jwt, clientSecret);
        final JwtAuthenticationToken token = (JwtAuthenticationToken) jwtIssuerAndRealm.realm.token(tc);
        final PlainActionFuture<AuthenticationResult<User>> plainActionFuture = new PlainActionFuture<>();
        jwtIssuerAndRealm.realm.authenticate(token, plainActionFuture);
        assertThat(plainActionFuture.get(), notNullValue());
        assertThat(plainActionFuture.get().isAuthenticated(), is(false));
    }

    protected List<JwtIssuerAndRealm> generateJwtIssuerRealmPairs(
        final int realmsCount,
        final int authzCount,
        final int algsCount,
        final int audiencesCount,
        final int usersCount,
        final int rolesCount,
        final int jwtCacheSize,
        final boolean createHttpsServer
    ) throws Exception {
        // Create JWT authc realms and mocked authz realms. Initialize each JWT realm, and test ensureInitialized() before and after.
        final List<Realm> allRealms = new ArrayList<>(); // authc and authz realms
        jwtIssuerAndRealms = new ArrayList<>(realmsCount);
        for (int i = 0; i < realmsCount; i++) {

            final JwtIssuer jwtIssuer = createJwtIssuer(i, algsCount, audiencesCount, usersCount, rolesCount, createHttpsServer);
            // If HTTPS server was created in JWT issuer, any exception after that point requires closing it to avoid a thread pool leak
            try {
                final JwtRealmSettingsBuilder realmSettingsBuilder = createJwtRealmSettingsBuilder(jwtIssuer, authzCount, jwtCacheSize);
                final JwtRealm jwtRealm = createJwtRealm(allRealms, jwtIssuer, realmSettingsBuilder);

                // verify exception before initialize()
                final Exception exception = expectThrows(IllegalStateException.class, jwtRealm::ensureInitialized);
                assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));

                final JwtIssuerAndRealm jwtIssuerAndRealm = new JwtIssuerAndRealm(jwtIssuer, jwtRealm, realmSettingsBuilder);
                jwtIssuerAndRealms.add(jwtIssuerAndRealm);
            } catch (Throwable t) {
                jwtIssuer.close();
                throw t;
            }
        }
        allRealms.forEach(realm -> realm.initialize(allRealms, licenseState)); // JWT realms and authz realms
        jwtIssuerAndRealms.forEach(p -> p.realm.ensureInitialized()); // verify no exception after initialize()
        return jwtIssuerAndRealms;
    }

    protected JwtIssuer createJwtIssuer(
        final int i,
        final int algsCount,
        final int audiencesCount,
        final int userCount,
        final int roleCount,
        final boolean createHttpsServer
    ) throws Exception {
        final String issuer = "iss" + (i + 1) + "_" + randomIntBetween(0, 9999);
        final List<String> audiences = IntStream.range(0, audiencesCount).mapToObj(j -> issuer + "_aud" + (j + 1)).toList();
        final Map<String, User> users = JwtTestCase.generateTestUsersWithRoles(userCount, roleCount);
        // Allow algorithm repeats, to cover testing of multiple JWKs for same algorithm
        final JwtIssuer jwtIssuer = new JwtIssuer(issuer, audiences, users, createHttpsServer);
        final List<String> algorithms = randomOfMinMaxNonUnique(algsCount, algsCount, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final boolean areHmacJwksOidcSafe = randomBoolean();
        final List<JwtIssuer.AlgJwkPair> algAndJwks = JwtRealmTestCase.randomJwks(algorithms, areHmacJwksOidcSafe);
        jwtIssuer.setJwks(algAndJwks, areHmacJwksOidcSafe);
        return jwtIssuer;
    }

    protected void copyIssuerJwksToRealmConfig(final JwtIssuerAndRealm jwtIssuerAndRealm) throws Exception {
        if (JwtRealmInspector.isConfiguredJwkSetPkc(jwtIssuerAndRealm.realm)
            && (JwtRealmInspector.getJwkSetPathUri(jwtIssuerAndRealm.realm) == null)) {
            logger.trace("Updating JwtRealm PKC public JWKSet local file");
            final Path path = PathUtils.get(JwtRealmInspector.getJwkSetPath(jwtIssuerAndRealm.realm));
            Files.writeString(path, jwtIssuerAndRealm.issuer.encodedJwkSetPkcPublic);
        }

        // TODO If x-pack Security plug-in add supports for reloadable settings, update HMAC JWKSet and HMAC OIDC JWK in ES Keystore
    }

    protected JwtRealmSettingsBuilder createJwtRealmSettingsBuilder(final JwtIssuer jwtIssuer, final int authzCount, final int jwtCacheSize)
        throws Exception {
        final String authcRealmName = "realm_" + jwtIssuer.issuerClaimValue;
        final String[] authzRealmNames = IntStream.range(0, authzCount).mapToObj(z -> authcRealmName + "_authz" + z).toArray(String[]::new);

        final ClientAuthenticationType clientAuthenticationType = randomFrom(ClientAuthenticationType.values());
        final Settings.Builder authcSettings = Settings.builder()
            .put(globalSettings)
            .put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_ISSUER), jwtIssuer.issuerClaimValue)
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                String.join(",", jwtIssuer.algorithmsAll)
            )
            .put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_AUDIENCES),
                randomFrom(jwtIssuer.audiencesClaimValue)
            );
        authcSettings.put(
            RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getClaim()),
            jwtIssuer.principalClaimName
        );
        if ((ClientAuthenticationType.SHARED_SECRET != clientAuthenticationType) || (randomBoolean())) {
            // always set "None", optionally set "SharedSecret" or let it get picked by default
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
                clientAuthenticationType
            );
        }
        if (randomBoolean()) {
            // optionally allow default, or set -1 disabled or non-zero for enabled
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
            );
        }
        if (jwtIssuer.encodedJwkSetPkcPublic.isEmpty() == false) {
            final String jwkSetPath; // file or HTTPS URL
            if (jwtIssuer.httpsServer == null) {
                jwkSetPath = saveToTempFile("jwkset.", ".json", jwtIssuer.encodedJwkSetPkcPublic);
            } else {
                authcSettings.putList(
                    RealmSettings.getFullSettingKey(
                        new RealmConfig.RealmIdentifier(JwtRealmSettings.TYPE, authcRealmName),
                        SSLConfigurationSettings.CAPATH_SETTING_REALM
                    ),
                    JwtIssuerHttpsServer.CERT_PATH.toString()
                );
                jwkSetPath = jwtIssuer.httpsServer.url;
            }
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.PKC_JWKSET_PATH), jwkSetPath);
        }
        if (randomBoolean()) {
            // principal claim name is required, but principal claim pattern is optional
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_PRINCIPAL.getPattern()), "^(.*)$");
        }
        if (randomBoolean()) {
            // groups claim name is optional
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getClaim()),
                authcRealmName + "_groups"
            );
            if (randomBoolean()) {
                // if groups claim name is set, groups claim pattern is optional
                authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_GROUPS.getPattern()), "^(.*)$");
            }
        }
        if (randomBoolean()) {
            // dn claim name is optional
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_DN.getClaim()),
                authcRealmName + "_dn"
            );
            if (randomBoolean()) {
                // if dn claim name is set, dn claim pattern is optional
                authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_DN.getPattern()), "^(.*)$");
            }
        }
        if (randomBoolean()) {
            // mail claim name is optional
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_MAIL.getClaim()),
                authcRealmName + "_mail"
            );
            if (randomBoolean()) {
                // if mail claim name is set, dn claim pattern is optional
                authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_MAIL.getPattern()), "^(.*)$");
            }
        }
        if (randomBoolean()) {
            // full name claim name is optional
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_NAME.getClaim()),
                authcRealmName + "_name"
            );
            if (randomBoolean()) {
                // if full name claim name is set, name claim pattern is optional
                authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLAIMS_NAME.getPattern()), "^(.*)$");
            }
        }
        if (randomBoolean()) {
            // allow default to be picked, or explicitly set true or false
            authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.POPULATE_USER_METADATA), randomBoolean());
        }
        if ((authzRealmNames.length != 0) || (randomBoolean())) {
            // always set non-empty list, otherwise leave it out or optionally set value to an empty list
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE)),
                String.join(",", authzRealmNames)
            );
        }

        // JWT cache (on/off controlled by jwtCacheSize)
        if (randomBoolean()) {
            authcSettings.put(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWT_CACHE_TTL),
                randomIntBetween(10, 120) + randomFrom("m", "h")
            );
        }
        authcSettings.put(RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.JWT_CACHE_SIZE), jwtCacheSize);

        // JWT authc realm secure settings
        final MockSecureSettings secureSettings = new MockSecureSettings();
        if (jwtIssuer.algAndJwksHmac.isEmpty() == false) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.HMAC_JWKSET),
                jwtIssuer.encodedJwkSetHmac
            );
        }
        if (jwtIssuer.encodedKeyHmacOidc != null) {
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.HMAC_KEY),
                jwtIssuer.encodedKeyHmacOidc
            );
        }
        if (clientAuthenticationType.equals(ClientAuthenticationType.SHARED_SECRET)) {
            // always set if type is "SharedSecret"
            final String clientAuthenticationSharedSecret = randomAlphaOfLength(64);
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcRealmName, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
                clientAuthenticationSharedSecret
            );
        }
        authcSettings.setSecureSettings(secureSettings);
        return new JwtRealmSettingsBuilder(authcRealmName, authcSettings);
    }

    protected JwtRealm createJwtRealm(
        final List<Realm> allRealms, // JWT realms and authz realms
        final JwtIssuer jwtIssuer,
        final JwtRealmSettingsBuilder realmSettingsBuilder
    ) {
        final String authcRealmName = realmSettingsBuilder.name;
        final Settings settings = realmSettingsBuilder.settingsBuilder.build();
        final RealmConfig authcConfig = buildRealmConfig(JwtRealmSettings.TYPE, authcRealmName, settings, (allRealms.size() + 1));
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        final List<String> authzRealmNames = settings.getAsList(
            RealmSettings.getFullSettingKey(authcRealmName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE))
        );
        final UserRoleMapper userRoleMapper = buildRoleMapper(authzRealmNames.isEmpty() ? jwtIssuer.principals : Map.of());

        // If authz names is not set, register the users here in the JWT authc realm.
        final JwtRealm jwtRealm = new JwtRealm(authcConfig, sslService, userRoleMapper);
        allRealms.add(jwtRealm);

        // If authz names is set, register the users here in one of the authz realms.
        if (authzRealmNames.isEmpty() == false) {
            final String selected = randomFrom(authzRealmNames);
            for (final String authzRealmName : authzRealmNames) {
                final RealmConfig authzConfig = buildRealmConfig("authz", authzRealmName, Settings.EMPTY, allRealms.size() + 1);
                final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
                if (authzRealmName.equals(selected)) {
                    jwtIssuer.principals.values().forEach(authzRealm::registerUser);
                }
                allRealms.add(authzRealm);
            }
        }
        return jwtRealm;
    }

    protected JwtIssuerAndRealm randomJwtIssuerRealmPair() throws ParseException {
        // Select random JWT issuer and JWT realm pair, and log the realm settings
        assertThat(jwtIssuerAndRealms, notNullValue());
        assertThat(jwtIssuerAndRealms, not(empty()));
        final JwtIssuerAndRealm jwtIssuerAndRealm = randomFrom(jwtIssuerAndRealms);
        final JwtRealm jwtRealm = jwtIssuerAndRealm.realm;
        assertThat(jwtRealm, notNullValue());
        assertThat(JwtRealmInspector.getAllowedIssuer(jwtRealm), equalTo(jwtIssuerAndRealm.issuer.issuerClaimValue));
        assertThat(
            jwtIssuerAndRealm.issuer.audiencesClaimValue.stream().anyMatch(JwtRealmInspector.getAllowedAudiences(jwtRealm)::contains),
            is(true)
        );
        printJwtRealmAndIssuer(jwtIssuerAndRealm);
        return jwtIssuerAndRealm;
    }

    protected void doMultipleAuthcAuthzAndVerifySuccess(
        final JwtRealm jwtRealm,
        final User user,
        final SecureString jwt,
        final SecureString sharedSecret,
        final int jwtAuthcRepeats
    ) {
        final List<JwtRealm> jwtRealmsList = jwtIssuerAndRealms.stream().map(p -> p.realm).toList();
        BytesArray firstCacheKeyFound = null;
        // Select different test JWKs from the JWT realm, and generate test JWTs for the test user. Run the JWT through the chain.
        for (int authcRun = 1; authcRun <= jwtAuthcRepeats; authcRun++) {

            final ThreadContext requestThreadContext = createThreadContext(jwt, sharedSecret);
            logger.debug("REQ[" + authcRun + "/" + jwtAuthcRepeats + "] HEADERS=" + requestThreadContext.getHeaders());

            // Any JWT realm can recognize and extract the request headers.
            final var jwtAuthenticationToken = (JwtAuthenticationToken) randomFrom(jwtRealmsList).token(requestThreadContext);
            assertThat(jwtAuthenticationToken, notNullValue());
            assertThat(jwtAuthenticationToken.principal(), notNullValue());
            assertThat(jwtAuthenticationToken.getClientAuthenticationSharedSecret(), equalTo(sharedSecret));

            // Loop through all authc/authz realms. Confirm user is returned with expected principal and roles.
            User authenticatedUser = null;
            realmLoop: for (final JwtRealm candidateJwtRealm : jwtRealmsList) {
                logger.debug("TRY AUTHC: expected=[" + jwtRealm.name() + "], candidate[" + candidateJwtRealm.name() + "].");
                final PlainActionFuture<AuthenticationResult<User>> authenticateFuture = new PlainActionFuture<>();
                candidateJwtRealm.authenticate(jwtAuthenticationToken, authenticateFuture);
                final AuthenticationResult<User> authenticationResult = authenticateFuture.actionGet();
                logger.debug("Authentication result with realm [{}]: [{}]", candidateJwtRealm.name(), authenticationResult);
                switch (authenticationResult.getStatus()) {
                    case SUCCESS:
                        assertThat("Unexpected realm SUCCESS status", candidateJwtRealm.name(), equalTo(jwtRealm.name()));
                        assertThat("Expected realm metadata empty", authenticationResult.getMetadata(), is(anEmptyMap()));
                        authenticatedUser = authenticationResult.getValue();
                        assertThat("Expected realm user null", authenticatedUser, notNullValue());
                        break realmLoop;
                    case CONTINUE:
                        assertThat("Expected realm CONTINUE status", candidateJwtRealm.name(), not(equalTo(jwtRealm.name())));
                        continue;
                    case TERMINATE:
                        fail("A JWT realm should never terminate the authentication process, but [" + candidateJwtRealm.name() + "] did");
                        break;
                    default:
                        fail("Unexpected AuthenticationResult.Status=[" + authenticationResult.getStatus() + "]");
                }
            }
            // Loop ended. Confirm user is returned with expected principal and roles.
            assertThat("Expected realm " + jwtRealm.name() + " to authenticate.", authenticatedUser, notNullValue());
            assertThat(user.principal(), equalTo(authenticatedUser.principal()));
            assertThat(new TreeSet<>(Arrays.asList(user.roles())), equalTo(new TreeSet<>(Arrays.asList(authenticatedUser.roles()))));
            if (jwtRealm.delegatedAuthorizationSupport.hasDelegation()) {
                assertThat(user.metadata(), equalTo(authenticatedUser.metadata())); // delegated authz returns user's metadata
            } else if (JwtRealmInspector.shouldPopulateUserMetadata(jwtRealm)) {
                assertThat(authenticatedUser.metadata(), hasEntry("jwt_token_type", JwtRealmInspector.getTokenType(jwtRealm).value()));
                assertThat(authenticatedUser.metadata(), hasKey(startsWith("jwt_claim_")));
            } else {
                assertThat(
                    authenticatedUser.metadata(),
                    equalTo(Map.of("jwt_token_type", JwtRealmInspector.getTokenType(jwtRealm).value()))
                );
            }
            // if the cache is enabled ensure the cache is used and does not change for the provided jwt
            if (jwtRealm.getJwtCache() != null) {
                Cache<BytesArray, JwtRealm.ExpiringUser> cache = jwtRealm.getJwtCache();
                if (firstCacheKeyFound == null) {
                    assertNotNull("could not find cache keys", cache.keys());
                    firstCacheKeyFound = cache.keys().iterator().next();
                }
                jwtAuthenticationToken.clearCredentials(); // simulates the realm's context closing which clears the credential
                boolean foundInCache = false;
                for (BytesArray key : cache.keys()) {
                    logger.trace("cache key: " + HexFormat.of().formatHex(key.array()));
                    if (key.equals(firstCacheKeyFound)) {
                        foundInCache = true;
                    }
                    assertFalse(
                        "cache key should not be nulled out",
                        IntStream.range(0, key.array().length).map(idx -> key.array()[idx]).allMatch(b -> b == 0)
                    );
                }
                assertTrue("cache key was not found in cache", foundInCache);
            }
        }
        logger.debug("Test succeeded");
    }

    protected User randomUser(final JwtIssuer jwtIssuer) {
        final User user = randomFrom(jwtIssuer.principals.values());
        logger.debug("USER[" + user.principal() + "]: roles=[" + String.join(",", user.roles()) + "].");
        return user;
    }

    protected SecureString randomJwt(final JwtIssuerAndRealm jwtIssuerAndRealm, User user) throws Exception {
        final JwtIssuer.AlgJwkPair algJwkPair = randomFrom(jwtIssuerAndRealm.issuer.algAndJwksAll);
        final JWK jwk = algJwkPair.jwk();
        logger.debug(
            "ALG["
                + algJwkPair.alg()
                + "]. JWK: kty=["
                + jwk.getKeyType()
                + "], len=["
                + jwk.size()
                + "], alg=["
                + jwk.getAlgorithm()
                + "], use=["
                + jwk.getKeyUse()
                + "], ops=["
                + jwk.getKeyOperations()
                + "], kid=["
                + jwk.getKeyID()
                + "]."
        );

        final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
            randomBoolean() ? null : JOSEObjectType.JWT.toString(), // kty
            randomBoolean() ? null : jwk.getKeyID(), // kid
            algJwkPair.alg(), // alg
            randomAlphaOfLengthBetween(10, 20), // jwtID
            JwtRealmInspector.getAllowedIssuer(jwtIssuerAndRealm.realm), // iss
            JwtRealmInspector.getAllowedAudiences(jwtIssuerAndRealm.realm), // aud
            randomBoolean() ? user.principal() : user.principal() + "_" + randomInt(9), // sub claim value
            JwtRealmInspector.getPrincipalClaimName(jwtIssuerAndRealm.realm), // principal claim name
            user.principal(), // principal claim value
            JwtRealmInspector.getGroupsClaimName(jwtIssuerAndRealm.realm), // group claim name
            List.of(user.roles()), // group claim value
            Date.from(now.minusSeconds(60 * randomLongBetween(10, 20))), // auth_time
            Date.from(now.minusSeconds(randomBoolean() ? 0 : 60 * randomLongBetween(5, 10))), // iat
            Date.from(now), // nbf
            Date.from(now.plusSeconds(60 * randomLongBetween(3600, 7200))), // exp
            randomBoolean() ? null : new Nonce(32).toString(),
            randomBoolean() ? null : Map.of("other1", randomAlphaOfLength(10), "other2", randomAlphaOfLength(10))
        );
        final SecureString signedJWT = signJwt(jwk, unsignedJwt);
        return signedJWT;
    }

    protected void printJwtRealmAndIssuer(JwtIssuerAndRealm jwtIssuerAndRealm) throws ParseException {
        printJwtIssuer(jwtIssuerAndRealm.issuer());
        printJwtRealm(jwtIssuerAndRealm.realm());
    }

    protected void printJwtRealm(final JwtRealm jwtRealm) {
        logger.debug(
            "REALM["
                + jwtRealm.name()
                + ","
                + jwtRealm.order()
                + "/"
                + jwtIssuerAndRealms.size()
                + "]: clientType=["
                + JwtRealmInspector.getClientAuthenticationType(jwtRealm)
                + "], clientSecret=["
                + JwtRealmInspector.getClientAuthenticationSharedSecret(jwtRealm)
                + "], iss=["
                + JwtRealmInspector.getAllowedIssuer(jwtRealm)
                + "], aud="
                + JwtRealmInspector.getAllowedAudiences(jwtRealm)
                + ", algsHmac="
                + JwtRealmInspector.getAllowedJwksAlgsHmac(jwtRealm)
                + ", filteredHmac="
                + JwtRealmInspector.getJwksAlgsHmac(jwtRealm).algs()
                + ", algsPkc="
                + JwtRealmInspector.getAllowedJwksAlgsPkc(jwtRealm)
                + ", filteredPkc="
                + JwtRealmInspector.getJwksAlgsPkc(jwtRealm).algs()
                + ", claimPrincipal=["
                + JwtRealmInspector.getPrincipalClaimName(jwtRealm)
                + "], claimGroups=["
                + JwtRealmInspector.getGroupsClaimName(jwtRealm)
                + "], authz=["
                + jwtRealm.delegatedAuthorizationSupport.hasDelegation()
                + "], meta=["
                + JwtRealmInspector.shouldPopulateUserMetadata(jwtRealm)
                + "], jwkSetPath=["
                + JwtRealmInspector.getJwkSetPath(jwtRealm)
                + "]."
        );
        for (final JWK jwk : JwtRealmInspector.getJwksAlgsHmac(jwtRealm).jwks()) {
            logger.debug("REALM HMAC: jwk=[{}]", jwk);
        }
        for (final JWK jwk : JwtRealmInspector.getJwksAlgsPkc(jwtRealm).jwks()) {
            logger.debug("REALM PKC: jwk=[{}]", jwk);
        }
    }

    protected void printJwtIssuer(final JwtIssuer jwtIssuer) {
        logger.debug(
            "ISSUER: iss=["
                + jwtIssuer.issuerClaimValue
                + "], aud=["
                + String.join(",", jwtIssuer.audiencesClaimValue)
                + "], principal=["
                + jwtIssuer.principalClaimName
                + "], algorithms=["
                + String.join(",", jwtIssuer.algorithmsAll)
                + "], httpServer=["
                + (jwtIssuer.httpsServer != null)
                + "]."
        );
        if (jwtIssuer.algAndJwkHmacOidc != null) {
            logger.debug("ISSUER HMAC OIDC: alg=[{}] jwk=[{}]", jwtIssuer.algAndJwkHmacOidc.alg(), jwtIssuer.encodedKeyHmacOidc);
        }
        for (final JwtIssuer.AlgJwkPair pair : jwtIssuer.algAndJwksHmac) {
            logger.debug("ISSUER HMAC: alg=[{}] jwk=[{}]", pair.alg(), pair.jwk());
        }
        for (final JwtIssuer.AlgJwkPair pair : jwtIssuer.algAndJwksPkc) {
            logger.debug("ISSUER PKC: alg=[{}] jwk=[{}]", pair.alg(), pair.jwk());
        }
    }
}
