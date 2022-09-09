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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.ClientAuthenticationType;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmsServiceSettings;
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

public abstract class JwtRealmTestCase extends JwtTestCase {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealmTestCase.class);

    record JwtRealmsServiceSettingsBuilder(Settings.Builder settingsBuilder) {}

    record JwtRealmSettingsBuilder(String name, Settings.Builder settingsBuilder) {}

    record JwtIssuerAndRealm(JwtIssuer issuer, JwtRealm realm, JwtRealmSettingsBuilder realmSettingsBuilder) {}

    record MinMax(int min, int max) {
        MinMax {
            assert min >= 0 && max >= min : "Invalid min=" + min + " max=" + max;
        }
    }

    protected ThreadPool threadPool;
    protected ResourceWatcherService resourceWatcherService;
    protected MockLicenseState licenseState;
    protected List<JwtIssuerAndRealm> jwtIssuerAndRealms;

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
            this.jwtIssuerAndRealms.forEach(jwtIssuerAndRealm -> {
                jwtIssuerAndRealm.realm.close(); // Close HTTPS client (if any)
                jwtIssuerAndRealm.issuer.close(); // Close HTTPS server (if any)
            });
        }
        this.resourceWatcherService.close();
        terminate(this.threadPool);
    }

    protected void verifyAuthenticateFailureHelper(
        final JwtIssuerAndRealm jwtIssuerAndRealm,
        final SecureString jwt,
        final SecureString clientSecret
    ) throws InterruptedException, ExecutionException {
        final ThreadContext tc = super.createThreadContext(jwt, clientSecret);
        final JwtAuthenticationToken token = (JwtAuthenticationToken) jwtIssuerAndRealm.realm.token(tc);
        final PlainActionFuture<AuthenticationResult<User>> plainActionFuture = PlainActionFuture.newFuture();
        jwtIssuerAndRealm.realm.authenticate(token, plainActionFuture);
        assertThat(plainActionFuture.get(), is(notNullValue()));
        assertThat(plainActionFuture.get().isAuthenticated(), is(false));
    }

    protected JwtRealmsService generateJwtRealmsService(final JwtRealmsServiceSettingsBuilder jwtRealmRealmsSettingsBuilder) {
        return new JwtRealmsService(jwtRealmRealmsSettingsBuilder.settingsBuilder.build());
    }

    protected List<JwtIssuerAndRealm> generateJwtIssuerRealmPairs(
        final JwtRealmsServiceSettingsBuilder jwtRealmsServiceSettingsBuilder,
        final MinMax realmsRange,
        final MinMax authzRange,
        final MinMax algsRange,
        final MinMax audiencesRange,
        final MinMax usersRange,
        final MinMax rolesRange,
        final MinMax jwtCacheSizeRange,
        final boolean createHttpsServer
    ) throws Exception {
        assertThat(realmsRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(authzRange.min(), is(greaterThanOrEqualTo(0)));
        assertThat(algsRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(audiencesRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(usersRange.min(), is(greaterThanOrEqualTo(1)));
        assertThat(rolesRange.min(), is(greaterThanOrEqualTo(0)));
        assertThat(jwtCacheSizeRange.min(), is(greaterThanOrEqualTo(0)));

        // Create JWT authc realms and mocked authz realms. Initialize each JWT realm, and test ensureInitialized() before and after.
        final JwtRealmsService jwtRealmsService = this.generateJwtRealmsService(jwtRealmsServiceSettingsBuilder);
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

            final JwtIssuer jwtIssuer = this.createJwtIssuer(
                i,
                randomFrom(jwtRealmsService.getPrincipalClaimNames()),
                algsCount,
                audiencesCount,
                usersCount,
                rolesCount,
                createHttpsServer
            );
            // If HTTPS server was created in JWT issuer, any exception after that point requires closing it to avoid a thread pool leak
            try {
                final JwtRealmSettingsBuilder realmSettingsBuilder = this.createJwtRealmSettingsBuilder(
                    jwtIssuer,
                    authzCount,
                    jwtCacheSize
                );
                final JwtRealm jwtRealm = this.createJwtRealm(allRealms, jwtRealmsService, jwtIssuer, realmSettingsBuilder);

                // verify exception before initialize()
                final Exception exception = expectThrows(IllegalStateException.class, jwtRealm::ensureInitialized);
                assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));

                final JwtIssuerAndRealm jwtIssuerAndRealm = new JwtIssuerAndRealm(jwtIssuer, jwtRealm, realmSettingsBuilder);
                this.jwtIssuerAndRealms.add(jwtIssuerAndRealm);
            } catch (Throwable t) {
                jwtIssuer.close();
                throw t;
            }
        }
        allRealms.forEach(realm -> realm.initialize(allRealms, this.licenseState)); // JWT realms and authz realms
        this.jwtIssuerAndRealms.forEach(p -> p.realm.ensureInitialized()); // verify no exception after initialize()
        return this.jwtIssuerAndRealms;
    }

    protected JwtIssuer createJwtIssuer(
        final int i,
        final String principalClaimName,
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
        final JwtIssuer jwtIssuer = new JwtIssuer(issuer, audiences, principalClaimName, users, createHttpsServer);
        final List<String> algorithms = randomOfMinMaxNonUnique(algsCount, algsCount, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final boolean areHmacJwksOidcSafe = randomBoolean();
        final List<JwtIssuer.AlgJwkPair> algAndJwks = JwtRealmTestCase.randomJwks(algorithms, areHmacJwksOidcSafe);
        jwtIssuer.setJwks(algAndJwks, areHmacJwksOidcSafe);
        return jwtIssuer;
    }

    protected void copyIssuerJwksToRealmConfig(final JwtIssuerAndRealm jwtIssuerAndRealm) throws Exception {
        if ((jwtIssuerAndRealm.realm.isConfiguredJwkSetPkc) && (jwtIssuerAndRealm.realm.getJwkSetPathUri() == null)) {
            LOGGER.trace("Updating JwtRealm PKC public JWKSet local file");
            final Path path = PathUtils.get(jwtIssuerAndRealm.realm.jwkSetPath);
            Files.writeString(path, jwtIssuerAndRealm.issuer.encodedJwkSetPkcPublic);
        }

        // TODO If x-pack Security plug-in add supports for reloadable settings, update HMAC JWKSet and HMAC OIDC JWK in ES Keystore
    }

    protected JwtRealmsServiceSettingsBuilder createJwtRealmsSettingsBuilder() throws Exception {
        final List<String> principalClaimNames = randomBoolean()
            ? List.of("principalClaim_" + randomAlphaOfLength(6))
            : randomSubsetOf(randomIntBetween(1, 6), JwtRealmsServiceSettings.DEFAULT_PRINCIPAL_CLAIMS);

        final Settings.Builder jwtRealmsServiceSettings = Settings.builder()
            .put(this.globalSettings)
            .put(JwtRealmsServiceSettings.PRINCIPAL_CLAIMS_SETTING.getKey(), String.join(",", principalClaimNames));

        final MockSecureSettings secureSettings = new MockSecureSettings(); // none for now, placeholder for future
        jwtRealmsServiceSettings.setSecureSettings(secureSettings);

        return new JwtRealmsServiceSettingsBuilder(jwtRealmsServiceSettings);
    }

    protected JwtRealmSettingsBuilder createJwtRealmSettingsBuilder(final JwtIssuer jwtIssuer, final int authzCount, final int jwtCacheSize)
        throws Exception {
        final String authcRealmName = "realm_" + jwtIssuer.issuerClaimValue;
        final String[] authzRealmNames = IntStream.range(0, authzCount).mapToObj(z -> authcRealmName + "_authz" + z).toArray(String[]::new);

        final ClientAuthenticationType clientAuthenticationType = randomFrom(ClientAuthenticationType.values());
        final Settings.Builder authcSettings = Settings.builder()
            .put(this.globalSettings)
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
                jwkSetPath = super.saveToTempFile("jwkset.", ".json", jwtIssuer.encodedJwkSetPkcPublic);
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
                randomIntBetween(10, 120) + randomFrom("s", "m", "h")
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
        final JwtRealmsService jwtRealmsService,
        final JwtIssuer jwtIssuer,
        final JwtRealmSettingsBuilder realmSettingsBuilder
    ) {
        final String authcRealmName = realmSettingsBuilder.name;
        final Settings settings = realmSettingsBuilder.settingsBuilder.build();
        final RealmConfig authcConfig = super.buildRealmConfig(JwtRealmSettings.TYPE, authcRealmName, settings, (allRealms.size() + 1));
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        final List<String> authzRealmNames = settings.getAsList(
            RealmSettings.getFullSettingKey(authcRealmName, DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(JwtRealmSettings.TYPE))
        );
        final UserRoleMapper userRoleMapper = super.buildRoleMapper(authzRealmNames.isEmpty() ? jwtIssuer.principals : Map.of());

        // If authz names is not set, register the users here in the JWT authc realm.
        final JwtRealm jwtRealm = new JwtRealm(authcConfig, jwtRealmsService, sslService, userRoleMapper);
        allRealms.add(jwtRealm);

        // If authz names is set, register the users here in one of the authz realms.
        if (authzRealmNames.isEmpty() == false) {
            final String selected = randomFrom(authzRealmNames);
            for (final String authzRealmName : authzRealmNames) {
                final RealmConfig authzConfig = this.buildRealmConfig("authz", authzRealmName, Settings.EMPTY, allRealms.size() + 1);
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
        assertThat(this.jwtIssuerAndRealms, is(notNullValue()));
        assertThat(this.jwtIssuerAndRealms, is(not(empty())));
        final JwtIssuerAndRealm jwtIssuerAndRealm = randomFrom(this.jwtIssuerAndRealms);
        final JwtRealm jwtRealm = jwtIssuerAndRealm.realm;
        assertThat(jwtRealm, is(notNullValue()));
        assertThat(jwtRealm.allowedIssuer, is(equalTo(jwtIssuerAndRealm.issuer.issuerClaimValue))); // assert equal, don't print both
        assertThat(jwtIssuerAndRealm.issuer.audiencesClaimValue.stream().anyMatch(jwtRealm.allowedAudiences::contains), is(true));
        this.printJwtRealmAndIssuer(jwtIssuerAndRealm);
        return jwtIssuerAndRealm;
    }

    protected void doMultipleAuthcAuthzAndVerifySuccess(
        final JwtRealm jwtRealm,
        final User user,
        final SecureString jwt,
        final SecureString sharedSecret,
        final MinMax jwtAuthcRange
    ) throws Exception {
        assertThat(jwtAuthcRange.min(), is(greaterThanOrEqualTo(1)));

        // Select one JWT authc Issuer/Realm pair. Select one test user, to use inside the authc test loop.
        final List<JwtRealm> jwtRealmsList = this.jwtIssuerAndRealms.stream().map(p -> p.realm).toList();

        // Select different test JWKs from the JWT realm, and generate test JWTs for the test user. Run the JWT through the chain.
        final int jwtAuthcRepeats = randomIntBetween(jwtAuthcRange.min(), jwtAuthcRange.max());
        for (int authcRun = 1; authcRun <= jwtAuthcRepeats; authcRun++) {
            // Create request with headers set
            final ThreadContext requestThreadContext = super.createThreadContext(jwt, sharedSecret);
            LOGGER.info("REQ[" + authcRun + "/" + jwtAuthcRepeats + "] HEADERS=" + requestThreadContext.getHeaders());

            // Loop through all authc/authz realms. Confirm a JWT authc realm recognizes and extracts the request headers.
            JwtAuthenticationToken jwtAuthenticationToken = null;
            for (final JwtRealm candidateJwtRealm : jwtRealmsList) {
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
            LOGGER.info("GOT TOKEN: principal=[" + tokenPrincipal + "], jwt=[" + tokenJwt + "], secret=[" + tokenSecret + "].");

            // Loop through all authc/authz realms. Confirm user is returned with expected principal and roles.
            User authenticatedUser = null;
            final List<String> realmAuthenticationResults = new ArrayList<>();
            final List<String> realmUsageStats = new ArrayList<>();
            final List<Exception> realmFailureExceptions = new ArrayList<>(jwtRealmsList.size());
            try {
                for (final JwtRealm candidateJwtRealm : jwtRealmsList) {
                    LOGGER.info("TRY AUTHC: expected=[" + jwtRealm.name() + "], candidate[" + candidateJwtRealm.name() + "].");
                    final PlainActionFuture<AuthenticationResult<User>> authenticateFuture = PlainActionFuture.newFuture();
                    try {
                        candidateJwtRealm.authenticate(jwtAuthenticationToken, authenticateFuture);
                        final AuthenticationResult<User> authenticationResult = authenticateFuture.actionGet();
                        final Exception authenticationResultException = authenticationResult.getException();
                        final String realmResult = "  realms=["
                            + jwtRealmsList.size()
                            + "], expected=["
                            + jwtRealm.name()
                            + ","
                            + jwtRealm.order()
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
                                assertThat("Unexpected realm SUCCESS status", candidateJwtRealm.name(), is(equalTo(jwtRealm.name())));
                                assertThat("Expected realm authc false", authenticationResult.isAuthenticated(), is(equalTo(true)));
                                assertThat("Expected realm exception thrown", authenticationResult.getException(), is(nullValue()));
                                assertThat("Expected realm message null", authenticationResult.getMessage(), is(nullValue()));
                                assertThat("Expected realm metadata empty", authenticationResult.getMetadata(), is(anEmptyMap()));
                                authenticatedUser = authenticationResult.getValue();
                                assertThat("Expected realm user null", authenticatedUser, is(notNullValue()));
                                break;
                            case CONTINUE:
                                assertThat("Expected realm CONTINUE status", candidateJwtRealm.name(), is(not(equalTo(jwtRealm.name()))));
                                assertThat("Unexpected realm authc success", authenticationResult.isAuthenticated(), is(equalTo(false)));
                                continue;
                            case TERMINATE:
                                assertThat("Expected realm TERMINATE status", candidateJwtRealm.name(), is(not(equalTo(jwtRealm.name()))));
                                assertThat("Unexpected realm authc success", authenticationResult.isAuthenticated(), is(equalTo(false)));
                                break;
                            default:
                                fail("Unexpected AuthenticationResult.Status=[" + authenticationResult.getStatus() + "]");
                                break;
                        }
                        break; // Only SUCCESS falls through to here, break out of the loop
                    } catch (Exception e) {
                        realmFailureExceptions.add(e);
                        throw e;
                    } finally {
                        final PlainActionFuture<Map<String, Object>> usageStatsFuture = PlainActionFuture.newFuture();
                        candidateJwtRealm.usageStats(usageStatsFuture);
                        realmUsageStats.add(
                            "   realm["
                                + candidateJwtRealm.name()
                                + ","
                                + candidateJwtRealm.order()
                                + "/"
                                + jwtRealmsList.size()
                                + "], stats=["
                                + usageStatsFuture.actionGet()
                                + "]"
                        );
                    }
                }
                // Loop ended. Confirm user is returned with expected principal and roles.
                assertThat("Expected realm " + jwtRealm.name() + " to authenticate.", authenticatedUser, is(notNullValue()));
                assertThat(user.principal(), equalTo(authenticatedUser.principal()));
                assertThat(new TreeSet<>(Arrays.asList(user.roles())), equalTo(new TreeSet<>(Arrays.asList(authenticatedUser.roles()))));
                if (jwtRealm.delegatedAuthorizationSupport.hasDelegation()) {
                    assertThat(user.metadata(), is(equalTo(authenticatedUser.metadata()))); // delegated authz returns user's metadata
                } else if (jwtRealm.populateUserMetadata) {
                    assertThat(authenticatedUser.metadata(), is(not(anEmptyMap()))); // role mapping with flag true returns non-empty
                } else {
                    assertThat(authenticatedUser.metadata(), is(anEmptyMap())); // role mapping with flag false returns empty
                }
            } catch (Throwable t) {
                realmFailureExceptions.forEach(t::addSuppressed); // all previous realm exceptions
                // LOGGER.error("Unexpected exception.", t);
                throw t;
            } finally {
                LOGGER.info("STATS: expected=[" + jwtRealm.name() + "]\n" + String.join("\n", realmUsageStats));
                if (authenticatedUser != null) {
                    LOGGER.info("RESULT: expected=[" + jwtRealm.name() + "]\n" + String.join("\n", realmAuthenticationResults));
                }
            }
        }
        LOGGER.info("Test succeeded");
    }

    protected User randomUser(final JwtIssuer jwtIssuer) {
        final User user = randomFrom(jwtIssuer.principals.values());
        LOGGER.info("USER[" + user.principal() + "]: roles=[" + String.join(",", user.roles()) + "].");
        return user;
    }

    protected SecureString randomJwt(final JwtIssuerAndRealm jwtIssuerAndRealm, User user) throws Exception {
        final JwtIssuer.AlgJwkPair algJwkPair = randomFrom(jwtIssuerAndRealm.issuer.algAndJwksAll);
        final JWK jwk = algJwkPair.jwk();
        LOGGER.info(
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
            jwtIssuerAndRealm.realm.allowedIssuer, // iss
            jwtIssuerAndRealm.realm.allowedAudiences, // aud
            randomBoolean() ? null : randomBoolean() ? user.principal() : user.principal() + "_" + randomInt(9), // sub claim value
            jwtIssuerAndRealm.realm.claimParserPrincipal.getClaimName(), // principal claim name
            user.principal(), // principal claim value
            jwtIssuerAndRealm.realm.claimParserGroups.getClaimName(), // group claim name
            List.of(user.roles()), // group claim value
            Date.from(now.minusSeconds(60 * randomLongBetween(10, 20))), // auth_time
            Date.from(now.minusSeconds(randomBoolean() ? 0 : 60 * randomLongBetween(5, 10))), // iat
            Date.from(now), // nbf
            Date.from(now.plusSeconds(60 * randomLongBetween(3600, 7200))), // exp
            randomBoolean() ? null : new Nonce(32).toString(),
            randomBoolean() ? null : Map.of("other1", randomAlphaOfLength(10), "other2", randomAlphaOfLength(10))
        );
        final SecureString signedJWT = JwtValidateUtil.signJwt(jwk, unsignedJwt);
        assertThat(JwtValidateUtil.verifyJwt(jwk, SignedJWT.parse(signedJWT.toString())), is(equalTo(true)));
        return signedJWT;
    }

    protected void printJwtRealmAndIssuer(JwtIssuerAndRealm jwtIssuerAndRealm) throws ParseException {
        this.printJwtIssuer(jwtIssuerAndRealm.issuer());
        this.printJwtRealm(jwtIssuerAndRealm.realm());
    }

    protected void printJwtRealm(final JwtRealm jwtRealm) {
        LOGGER.info(
            "REALM["
                + jwtRealm.name()
                + ","
                + jwtRealm.order()
                + "/"
                + this.jwtIssuerAndRealms.size()
                + "]: clientType=["
                + jwtRealm.clientAuthenticationType
                + "], clientSecret=["
                + jwtRealm.clientAuthenticationSharedSecret
                + "], iss=["
                + jwtRealm.allowedIssuer
                + "], aud="
                + jwtRealm.allowedAudiences
                + ", algsHmac="
                + jwtRealm.allowedJwksAlgsHmac
                + ", filteredHmac="
                + jwtRealm.contentAndJwksAlgsHmac.jwksAlgs().algs()
                + ", algsPkc="
                + jwtRealm.allowedJwksAlgsPkc
                + ", filteredPkc="
                + jwtRealm.getJwksAlgsPkc().jwksAlgs().algs()
                + ", claimPrincipal=["
                + jwtRealm.claimParserPrincipal.getClaimName()
                + "], claimGroups=["
                + jwtRealm.claimParserGroups.getClaimName()
                + "], authz=["
                + jwtRealm.delegatedAuthorizationSupport.hasDelegation()
                + "], meta=["
                + jwtRealm.populateUserMetadata
                + "], jwkSetPath=["
                + jwtRealm.jwkSetPath
                + "]."
        );
        for (final JWK jwk : jwtRealm.contentAndJwksAlgsHmac.jwksAlgs().jwks()) {
            LOGGER.info("REALM HMAC: jwk=[{}]", jwk);
        }
        for (final JWK jwk : jwtRealm.getJwksAlgsPkc().jwksAlgs().jwks()) {
            LOGGER.info("REALM PKC: jwk=[{}]", jwk);
        }
    }

    protected void printJwtIssuer(final JwtIssuer jwtIssuer) {
        LOGGER.info(
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
            LOGGER.info("ISSUER HMAC OIDC: alg=[{}] jwk=[{}]", jwtIssuer.algAndJwkHmacOidc.alg(), jwtIssuer.encodedKeyHmacOidc);
        }
        for (final JwtIssuer.AlgJwkPair pair : jwtIssuer.algAndJwksHmac) {
            LOGGER.info("ISSUER HMAC: alg=[{}] jwk=[{}]", pair.alg(), pair.jwk());
        }
        for (final JwtIssuer.AlgJwkPair pair : jwtIssuer.algAndJwksPkc) {
            LOGGER.info("ISSUER PKC: alg=[{}] jwk=[{}]", pair.alg(), pair.jwk());
        }
    }
}
