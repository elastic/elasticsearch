/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.elasticsearch.xpack.security.support.FileReloadListener;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.metadata.resolver.impl.AbstractReloadingMetadataResolver;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.NameIDType;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;
import org.opensaml.saml.saml2.metadata.SingleSignOnService;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PrivilegedActionException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Basic unit tests for the SAMLRealm
 */
public class SamlRealmTests extends SamlTestCase {

    public static final String TEST_IDP_ENTITY_ID = "http://demo_josso_1.josso.dev.docker:8081/IDBUS/JOSSO-TUTORIAL/IDP1/SAML2/MD";
    private static final TimeValue METADATA_REFRESH = TimeValue.timeValueMillis(3000);

    private static final String REALM_NAME = "my-saml";
    private static final String REALM_SETTINGS_PREFIX = "xpack.security.authc.realms.saml." + REALM_NAME;

    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setupEnv() throws PrivilegedActionException {
        SamlUtils.initialize(logger);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
    }

    public void testReadIdpMetadataFromFile() throws Exception {
        final Path path = getDataPath("idp1.xml");
        Tuple<RealmConfig, SSLService> config = buildConfig(path.toString());
        final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
        Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple = SamlRealm.initializeResolver(
            logger,
            config.v1(),
            config.v2(),
            watcherService
        );
        try {
            assertIdp1MetadataParsedCorrectly(tuple.v2().get());
        } finally {
            tuple.v1().destroy();
        }
    }

    public void testReadIdpMetadataFromHttps() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = Files.readString(path);
        TestsSSLService sslService = buildTestSslService();
        try (MockWebServer proxyServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            assertEquals(0, proxyServer.requests().size());

            Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + proxyServer.getPort());
            logger.info("Settings\n{}", config.v1().settings().toDelimitedString('\n'));
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple = SamlRealm.initializeResolver(
                logger,
                config.v1(),
                config.v2(),
                watcherService
            );

            try {
                final int firstRequestCount = proxyServer.requests().size();
                assertThat(firstRequestCount, greaterThanOrEqualTo(1));
                assertIdp1MetadataParsedCorrectly(tuple.v2().get());
                assertBusy(() -> assertThat(proxyServer.requests().size(), greaterThan(firstRequestCount)));
            } finally {
                tuple.v1().destroy();
            }
        }
    }

    public void testFailOnErrorForInvalidHttpsMetadata() throws Exception {
        TestsSSLService sslService = buildTestSslService();
        try (MockWebServer webServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            webServer.start();
            webServer.enqueue(
                new MockResponse().setResponseCode(randomIntBetween(400, 405))
                    .setBody("No metadata available")
                    .addHeader("Content-Type", "text/plain")
            );
            assertEquals(0, webServer.requests().size());

            var metadataPath = "https://localhost:" + webServer.getPort();
            final Tuple<RealmConfig, SSLService> config = buildConfig(
                metadataPath,
                settings -> settings.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR),
                    true
                )
            );
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            var exception = expectThrows(
                ElasticsearchSecurityException.class,
                () -> SamlRealm.initializeResolver(logger, config.v1(), config.v2(), watcherService)
            );
            assertThat(exception, throwableWithMessage("cannot load SAML metadata from [" + metadataPath + "]"));
        }
    }

    public void testRetryFailedHttpsMetadata() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = Files.readString(path);
        TestsSSLService sslService = buildTestSslService();
        doTestReloadFailedHttpsMetadata(body, sslService, true);
        doTestReloadFailedHttpsMetadata(body, sslService, false);
    }

    /**
     * @param testBackgroundRefresh If {@code true}, the test asserts that the metadata is automatically loaded in the background.
     *                              If {@code false}, the test triggers activity on the realm to force a reload of the metadata.
     */
    private void doTestReloadFailedHttpsMetadata(String metadataBody, TestsSSLService sslService, boolean testBackgroundRefresh)
        throws Exception {
        try (MockWebServer webServer = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            webServer.start();
            webServer.enqueue(
                new MockResponse().setResponseCode(randomIntBetween(400, 405))
                    .setBody("No metadata available")
                    .addHeader("Content-Type", "text/plain")
            );
            assertEquals(0, webServer.requests().size());

            // Even with a long refresh we should automatically retry metadata that fails
            final TimeValue defaultRefreshTime = TimeValue.timeValueHours(24);
            // OpenSAML (4.0) has a bug that can attempt to set negative duration timers if the refresh is too short.
            // Don't set this too small or we may hit "java.lang.IllegalArgumentException: Negative delay."
            final TimeValue minimumRefreshTime = testBackgroundRefresh ? TimeValue.timeValueMillis(500) : defaultRefreshTime;

            final Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + webServer.getPort(), builder -> {
                builder.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH),
                    defaultRefreshTime.getStringRep()
                );
                builder.put(
                    SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH),
                    minimumRefreshTime.getStringRep()
                );
                if (randomBoolean()) {
                    builder.put(
                        SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR),
                        false
                    );
                }
            });
            logger.info("Settings\n{}", config.v1().settings().toDelimitedString('\n'));
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple = SamlRealm.initializeResolver(
                logger,
                config.v1(),
                config.v2(),
                watcherService
            );

            try {
                final int firstRequestCount = webServer.requests().size();
                assertThat(firstRequestCount, greaterThanOrEqualTo(1));
                assertThat(tuple.v2().get(), instanceOf(UnresolvedEntity.class));

                webServer.enqueue(
                    new MockResponse().setResponseCode(200).setBody(metadataBody).addHeader("Content-Type", "application/xml")
                );
                if (testBackgroundRefresh) {
                    assertBusy(() -> assertThat(webServer.requests().size(), greaterThan(firstRequestCount)), 2, TimeUnit.SECONDS);
                } else {
                    // The Supplier.get() call will trigger a reload anyway
                }
                assertBusy(() -> assertIdp1MetadataParsedCorrectly(tuple.v2().get()), 3, TimeUnit.SECONDS);

            } finally {
                tuple.v1().destroy();
            }
        }
    }

    public void testMinRefreshGreaterThanRefreshThrowsSettingsException() throws GeneralSecurityException, IOException {
        var refresh = randomTimeValue(20, 110, TimeUnit.MINUTES, TimeUnit.SECONDS);
        var minRefresh = randomTimeValue(2, 8, TimeUnit.HOURS);

        Tuple<RealmConfig, SSLService> tuple = buildConfig(
            "https://localhost:9900/metadata.xml",
            builder -> builder.put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH),
                refresh
            ).put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH), minRefresh)
        );
        final RealmConfig config = tuple.v1();
        final SSLService sslService = tuple.v2();

        final SettingsException settingsException = expectThrows(
            SettingsException.class,
            () -> SamlRealm.create(
                config,
                sslService,
                mock(ResourceWatcherService.class),
                mock(UserRoleMapper.class),
                mock(SingleSamlSpConfiguration.class)
            )
        );

        assertThat(
            settingsException,
            throwableWithMessage(
                containsString(
                    "the value ("
                        + minRefresh
                        + ") for ["
                        + SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH)
                        + "]"
                )
            )
        );
        assertThat(
            settingsException,
            throwableWithMessage(
                containsString(
                    "greater than the value ("
                        + refresh.getStringRep()
                        + ") for ["
                        + SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH)
                        + "]"
                )
            )
        );
    }

    public void testAbsurdlyLowMinimumRefreshThrowsException() {
        var minRefresh = randomBoolean()
            ? randomTimeValue(1, 450, TimeUnit.MILLISECONDS)
            : randomTimeValue(1, 999, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS);

        Tuple<RealmConfig, SSLService> tuple = buildConfig(
            "https://localhost:9900/metadata.xml",
            builder -> builder.put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH),
                minRefresh
            )
        );
        final RealmConfig config = tuple.v1();
        final SSLService sslService = tuple.v2();

        final IllegalArgumentException settingsException = expectThrows(
            IllegalArgumentException.class,
            () -> SamlRealm.create(
                config,
                sslService,
                mock(ResourceWatcherService.class),
                mock(UserRoleMapper.class),
                mock(SingleSamlSpConfiguration.class)
            )
        );

        assertThat(
            settingsException,
            throwableWithMessage(
                "failed to parse value ["
                    + minRefresh
                    + "] for setting ["
                    + SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH)
                    + "], must be >= [500ms]"
            )
        );
    }

    private UserRoleMapper mockRoleMapper(Set<String> rolesToReturn, AtomicReference<UserRoleMapper.UserData> userData) {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            userData.set((UserRoleMapper.UserData) invocation.getArguments()[0]);
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(rolesToReturn);
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        return roleMapper;
    }

    public void testAuthenticateWithEmptyRoleMapping() throws Exception {
        final AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        final UserRoleMapper roleMapper = mockRoleMapper(Set.of(), userData);

        final boolean testWithDelimiter = randomBoolean();
        final AuthenticationResult<User> result = performAuthentication(
            roleMapper,
            randomBoolean(),
            randomBoolean(),
            randomFrom(Boolean.TRUE, Boolean.FALSE, null),
            false,
            randomBoolean() ? REALM_NAME : null,
            testWithDelimiter ? List.of("STRIKE Team: Delta$shield") : Arrays.asList("avengers", "shield"),
            testWithDelimiter ? "$" : null,
            randomBoolean() ? List.of("superuser", "kibana_admin") : randomFrom(List.of(), null)
        );
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue().roles().length, equalTo(0));
    }

    public void testAuthenticateWithRoleMapping() throws Exception {
        final AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        final UserRoleMapper roleMapper = mockRoleMapper(Set.of("superuser", "kibana_admin"), userData);

        final boolean excludeRoles = randomBoolean();
        final List<String> rolesToExclude = excludeRoles ? List.of("superuser") : randomFrom(List.of(), null);
        final boolean useNameId = randomBoolean();
        final boolean principalIsEmailAddress = randomBoolean();
        final Boolean populateUserMetadata = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final boolean testWithDelimiter = randomBoolean();
        final AuthenticationResult<User> result = performAuthentication(
            roleMapper,
            useNameId,
            principalIsEmailAddress,
            populateUserMetadata,
            false,
            authenticatingRealm,
            testWithDelimiter ? List.of("STRIKE Team: Delta$shield") : Arrays.asList("avengers", "shield"),
            testWithDelimiter ? "$" : null,
            rolesToExclude
        );

        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue().principal(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(result.getValue().email(), equalTo("cbarton@shield.gov"));
        if (excludeRoles) {
            assertThat(result.getValue().roles(), arrayContainingInAnyOrder("kibana_admin"));
        } else {
            assertThat(result.getValue().roles(), arrayContainingInAnyOrder("kibana_admin", "superuser"));
        }
        if (populateUserMetadata == Boolean.FALSE) {
            // TODO : "saml_nameid" should be null too, but the logout code requires it for now.
            assertThat(result.getValue().metadata().get("saml_uid"), nullValue());
        } else {
            final String nameIdValue = principalIsEmailAddress ? "clint.barton@shield.gov" : "clint.barton";
            final String uidValue = principalIsEmailAddress ? "cbarton@shield.gov" : "cbarton";
            assertThat(result.getValue().metadata().get("saml_nameid"), equalTo(nameIdValue));
            assertThat(result.getValue().metadata().get("saml_uid"), instanceOf(Iterable.class));
            assertThat((Iterable<?>) result.getValue().metadata().get("saml_uid"), contains(uidValue));
        }

        assertThat(userData.get().getUsername(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        if (testWithDelimiter) {
            assertThat(userData.get().getGroups(), containsInAnyOrder("STRIKE Team: Delta", "shield"));
        } else {
            assertThat(userData.get().getGroups(), containsInAnyOrder("avengers", "shield"));
        }
    }

    public void testAuthenticateWithAuthorizingRealm() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onFailure(new RuntimeException("Role mapping should not be called"));
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());

        final boolean useNameId = randomBoolean();
        final boolean principalIsEmailAddress = randomBoolean();
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        AuthenticationResult<User> result = performAuthentication(
            roleMapper,
            useNameId,
            principalIsEmailAddress,
            null,
            true,
            authenticatingRealm
        );
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue().principal(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(result.getValue().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getValue().roles(), arrayContainingInAnyOrder("lookup_user_role"));
        assertThat(result.getValue().fullName(), equalTo("Clinton Barton"));
        assertThat(result.getValue().metadata().entrySet(), Matchers.iterableWithSize(1));
        assertThat(result.getValue().metadata().get("is_lookup"), Matchers.equalTo(true));
    }

    public void testAuthenticateWithWrongRealmName() throws Exception {
        AuthenticationResult<User> result = performAuthentication(
            mock(UserRoleMapper.class),
            randomBoolean(),
            randomBoolean(),
            null,
            true,
            REALM_NAME + randomAlphaOfLength(8)
        );
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
    }

    private AuthenticationResult<User> performAuthentication(
        UserRoleMapper roleMapper,
        boolean useNameId,
        boolean principalIsEmailAddress,
        Boolean populateUserMetadata,
        boolean useAuthorizingRealm,
        String authenticatingRealm
    ) throws Exception {
        return performAuthentication(
            roleMapper,
            useNameId,
            principalIsEmailAddress,
            populateUserMetadata,
            useAuthorizingRealm,
            authenticatingRealm,
            Arrays.asList("avengers", "shield"),
            null
        );
    }

    private AuthenticationResult<User> performAuthentication(
        UserRoleMapper roleMapper,
        boolean useNameId,
        boolean principalIsEmailAddress,
        Boolean populateUserMetadata,
        boolean useAuthorizingRealm,
        String authenticatingRealm,
        List<String> groups,
        String groupsDelimiter
    ) throws Exception {
        return performAuthentication(
            roleMapper,
            useNameId,
            principalIsEmailAddress,
            populateUserMetadata,
            useAuthorizingRealm,
            authenticatingRealm,
            groups,
            groupsDelimiter,
            null
        );
    }

    private AuthenticationResult<User> performAuthentication(
        UserRoleMapper roleMapper,
        boolean useNameId,
        boolean principalIsEmailAddress,
        Boolean populateUserMetadata,
        boolean useAuthorizingRealm,
        String authenticatingRealm,
        List<String> groups,
        String groupsDelimiter,
        List<String> rolesToExclude
    ) throws Exception {
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final String userPrincipal = useNameId ? "clint.barton" : "cbarton";
        final String nameIdValue = principalIsEmailAddress ? "clint.barton@shield.gov" : "clint.barton";
        final String uidValue = principalIsEmailAddress ? "cbarton@shield.gov" : "cbarton";
        final String realmType = SingleSpSamlRealmSettings.TYPE;

        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "mock_lookup");
        final MockLookupRealm lookupRealm = new MockLookupRealm(
            new RealmConfig(
                realmIdentifier,
                Settings.builder()
                    .put(globalSettings)
                    .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                    .build(),
                env,
                threadContext
            )
        );

        final Settings.Builder settingsBuilder = Settings.builder()
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()),
                useNameId ? "nameid" : "uid"
            )
            .put(
                RealmSettings.getFullSettingKey(
                    REALM_NAME,
                    SamlRealmSettings.GROUPS_ATTRIBUTE.apply(realmType).getAttributeSetting().getAttribute()
                ),
                "groups"
            )
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.MAIL_ATTRIBUTE.apply(realmType).getAttribute()), "mail");

        if (groupsDelimiter != null) {
            settingsBuilder.put(
                RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.GROUPS_ATTRIBUTE.apply(realmType).getDelimiter()),
                groupsDelimiter
            );
        }
        if (principalIsEmailAddress) {
            final boolean anchoredMatch = randomBoolean();
            settingsBuilder.put(
                RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getPattern()),
                anchoredMatch ? "^([^@]+)@shield.gov$" : "^([^@]+)@"
            );
        }
        if (populateUserMetadata != null) {
            settingsBuilder.put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.POPULATE_USER_METADATA),
                populateUserMetadata.booleanValue()
            );
        }
        if (rolesToExclude != null) {
            settingsBuilder.put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.EXCLUDE_ROLES),
                String.join(",", rolesToExclude)
            );
        }
        if (useAuthorizingRealm) {
            settingsBuilder.putList(
                RealmSettings.getFullSettingKey(
                    new RealmConfig.RealmIdentifier("saml", REALM_NAME),
                    DelegatedAuthorizationSettings.AUTHZ_REALMS
                ),
                lookupRealm.name()
            );
            lookupRealm.registerUser(
                new User(
                    userPrincipal,
                    new String[] { "lookup_user_role" },
                    "Clinton Barton",
                    "cbarton@shield.gov",
                    Collections.singletonMap("is_lookup", true),
                    true
                )
            );
        }

        final Settings realmSettings = settingsBuilder.build();
        final RealmConfig config = buildConfig(realmSettings);
        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);
        initializeRealms(realm, lookupRealm);

        final SamlToken token = new SamlToken(new byte[0], Collections.singletonList("<id>"), authenticatingRealm);

        final SamlAttributes attributes = new SamlAttributes(
            new SamlNameId(NameIDType.PERSISTENT, nameIdValue, idp.getEntityID(), sp.getEntityId(), null),
            randomAlphaOfLength(16),
            Arrays.asList(
                new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.1", "uid", Collections.singletonList(uidValue)),
                new SamlAttributes.SamlAttribute("urn:oid:1.3.6.1.4.1.5923.1.5.1.1", "groups", groups),
                new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail", Arrays.asList("cbarton@shield.gov"))
            )
        );
        when(authenticator.authenticate(token)).thenReturn(attributes);

        final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        return future.get();
    }

    private void initializeRealms(Realm... realms) {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);

        final List<Realm> realmList = Arrays.asList(realms);
        for (Realm realm : realms) {
            realm.initialize(realmList, licenseState);
        }
    }

    public SamlRealm buildRealm(
        RealmConfig config,
        UserRoleMapper roleMapper,
        SamlAuthenticator authenticator,
        SamlLogoutRequestHandler logoutHandler,
        EntityDescriptor idp,
        SpConfiguration sp
    ) throws Exception {
        try {
            return new SamlRealm(
                config,
                roleMapper,
                authenticator,
                logoutHandler,
                mock(SamlLogoutResponseHandler.class),
                () -> idp,
                sp,
                SamlRealmSettings.UserAttributeNameConfiguration.fromConfig(config)
            );
        } catch (SettingsException e) {
            logger.info(() -> format("Settings are invalid:\n%s", config.settings().toDelimitedString('\n')), e);
            throw e;
        }
    }

    public void testAttributeSelectionWithSplit() {
        List<String> strings = performAttributeSelectionWithSplit(",", "departments", "engineering", "elasticsearch-admins", "employees");
        assertThat("For attributes: " + strings, strings, contains("engineering", "elasticsearch-admins", "employees"));
    }

    public void testAttributeSelectionWithSplitEmptyInput() {
        List<String> strings = performAttributeSelectionWithSplit(",", "departments");
        assertThat("For attributes: " + strings, strings, is(empty()));
    }

    public void testAttributeSelectionWithSplitJustDelimiter() {
        List<String> strings = performAttributeSelectionWithSplit(",", ",");
        assertThat("For attributes: " + strings, strings, is(empty()));
    }

    public void testAttributeSelectionWithSplitNoDelimiter() {
        List<String> strings = performAttributeSelectionWithSplit(",", "departments", "elasticsearch-team");
        assertThat("For attributes: " + strings, strings, contains("elasticsearch-team"));
    }

    private List<String> performAttributeSelectionWithSplit(String delimiter, String groupAttributeName, String... returnedGroups) {
        final Settings settings = Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".attributes.groups", groupAttributeName)
            .put(REALM_SETTINGS_PREFIX + ".attribute_delimiters.groups", delimiter)
            .build();

        final RealmConfig config = buildConfig(settings);

        final SamlRealmSettings.AttributeSettingWithDelimiter groupSetting = new SamlRealmSettings.AttributeSettingWithDelimiter(
            config.type(),
            "groups"
        );
        final SamlRealm.AttributeParser parser = SamlRealm.AttributeParser.forSetting(logger, groupSetting, config);

        final SamlAttributes attributes = new SamlAttributes(
            new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(24), null, null, null),
            randomAlphaOfLength(16),
            Collections.singletonList(
                new SamlAttributes.SamlAttribute(
                    "departments",
                    "departments",
                    Collections.singletonList(String.join(delimiter, returnedGroups))
                )
            )
        );
        return parser.getAttribute(attributes);
    }

    public void testAttributeSelectionWithDelimiterAndPatternThrowsSettingsException() throws Exception {
        final Settings settings = Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".attributes.groups", "departments")
            .put(REALM_SETTINGS_PREFIX + ".attribute_delimiters.groups", ",")
            .put(REALM_SETTINGS_PREFIX + ".attribute_patterns.groups", "^(.+)@\\w+.example.com$")
            .build();

        final RealmConfig config = buildConfig(settings);

        final SamlRealmSettings.AttributeSettingWithDelimiter groupSetting = new SamlRealmSettings.AttributeSettingWithDelimiter(
            config.type(),
            "groups"
        );

        final SettingsException settingsException = expectThrows(
            SettingsException.class,
            () -> SamlRealm.AttributeParser.forSetting(logger, groupSetting, config)
        );

        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attribute_delimiters.groups"));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attribute_patterns.groups"));
    }

    public void testAttributeSelectionNoGroupsConfiguredThrowsSettingsException() {
        String delimiter = ",";
        final Settings settings = Settings.builder().put(REALM_SETTINGS_PREFIX + ".attribute_delimiters.groups", delimiter).build();
        final RealmConfig config = buildConfig(settings);
        final SamlRealmSettings.AttributeSettingWithDelimiter groupSetting = new SamlRealmSettings.AttributeSettingWithDelimiter(
            config.type(),
            "groups"
        );

        final SettingsException settingsException = expectThrows(
            SettingsException.class,
            () -> SamlRealm.AttributeParser.forSetting(logger, groupSetting, config)
        );

        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attribute_delimiters.groups"));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.groups"));
    }

    public void testAttributeSelectionWithSplitAndListThrowsSecurityException() {
        String delimiter = ",";

        final Settings settings = Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".attributes.groups", "departments")
            .put(REALM_SETTINGS_PREFIX + ".attribute_delimiters.groups", delimiter)
            .build();

        final RealmConfig config = buildConfig(settings);

        final SamlRealmSettings.AttributeSettingWithDelimiter groupSetting = new SamlRealmSettings.AttributeSettingWithDelimiter(
            config.type(),
            "groups"
        );
        final SamlRealm.AttributeParser parser = SamlRealm.AttributeParser.forSetting(logger, groupSetting, config);

        final SamlAttributes attributes = new SamlAttributes(
            new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(24), null, null, null),
            randomAlphaOfLength(16),
            Collections.singletonList(
                new SamlAttributes.SamlAttribute(
                    "departments",
                    "departments",
                    List.of("engineering", String.join(delimiter, "elasticsearch-admins", "employees"))
                )
            )
        );

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> parser.getAttribute(attributes)
        );

        assertThat(securityException.getMessage(), containsString("departments"));
    }

    public void testAttributeSelectionWithRegex() {
        final boolean useFriendlyName = randomBoolean();
        final Settings settings = Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".attributes.principal", useFriendlyName ? "mail" : "urn:oid:0.9.2342.19200300.100.1.3")
            .put(REALM_SETTINGS_PREFIX + ".attribute_patterns.principal", "^(.+)@\\w+.example.com$")
            .build();

        final RealmConfig config = buildConfig(settings);

        final SamlRealmSettings.AttributeSetting principalSetting = new SamlRealmSettings.AttributeSetting(config.type(), "principal");
        final SamlRealm.AttributeParser parser = SamlRealm.AttributeParser.forSetting(logger, principalSetting, config, false);

        final SamlAttributes attributes = new SamlAttributes(
            new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(24), null, null, null),
            randomAlphaOfLength(16),
            Collections.singletonList(
                new SamlAttributes.SamlAttribute(
                    "urn:oid:0.9.2342.19200300.100.1.3",
                    "mail",
                    Arrays.asList("john.smith@personal.example.net", "john.smith@corporate.example.com", "jsmith@corporate.example.com")
                )
            )
        );

        final List<String> strings = parser.getAttribute(attributes);
        assertThat("For attributes: " + strings, strings, contains("john.smith", "jsmith"));
    }

    public void testSettingPatternWithoutAttributeThrowsSettingsException() throws Exception {
        final String realmType = SingleSpSamlRealmSettings.TYPE;
        final Settings realmSettings = Settings.builder()
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()),
                "nameid"
            )
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.NAME_ATTRIBUTE.apply(realmType).getPattern()),
                "^\\s*(\\S.*\\S)\\s*$"
            )
            .build();
        final RealmConfig config = buildConfig(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());

        final SettingsException settingsException = expectThrows(
            SettingsException.class,
            () -> buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp)
        );
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attribute_patterns.name"));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.name"));
    }

    public void testSettingExcludeRolesAndAuthorizationRealmsThrowsException() throws Exception {
        final Settings realmSettings = Settings.builder()
            .putList(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.EXCLUDE_ROLES), "superuser", "kibana_admin")
            .putList(
                RealmSettings.getFullSettingKey(
                    new RealmConfig.RealmIdentifier("saml", REALM_NAME),
                    DelegatedAuthorizationSettings.AUTHZ_REALMS
                ),
                "ldap"
            )
            .put(
                RealmSettings.getFullSettingKey(
                    REALM_NAME,
                    SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(SingleSpSamlRealmSettings.TYPE).getAttribute()
                ),
                "mail"
            )
            .build();
        final RealmConfig config = buildConfig(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());

        var e = expectThrows(IllegalArgumentException.class, () -> buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp));

        assertThat(
            e.getCause().getMessage(),
            containsString(
                "Setting ["
                    + REALM_SETTINGS_PREFIX
                    + ".exclude_roles] is not permitted when setting ["
                    + REALM_SETTINGS_PREFIX
                    + ".authorization_realms] is configured."
            )
        );
    }

    public void testMissingPrincipalSettingThrowsSettingsException() throws Exception {
        final Settings realmSettings = Settings.EMPTY;
        final RealmConfig config = buildConfig(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());

        final SettingsException settingsException = expectThrows(
            SettingsException.class,
            () -> buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp)
        );
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.principal"));
    }

    public void testNonMatchingPrincipalPatternThrowsSamlException() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final String realmType = SingleSpSamlRealmSettings.TYPE;

        final Settings realmSettings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()), "mail")
            .put(
                RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getPattern()),
                "^([^@]+)@mycorp\\.example\\.com$"
            )
            .build();

        final RealmConfig config = buildConfig(realmSettings);

        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final SamlToken token = new SamlToken(new byte[0], Collections.singletonList("<id>"), authenticatingRealm);

        for (String mail : Arrays.asList("john@your-corp.example.com", "john@mycorp.example.com.example.net", "john")) {
            final SamlAttributes attributes = new SamlAttributes(
                new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(12), null, null, null),
                randomAlphaOfLength(16),
                Collections.singletonList(
                    new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail", Collections.singletonList(mail))
                )
            );
            when(authenticator.authenticate(token)).thenReturn(attributes);

            final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            final AuthenticationResult<User> result = future.actionGet();
            assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
            assertThat(result.getMessage(), containsString("attributes.principal"));
            assertThat(result.getMessage(), containsString("mail"));
            assertThat(result.getMessage(), containsString("@mycorp\\.example\\.com"));
        }
    }

    public void testCreateCredentialFromPemFiles() throws Exception {
        final Settings.Builder builder = buildSettings("http://example.com");
        final Path dir = createTempDir("encryption");
        final Path encryptionKeyPath = getDataPath("encryption.key");
        final Path destEncryptionKeyPath = dir.resolve("encryption.key");
        final PrivateKey encryptionKey = PemUtils.readPrivateKey(encryptionKeyPath, "encryption"::toCharArray);
        final Path encryptionCertPath = getDataPath("encryption.crt");
        final Path destEncryptionCertPath = dir.resolve("encryption.crt");
        final X509Certificate encryptionCert = CertParsingUtils.readX509Certificates(Collections.singletonList(encryptionCertPath))[0];
        Files.copy(encryptionKeyPath, destEncryptionKeyPath);
        Files.copy(encryptionCertPath, destEncryptionCertPath);
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.key", destEncryptionKeyPath);
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.certificate", destEncryptionCertPath);
        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);
        final Credential credential = SamlRealm.buildEncryptionCredential(realmConfig).get(0);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(encryptionKey));
        assertThat(credential.getPublicKey(), equalTo(encryptionCert.getPublicKey()));
    }

    public void testCreateEncryptionCredentialFromKeyStore() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PKCS12 keystores are not usable", inFipsJvm());
        final Path dir = createTempDir();
        final Settings.Builder builder = Settings.builder().put(REALM_SETTINGS_PREFIX + ".type", "saml").put("path.home", dir);
        final Path ksFile = dir.resolve("cred.p12");
        final boolean testMultipleEncryptionKeyPair = randomBoolean();
        final Tuple<X509Certificate, PrivateKey> certKeyPair1 = readKeyPair("RSA_4096");
        final Tuple<X509Certificate, PrivateKey> certKeyPair2 = readKeyPair("RSA_2048");
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null);
        ks.setKeyEntry(
            getAliasName(certKeyPair1),
            certKeyPair1.v2(),
            "key-password".toCharArray(),
            new Certificate[] { certKeyPair1.v1() }
        );
        if (testMultipleEncryptionKeyPair) {
            ks.setKeyEntry(
                getAliasName(certKeyPair2),
                certKeyPair2.v2(),
                "key-password".toCharArray(),
                new Certificate[] { certKeyPair2.v1() }
            );
        }
        try (OutputStream out = Files.newOutputStream(ksFile)) {
            ks.store(out, "ks-password".toCharArray());
        }
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.keystore.path", ksFile.toString());
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.keystore.type", "PKCS12");
        final boolean isEncryptionKeyStoreAliasSet = randomBoolean();
        if (isEncryptionKeyStoreAliasSet) {
            builder.put(REALM_SETTINGS_PREFIX + ".encryption.keystore.alias", getAliasName(certKeyPair1));
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".encryption.keystore.secure_password", "ks-password");
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".encryption.keystore.secure_key_password", "key-password");
        builder.setSecureSettings(secureSettings);

        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);
        final List<X509Credential> credentials = SamlRealm.buildEncryptionCredential(realmConfig);

        assertThat("Encryption Credentials should not be null", credentials, notNullValue());
        final int expectedCredentials = (isEncryptionKeyStoreAliasSet) ? 1 : (testMultipleEncryptionKeyPair) ? 2 : 1;
        assertEquals("Expected encryption credentials size does not match", expectedCredentials, credentials.size());
        credentials.forEach((credential) -> {
            assertTrue(
                "Unexpected private key in the list of encryption credentials",
                Arrays.asList(new PrivateKey[] { certKeyPair1.v2(), certKeyPair2.v2() }).contains(credential.getPrivateKey())
            );
            assertTrue(
                "Unexpected public key in the list of encryption credentials",
                Arrays.asList(new PublicKey[] { (certKeyPair1.v1()).getPublicKey(), certKeyPair2.v1().getPublicKey() })
                    .contains(credential.getPublicKey())
            );
        });
    }

    public void testCreateSigningCredentialFromKeyStoreSuccessScenarios() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PKCS12 keystores are not usable", inFipsJvm());
        final Path dir = createTempDir();
        final Settings.Builder builder = Settings.builder().put(REALM_SETTINGS_PREFIX + ".type", "saml").put("path.home", dir);
        final Path ksFile = dir.resolve("cred.p12");
        final Tuple<X509Certificate, PrivateKey> certKeyPair1 = readRandomKeyPair("RSA");
        final Tuple<X509Certificate, PrivateKey> certKeyPair2 = readRandomKeyPair("EC");

        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null);
        ks.setKeyEntry(
            getAliasName(certKeyPair1),
            certKeyPair1.v2(),
            "key-password".toCharArray(),
            new Certificate[] { certKeyPair1.v1() }
        );
        ks.setKeyEntry(
            getAliasName(certKeyPair2),
            certKeyPair2.v2(),
            "key-password".toCharArray(),
            new Certificate[] { certKeyPair2.v1() }
        );
        try (OutputStream out = Files.newOutputStream(ksFile)) {
            ks.store(out, "ks-password".toCharArray());
        }
        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.path", ksFile.toString());
        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.type", "PKCS12");
        final boolean isSigningKeyStoreAliasSet = randomBoolean();
        if (isSigningKeyStoreAliasSet) {
            builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.alias", getAliasName(certKeyPair1));
        }

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_password", "ks-password");
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_key_password", "key-password");
        builder.setSecureSettings(secureSettings);

        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);

        // Should build signing credential and use the key from KS.
        final SigningConfiguration signingConfig = SamlRealm.buildSigningConfiguration(realmConfig);
        final Credential credential = signingConfig.getCredential();
        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(certKeyPair1.v2()));
        assertThat(credential.getPublicKey(), equalTo(certKeyPair1.v1().getPublicKey()));
    }

    public void testCreateSigningCredentialFromKeyStoreFailureScenarios() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PKCS12 keystores are not usable", inFipsJvm());
        final Path dir = createTempDir();
        final Settings.Builder builder = Settings.builder().put(REALM_SETTINGS_PREFIX + ".type", "saml").put("path.home", dir);
        final Path ksFile = dir.resolve("cred.p12");
        final Tuple<X509Certificate, PrivateKey> certKeyPair1 = readKeyPair("RSA_4096");
        final Tuple<X509Certificate, PrivateKey> certKeyPair2 = readKeyPair("RSA_2048");
        final Tuple<X509Certificate, PrivateKey> certKeyPair3 = readRandomKeyPair("EC");

        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null);
        final boolean noRSAKeysInKS = randomBoolean();
        if (noRSAKeysInKS == false) {
            ks.setKeyEntry(
                getAliasName(certKeyPair1),
                certKeyPair1.v2(),
                "key-password".toCharArray(),
                new Certificate[] { certKeyPair1.v1() }
            );
            ks.setKeyEntry(
                getAliasName(certKeyPair2),
                certKeyPair2.v2(),
                "key-password".toCharArray(),
                new Certificate[] { certKeyPair2.v1() }
            );
        }
        ks.setKeyEntry(
            getAliasName(certKeyPair3),
            certKeyPair3.v2(),
            "key-password".toCharArray(),
            new Certificate[] { certKeyPair3.v1() }
        );
        try (OutputStream out = Files.newOutputStream(ksFile)) {
            ks.store(out, "ks-password".toCharArray());
        }

        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.path", ksFile.toString());
        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.type", "PKCS12");
        final boolean isSigningKeyStoreAliasSet = randomBoolean();
        final Tuple<X509Certificate, PrivateKey> chosenAliasCertKeyPair;
        final String unknownAlias = randomAlphaOfLength(5);
        if (isSigningKeyStoreAliasSet) {
            chosenAliasCertKeyPair = randomFrom(Arrays.asList(certKeyPair3, null));
            if (chosenAliasCertKeyPair == null) {
                // Unknown alias
                builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.alias", unknownAlias);
            } else {
                builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.alias", getAliasName(chosenAliasCertKeyPair));
            }
        } else {
            chosenAliasCertKeyPair = null;
        }

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_password", "ks-password");
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_key_password", "key-password");
        builder.setSecureSettings(secureSettings);

        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);

        if (isSigningKeyStoreAliasSet) {
            if (chosenAliasCertKeyPair == null) {
                // Unknown alias, this must throw exception
                final IllegalArgumentException illegalArgumentException = expectThrows(
                    IllegalArgumentException.class,
                    () -> SamlRealm.buildSigningConfiguration(realmConfig)
                );
                final String expectedErrorMessage = "The configured key store for "
                    + RealmSettings.realmSettingPrefix(realmConfig.identifier())
                    + "signing."
                    + " does not have a key associated with alias ["
                    + unknownAlias
                    + "] "
                    + "(from setting "
                    + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_KEY_ALIAS)
                    + ")";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            } else {
                final String chosenAliasName = getAliasName(chosenAliasCertKeyPair);
                // Since this is unsupported key type, this must throw exception
                final IllegalArgumentException illegalArgumentException = expectThrows(
                    IllegalArgumentException.class,
                    () -> SamlRealm.buildSigningConfiguration(realmConfig)
                );
                final String expectedErrorMessage = "The key associated with alias ["
                    + chosenAliasName
                    + "] "
                    + "(from setting "
                    + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_KEY_ALIAS)
                    + ") uses unsupported key algorithm type ["
                    + chosenAliasCertKeyPair.v2().getAlgorithm()
                    + "], only RSA is supported";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            }
        } else {
            if (noRSAKeysInKS) {
                // Should throw exception as no RSA keys in the keystore
                final IllegalArgumentException illegalArgumentException = expectThrows(
                    IllegalArgumentException.class,
                    () -> SamlRealm.buildSigningConfiguration(realmConfig)
                );
                final String expectedErrorMessage = "The configured key store for "
                    + RealmSettings.realmSettingPrefix(realmConfig.identifier())
                    + "signing."
                    + " does not contain any RSA key pairs";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            } else {
                // Should throw exception when multiple signing keys found and alias not set
                final IllegalArgumentException illegalArgumentException = expectThrows(
                    IllegalArgumentException.class,
                    () -> SamlRealm.buildSigningConfiguration(realmConfig)
                );
                final String expectedErrorMessage = "The configured key store for "
                    + RealmSettings.realmSettingPrefix(realmConfig.identifier())
                    + "signing."
                    + " has multiple keys but no alias has been specified (from setting "
                    + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_KEY_ALIAS)
                    + ")";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            }
        }
    }

    private String getAliasName(final Tuple<X509Certificate, PrivateKey> certKeyPair) {
        // Keys are pre-generated with the same name, so add the serial no to the alias so that keystore entries won't be overwritten
        return certKeyPair.v1().getSubjectX500Principal().getName().toLowerCase(Locale.US)
            + "-"
            + certKeyPair.v1().getSerialNumber()
            + "-alias";
    }

    public void testBuildLogoutRequest() throws Exception {
        final Boolean useSingleLogout = randomFrom(true, false, null);
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final IDPSSODescriptor role = mock(IDPSSODescriptor.class);
        final SingleLogoutService slo = SamlUtils.buildObject(SingleLogoutService.class, SingleLogoutService.DEFAULT_ELEMENT_NAME);
        when(idp.getRoleDescriptors(IDPSSODescriptor.DEFAULT_ELEMENT_NAME)).thenReturn(Collections.singletonList(role));
        when(role.getSingleLogoutServices()).thenReturn(Collections.singletonList(slo));
        slo.setBinding(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
        slo.setLocation("https://logout.saml/");

        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final String realmType = SingleSpSamlRealmSettings.TYPE;

        final Settings.Builder realmSettings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()), "uid");
        if (useSingleLogout != null) {
            realmSettings.put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_SINGLE_LOGOUT),
                useSingleLogout.booleanValue()
            );
        }

        final RealmConfig config = buildConfig(realmSettings.build());

        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);

        final NameID nameId = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameId.setFormat(NameID.TRANSIENT);
        nameId.setValue(SamlUtils.generateSecureNCName(18));
        final String session = SamlUtils.generateSecureNCName(12);

        final LogoutRequest request = realm.buildLogoutRequest(nameId, session);
        if (Boolean.FALSE.equals(useSingleLogout)) {
            assertThat(request, nullValue());
        } else {
            assertThat(request, notNullValue());
            assertThat(request.getDestination(), equalTo("https://logout.saml/"));
            assertThat(request.getNameID(), equalTo(nameId));
            assertThat(request.getSessionIndexes(), iterableWithSize(1));
            assertThat(request.getSessionIndexes().get(0).getValue(), equalTo(session));
        }
    }

    public void testCorrectRealmSelected() throws Exception {
        final String acsUrl = "https://idp.test/saml/login";
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SingleSamlSpConfiguration("<sp>", acsUrl, null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final String realmType = SingleSpSamlRealmSettings.TYPE;
        final Settings.Builder realmSettings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()), "uid")
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), "http://url.to/metadata")
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID), TEST_IDP_ENTITY_ID)
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SingleSpSamlRealmSettings.SP_ACS), acsUrl);
        final RealmConfig config = buildConfig(realmSettings.build());
        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);
        final Realms realms = mock(Realms.class);
        when(realms.realm(REALM_NAME)).thenReturn(realm);
        when(realms.stream()).thenAnswer(i -> Stream.of(realm));
        final String emptyRealmName = randomBoolean() ? null : "";
        assertThat(SamlRealm.findSamlRealms(realms, emptyRealmName, acsUrl), hasSize(1));
        assertThat(SamlRealm.findSamlRealms(realms, emptyRealmName, acsUrl).get(0), equalTo(realm));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", acsUrl), hasSize(1));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", acsUrl).get(0), equalTo(realm));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", null), hasSize(1));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", null).get(0), equalTo(realm));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", "https://idp.test:443/saml/login"), empty());
        assertThat(SamlRealm.findSamlRealms(realms, "incorrect", acsUrl), empty());
        assertThat(SamlRealm.findSamlRealms(realms, "incorrect", "https://idp.test:443/saml/login"), empty());
    }

    public void testReadDifferentIdpMetadataSameKeyFromFiles() throws Exception {
        // Confirm these files are located in /x-pack/plugin/security/src/test/resources/org/elasticsearch/xpack/security/authc/saml/
        final Path originalMetadataPath = getDataPath("idp1.xml");
        final Path updatedMetadataPath = getDataPath("idp1-same-certs-updated-id-cacheDuration.xml");
        assertThat(Files.exists(originalMetadataPath), is(true));
        assertThat(Files.exists(updatedMetadataPath), is(true));
        // Confirm the file contents are different
        assertThat(Files.readString(originalMetadataPath), is(not(equalTo(Files.readString(updatedMetadataPath)))));

        // Use a temp file to trigger load and reload by ResourceWatcherService
        final Path realmMetadataPath = Files.createTempFile(PathUtils.get(createTempDir().toString()), "idp1-metadata", "xml");

        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(SingleSpSamlRealmSettings.TYPE, "saml-idp1");
        final RealmConfig realmConfig = new RealmConfig(
            realmIdentifier,
            Settings.builder().put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 1).build(),
            this.env,
            this.threadContext
        );

        final TestThreadPool testThreadPool = new TestThreadPool("Async Reload");
        try {
            // Put original metadata contents into realm metadata file
            Files.writeString(realmMetadataPath, Files.readString(originalMetadataPath));

            final TimeValue timeValue = TimeValue.timeValueMillis(10);
            final Settings resourceWatcherSettings = Settings.builder()
                .put(ResourceWatcherService.RELOAD_INTERVAL_HIGH.getKey(), timeValue)
                .put(ResourceWatcherService.RELOAD_INTERVAL_MEDIUM.getKey(), timeValue)
                .put(ResourceWatcherService.RELOAD_INTERVAL_LOW.getKey(), timeValue)
                .build();
            try (ResourceWatcherService watcherService = new ResourceWatcherService(resourceWatcherSettings, testThreadPool)) {
                Tuple<RealmConfig, SSLService> config = buildConfig(realmMetadataPath.toString());
                Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple = SamlRealm.initializeResolver(
                    logger,
                    config.v1(),
                    config.v2(),
                    watcherService
                );
                try {
                    assertIdp1MetadataParsedCorrectly(tuple.v2().get());
                    final IdpConfiguration idpConf = SamlRealm.getIdpConfiguration(realmConfig, tuple.v1(), tuple.v2());

                    // Trigger initialized log message
                    final List<PublicKey> keys1 = idpConf.getSigningCredentials().stream().map(Credential::getPublicKey).toList();

                    // Add metadata update listener
                    final CountDownLatch metadataUpdateLatch = new CountDownLatch(1);
                    FileReloadListener metadataUpdateListener = new FileReloadListener(realmMetadataPath, metadataUpdateLatch::countDown);
                    FileWatcher metadataUpdateWatcher = new FileWatcher(realmMetadataPath);
                    metadataUpdateWatcher.addListener(metadataUpdateListener);
                    watcherService.add(metadataUpdateWatcher, ResourceWatcherService.Frequency.MEDIUM);
                    // Put updated metadata contents into realm metadata file
                    Files.writeString(realmMetadataPath, Files.readString(updatedMetadataPath));
                    // Remove metadata update listener
                    metadataUpdateLatch.await();
                    metadataUpdateWatcher.remove(metadataUpdateListener);

                    assertThat(Files.readString(realmMetadataPath), is(equalTo(Files.readString(updatedMetadataPath))));
                    // Trigger changed log message
                    final List<PublicKey> keys2 = idpConf.getSigningCredentials().stream().map(Credential::getPublicKey).toList();
                    assertThat(keys1, is(equalTo(keys2)));

                    // Trigger not changed log message
                    assertThat(Files.readString(realmMetadataPath), is(equalTo(Files.readString(updatedMetadataPath))));
                    final List<PublicKey> keys3 = idpConf.getSigningCredentials().stream().map(Credential::getPublicKey).toList();
                    assertThat(keys1, is(equalTo(keys3)));
                } finally {
                    tuple.v1().destroy();
                }
            }
        } finally {
            testThreadPool.shutdown();
        }
    }

    private EntityDescriptor mockIdp() {
        final EntityDescriptor descriptor = mock(EntityDescriptor.class);
        when(descriptor.getEntityID()).thenReturn("https://idp.saml/");
        return descriptor;
    }

    private Tuple<RealmConfig, SSLService> buildConfig(String idpMetadataPath) {
        return buildConfig(idpMetadataPath, ignore -> {});
    }

    private Tuple<RealmConfig, SSLService> buildConfig(String idpMetadataPath, Consumer<Settings.Builder> additionalSettings) {
        var settings = buildSettings(idpMetadataPath);
        additionalSettings.accept(settings);
        final RealmConfig config = realmConfigFromGlobalSettings(settings.build());
        final SSLService sslService = new SSLService(config.env());
        return new Tuple<>(config, sslService);
    }

    private Settings.Builder buildSettings(String idpMetadataPath) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        final String realmType = SingleSpSamlRealmSettings.TYPE;
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".ssl.secure_key_passphrase", "testnode");
        return Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".ssl.verification_mode", "certificate")
            .put(
                REALM_SETTINGS_PREFIX + ".ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem")
            )
            .put(
                REALM_SETTINGS_PREFIX + ".ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put(
                REALM_SETTINGS_PREFIX + ".ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), idpMetadataPath)
            .put(SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID), TEST_IDP_ENTITY_ID)
            .put(
                SingleSpSamlRealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH),
                METADATA_REFRESH.getStringRep()
            )
            .put(RealmSettings.getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.apply(realmType).getAttribute()), "uid")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings);
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).put(realmSettings).build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("saml", REALM_NAME);
        return new RealmConfig(
            realmIdentifier,
            Settings.builder().put(settings).put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            env,
            threadContext
        );
    }

    private RealmConfig realmConfigFromGlobalSettings(Settings globalSettings) {
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("saml", REALM_NAME);
        return new RealmConfig(
            realmIdentifier,
            Settings.builder()
                .put(globalSettings)
                .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                .build(),
            env,
            new ThreadContext(globalSettings)
        );
    }

    private TestsSSLService buildTestSslService() {
        final MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put(
                "xpack.security.http.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .put(
                "xpack.security.http.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
            )
            .putList("xpack.security.http.ssl.supported_protocols", XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS)
            .put("path.home", createTempDir())
            .setSecureSettings(mockSecureSettings)
            .build();
        return new TestsSSLService(TestEnvironment.newEnvironment(settings));
    }

    private void assertIdp1MetadataParsedCorrectly(EntityDescriptor descriptor) {
        try {
            IDPSSODescriptor idpssoDescriptor = descriptor.getIDPSSODescriptor(SAMLConstants.SAML20P_NS);
            assertNotNull(idpssoDescriptor);
            List<SingleSignOnService> ssoServices = idpssoDescriptor.getSingleSignOnServices();
            assertEquals(2, ssoServices.size());
            assertEquals(SAMLConstants.SAML2_POST_BINDING_URI, ssoServices.get(0).getBinding());
            assertEquals(SAMLConstants.SAML2_REDIRECT_BINDING_URI, ssoServices.get(1).getBinding());
        } catch (ElasticsearchSecurityException e) {
            // Convert a SAML exception into an assertion failure so that we can `assertBusy` on these checks
            throw new AssertionError("Failed to retrieve IdP metadata", e);
        }
    }
}
