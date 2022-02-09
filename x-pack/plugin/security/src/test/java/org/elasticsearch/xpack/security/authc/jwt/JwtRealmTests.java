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
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.auth.Secret;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
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
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.elasticsearch.xpack.security.support.CapturingActionListener;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.crypto.SecretKey;

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
    private SSLService sslService;
    private MockLicenseState licenseState;

    @Before
    public void init() throws Exception {
        final Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        this.threadPool = new TestThreadPool("JWT realm tests");
        this.resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, this.threadPool);
        this.sslService = new SSLService(TestEnvironment.newEnvironment(globalSettings));
        this.licenseState = mock(MockLicenseState.class);
        when(this.licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        this.resourceWatcherService.close();
        terminate(this.threadPool);
    }

    public void testClientAuthorizationTypeValidation() {
        final String authcName = "jwt" + randomIntBetween(0, 9);
        final RealmConfig authcConfig = super.buildRealmConfig(JwtRealmSettings.TYPE, authcName, Settings.EMPTY, 0);
        final SecureString validSecret = new SecureString(randomAlphaOfLength(32).toCharArray());
        final SecureString invalidSecretForTypeSharedSecret = randomBoolean() ? new SecureString("".toCharArray()) : null;
        final SecureString invalidSecretForTypeNone = validSecret;

        // If type is None, verify null is accepted
        JwtRealm.validateClientAuthorizationSettings(JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE, null, authcConfig);
        // If type is SharedSecret, verify non-null and non-empty are accepted
        JwtRealm.validateClientAuthorizationSettings(
            JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
            validSecret,
            authcConfig
        );
        // If type is SharedSecret, verify null or blank are rejected
        final Exception exception2 = expectThrows(
            SettingsException.class,
            "Exception expected for invalidSecretForTypeSharedSecret=[" + invalidSecretForTypeSharedSecret + "]",
            () -> JwtRealm.validateClientAuthorizationSettings(
                JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
                invalidSecretForTypeSharedSecret,
                authcConfig
            )
        );
        assertThat(
            exception2.getMessage(),
            is(
                equalTo(
                    "Missing setting for ["
                        + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                        + "]. It is required when setting ["
                        + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE)
                        + "] is ["
                        + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
                        + "]"
                )
            )
        );
        // If type is None, verify blank or non-blank are rejected
        final Exception exception3 = expectThrows(
            SettingsException.class,
            "Exception expected for invalidSecretForTypeSharedSecret=[" + invalidSecretForTypeNone + "]",
            () -> JwtRealm.validateClientAuthorizationSettings(
                JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE,
                invalidSecretForTypeNone,
                authcConfig
            )
        );
        assertThat(
            exception3.getMessage(),
            is(
                equalTo(
                    "Setting ["
                        + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                        + "] is not supported, because setting ["
                        + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE)
                        + "] is ["
                        + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE
                        + "]"
                )
            )
        );
    }

    public void testIssuerCredentialsValidation() throws Exception {
        final String authcName = "jwt" + randomIntBetween(0, 9);
        final RealmConfig authcConfig = super.buildRealmConfig(JwtRealmSettings.TYPE, authcName, Settings.EMPTY, 0);
        final SecureString invalidHmacKeySecureString = randomBoolean() ? null : new SecureString("".toCharArray());
        final String signatureAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS);
        final Secret validHmacKeySecret = JwtUtilTests.randomSecret(signatureAlgorithm);
        final SecureString validHmacKeySecureString = new SecureString(validHmacKeySecret.getValue().toCharArray());
        // final SecretKey validHmacKeySecretKey =
        // JwtUtil.generateSecretKey(randomFrom(JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS));
        // final SecureString validHmacKeySecureString =
        // new SecureString(Base64.getEncoder().encodeToString(validHmacKeySecretKey.getEncoded()).toCharArray());
        final String validJwtSetPathHttps = "https://op.example.com/jwkset.json";
        final String invalidJwkSetPathHttp = "http://invalid.example.com/jwkset.json";
        final String validJwkSetPathFile = Files.createTempFile(PathUtils.get(this.pathHome), "jwkset.", ".json").toString();
        Files.writeString(PathUtils.get(validJwkSetPathFile), "Non-empty JWK Set Path contents");
        final String validJwkSetPath = randomBoolean() ? validJwtSetPathHttps : validJwkSetPathFile;

        // If HTTPS URL or local file, verify it is accepted
        // If HTTP URL, verify it is rejected
        JwtRealm.validateJwkSetPathSetting(authcConfig, validJwtSetPathHttps);
        JwtRealm.validateJwkSetPathSetting(authcConfig, validJwkSetPathFile);
        final Exception exception0 = expectThrows(
            SettingsException.class,
            () -> JwtRealm.validateJwkSetPathSetting(authcConfig, invalidJwkSetPathHttp)
        );
        assertThat(
            exception0.getMessage(),
            equalTo(
                "Invalid value ["
                    + invalidJwkSetPathHttp
                    + "] for setting "
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.JWKSET_PATH)
                    + ""
            )
        );

        // If only valid HMAC Key Secure String present and only HMAC algorithms, verify it is accepted
        JwtRealm.validateIssuerCredentialSettings(
            authcConfig,
            validHmacKeySecureString,
            null,
            JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS
        );
        // If only valid JWT Set Path present and only Public Key algorithms, verify it is accepted
        JwtRealm.validateIssuerCredentialSettings(
            authcConfig,
            invalidHmacKeySecureString,
            validJwkSetPath,
            JwtRealmSettings.SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS
        );

        // If both valid credentials present and both algorithms present, verify they are accepted
        JwtRealm.validateIssuerCredentialSettings(
            authcConfig,
            validHmacKeySecureString,
            validJwkSetPath,
            JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS
        );

        // If only HMAC Key Secure String present but no HMAC algorithms, verify it is rejected
        final Exception exception1 = expectThrows(
            SettingsException.class,
            () -> JwtRealm.validateIssuerCredentialSettings(
                authcConfig,
                validHmacKeySecureString,
                null,
                JwtRealmSettings.SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS
            )
        );
        assertThat(
            exception1.getMessage(),
            equalTo(
                "Issuer HMAC Secret Key is configured in setting ["
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "], but no HMAC signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "]"
            )
        );
        // If only valid JWT Set Path present but no Public Key algorithms, verify it is rejected
        final Exception exception2 = expectThrows(
            SettingsException.class,
            () -> JwtRealm.validateIssuerCredentialSettings(
                authcConfig,
                null,
                validJwkSetPath,
                JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS
            )
        );
        assertThat(
            exception2.getMessage(),
            equalTo(
                "HMAC signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "], but no Issuer HMAC Secret Key is configured in setting ["
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            )
        );

        // If both credentials missing, verify they are rejected
        final Exception exception3 = expectThrows(
            SettingsException.class,
            () -> JwtRealm.validateIssuerCredentialSettings(authcConfig, null, "", JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS)
        );
        assertThat(
            exception3.getMessage(),
            equalTo(
                "At least one setting is required for ["
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "] or ["
                    + RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            )
        );
    }

    public void testJwtAuthcRealmAuthenticateWithEmptyRoles() throws Exception {
        final Tuple<Integer, Integer> authcRange = new Tuple<>(1, 1);
        final Tuple<Integer, Integer> authzRange = new Tuple<>(0, 0);
        final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 0);
        this.realmTestHelper(authcRange, authzRange, audiencesRange, rolesRange);
    }

    public void testJwtAuthcRealmAuthenticateWithoutAuthzRealms() throws Exception {
        final Tuple<Integer, Integer> authcRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> authzRange = new Tuple<>(0, 0);
        final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 3);
        this.realmTestHelper(authcRange, authzRange, audiencesRange, rolesRange);
    }

    public void testJwtAuthcRealmAuthenticateWithAuthzRealms() throws Exception {
        final Tuple<Integer, Integer> authcRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> authzRange = new Tuple<>(0, 3);
        final Tuple<Integer, Integer> audiencesRange = new Tuple<>(1, 3);
        final Tuple<Integer, Integer> rolesRange = new Tuple<>(0, 3);
        this.realmTestHelper(authcRange, authzRange, audiencesRange, rolesRange);
    }

    public void realmTestHelper(
        final Tuple<Integer, Integer> authcRange,
        final Tuple<Integer, Integer> authzRange,
        final Tuple<Integer, Integer> audiencesRange,
        final Tuple<Integer, Integer> rolesRange
    ) throws Exception {
        final int authcCount = randomIntBetween(authcRange.v1(), authcRange.v2());
        final int selectedAuthcIndex = randomIntBetween(0, authcCount - 1);
        LOGGER.info("authcCount=" + authcCount + ", selectedAuthcIndex=" + selectedAuthcIndex);

        // Details from a random selected JWT authc realm. These unique values will be used to create a JWT authentication token.
        Realm selectedJwtRealm = null;
        String selectedClientAuthorizationSharedSecret = null;
        String selectedSignatureAlgorithmSecretKey = null;
        String selectedSignatureAlgorithmKeyPair = null;
        SecretKey selectedSignatureSecretKey = null;
        KeyPair selectedSignatureKeyPair = null;
        String selectedIssuer = null;
        List<String> selectedAudiences = null;
        String selectedPrincipalClaim = null;
        String selectedPrincipal = null;
        String selectedGroupsClaim = null;
        boolean selectedPopulateUserMetadata = false;
        String[] selectedRoleNames = null;
        String selectedDnClaim = null;
        String selectedDn = null;
        String selectedFullNameClaim = null;
        String selectedFullName = null;
        String selectedEmailClaim = null;
        String selectedEmail = null;

        final List<Realm> allRealms = new ArrayList<>();

        // Create 0..N-1 JWT authc realms. Pick a random one to use for generating a JWT and optional client secret.
        for (final int authcIndex : IntStream.range(0, authcCount).toArray()) {
            // If authzCount == 0, authc realm will resolve the roles via UserRoleMapper
            // If authzCount >= 1, authz realm will resolve the roles via User lookup
            final int authzCount = randomIntBetween(authzRange.v1(), authzRange.v2());
            final int audienceCount = randomIntBetween(audiencesRange.v1(), audiencesRange.v2());
            final int rolesCount = randomIntBetween(rolesRange.v1(), rolesRange.v2());
            // Unique settings per JWT authc realm helps with debugging and tracing.
            final String authcName = "jwt" + authcIndex + randomIntBetween(0, 9);
            final String[] authzNames = IntStream.range(0, authzCount).mapToObj(i -> authcName + "_authz" + i).toArray(String[]::new);
            LOGGER.info("Creating JWT authc realm: authcName=" + authcName + ", authzNames=" + Arrays.toString(authzNames));
            final String issuer = authcName + "_iss";
            final List<String> allowedAudiences = IntStream.range(0, audienceCount)
                .mapToObj(i -> authcName + "_aud" + i)
                .collect(Collectors.toList());
            final String claimPrincipal = randomBoolean() ? "sub" : authcName + "_claimPrincipal";
            final String claimGroups = randomBoolean() ? null : authcName + "_claimGroups";
            final String claimDn = randomBoolean() ? null : authcName + "_claimDn";
            final String claimFullName = randomBoolean() ? null : authcName + "_claimFullName";
            final String claimEmail = randomBoolean() ? null : authcName + "_claimEmail";
            final String clientAuthorizationType = randomFrom(JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPES);
            final String clientAuthorizationSharedSecret = clientAuthorizationType.equals(
                JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
            ) ? Base64.getUrlEncoder().encodeToString(randomByteArrayOfLength(32)) : null;
            final boolean populateUserMetadata = randomBoolean();
            final String[] roleNames = IntStream.range(0, rolesCount).mapToObj(i -> authcName + "_role" + i).toArray(String[]::new);
            final String principal = authcName + "_principal";
            final String dn = authcName + "_dn";
            final String fullName = authcName + "_fullName";
            final String email = authcName + "_email@example.com";

            // pick signature algorithms (random size 1..N, and random order)
            final Collection<String> supportedSignatureAlgorithms = random(1, JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
            // separate SecretKey vs KeyPair (one list may be empty)
            final List<String> supportedSignatureAlgorithmsSecretKey = supportedSignatureAlgorithms.stream()
                .filter(JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS::contains)
                .collect(Collectors.toList());
            final List<String> supportedSignatureAlgorithmsPublicKey = supportedSignatureAlgorithms.stream()
                .filter(JwtRealmSettings.SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS::contains)
                .collect(Collectors.toList());
            // pick one each for SecretKey and KeyPair (one algorithm may be null)
            final String signatureAlgorithmSecretKey = supportedSignatureAlgorithmsSecretKey.isEmpty()
                ? null
                : randomFrom(supportedSignatureAlgorithmsSecretKey);
            final String signatureAlgorithmPublicKey = supportedSignatureAlgorithmsPublicKey.isEmpty()
                ? null
                : randomFrom(supportedSignatureAlgorithmsPublicKey);
            // generate one each for SecretKey and KeyPair (one may be null)
            final SecretKey signatureSecretKey = Strings.hasText(signatureAlgorithmSecretKey)
                ? JwtUtilTests.randomSecretKey(signatureAlgorithmSecretKey)
                : null;
            final KeyPair signatureKeyPair = Strings.hasText(signatureAlgorithmPublicKey)
                ? JwtUtilTests.randomKeyPair(signatureAlgorithmPublicKey)
                : null;

            final String jwkSetPath = supportedSignatureAlgorithmsPublicKey.isEmpty()
                ? null
                : Files.createTempFile(PathUtils.get(this.pathHome), "jwkset.", ".json").toString();
            if (Strings.hasText(jwkSetPath)) {
                Files.writeString(PathUtils.get(jwkSetPath), "Non-empty JWK Set Path contents");
            }

            // JWT authc realm basic settings
            final Settings.Builder authcSettings = Settings.builder()
                .put(RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_ISSUER), issuer)
                .put(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
                    String.join(",", supportedSignatureAlgorithms)
                )
                .put(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ALLOWED_CLOCK_SKEW),
                    randomBoolean() ? "-1" : randomBoolean() ? "0" : randomIntBetween(1, 5) + randomFrom("s", "m", "h")
                )
                .put(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.JWKSET_PATH),
                    Strings.hasText(jwkSetPath) ? jwkSetPath : ""
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
            if (signatureAlgorithmSecretKey != null) {
                secureSettings.setString(
                    RealmSettings.getFullSettingKey(authcName, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY),
                    Base64.getUrlEncoder().encodeToString(signatureSecretKey.getEncoded())
                );
            }
            secureSettings.setString(
                RealmSettings.getFullSettingKey(authcName, SSLConfigurationSettings.TRUSTSTORE_PASSWORD.realm(JwtRealmSettings.TYPE)),
                randomAlphaOfLengthBetween(10, 10)
            );
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
                allRealms.size()
            );
            final UserRoleMapper authcUserRoleMapper = super.buildRoleMapper(principal, Set.of(roleNames));
            final JwtRealm authcRealm = new JwtRealm(
                authcConfig,
                this.threadPool,
                this.sslService,
                authcUserRoleMapper,
                this.resourceWatcherService
            );
            allRealms.add(authcRealm);

            // Add 0..N other authz realms (if any).
            if (authzCount >= 1) {
                final int selectedAuthzIndex = randomIntBetween(0, authzCount - 1);
                for (final int authzIndex : IntStream.range(0, authzCount).toArray()) {
                    final RealmConfig authzConfig = this.buildRealmConfig("mock", authzNames[authzIndex], Settings.EMPTY, allRealms.size());
                    final MockLookupRealm authzRealm = new MockLookupRealm(authzConfig);
                    // For the current JWT authc realm, select only one authz realm to resolve the roles for the authc principal
                    if ((authcIndex == selectedAuthcIndex) && (authzIndex == selectedAuthzIndex)) {
                        final Map<String, Object> userMetadata = Collections.singletonMap("is_lookup", true);
                        authzRealm.registerUser(new User(principal, roleNames, fullName, email, userMetadata, true));
                    }
                    allRealms.add(authzRealm);
                }
            }

            LOGGER.info(
                "Created JWT authc realm:"
                    + " authcName="
                    + authcName
                    + ", authzNames="
                    + Arrays.toString(authzNames)
                    + ", issuer="
                    + issuer
                    + ", allowedAudiences="
                    + allowedAudiences
                    + ", claimPrincipal="
                    + claimPrincipal
                    + ", claimGroups="
                    + claimGroups
                    + ", clientAuthorizationType="
                    + clientAuthorizationType
                    + ", clientAuthorizationSharedSecret="
                    + clientAuthorizationSharedSecret
                    + ", populateUserMetadata="
                    + populateUserMetadata
                    + ", roleNames="
                    + Arrays.toString(roleNames)
                    + ", supportedSignatureAlgorithms="
                    + supportedSignatureAlgorithms
                    + ", supportedSignatureAlgorithmsSecretKey="
                    + supportedSignatureAlgorithmsSecretKey
                    + ", supportedSignatureAlgorithmsPublicKey="
                    + supportedSignatureAlgorithmsPublicKey
                    + ", signatureAlgorithmSecretKey="
                    + signatureAlgorithmSecretKey
                    + ", signatureAlgorithmPublicKey="
                    + signatureAlgorithmPublicKey
                    + ", signatureSecretKey="
                    + (signatureSecretKey == null ? "null" : signatureSecretKey.getAlgorithm())
                    + ", signatureKeyPair="
                    + (signatureKeyPair == null ? "null" : signatureKeyPair.getPublic().getAlgorithm())
                    + ", jwkSetPath="
                    + jwkSetPath
            );

            // select one of the generated JWT authc realm details to use for generating and testing a JWT authentication token
            if (authcIndex == selectedAuthcIndex) {
                selectedJwtRealm = authcRealm;
                selectedClientAuthorizationSharedSecret = clientAuthorizationSharedSecret;
                selectedSignatureAlgorithmSecretKey = signatureAlgorithmSecretKey;
                selectedSignatureAlgorithmKeyPair = signatureAlgorithmPublicKey;
                selectedSignatureSecretKey = signatureSecretKey;
                selectedSignatureKeyPair = signatureKeyPair;
                selectedIssuer = issuer;
                selectedAudiences = allowedAudiences;
                selectedPrincipalClaim = claimPrincipal;
                selectedPrincipal = principal;
                selectedGroupsClaim = claimGroups;
                selectedPopulateUserMetadata = populateUserMetadata;
                selectedRoleNames = roleNames;
                selectedDnClaim = claimDn;
                selectedDn = dn;
                selectedFullNameClaim = claimFullName;
                selectedFullName = fullName;
                selectedEmailClaim = claimEmail;
                selectedEmail = email;

                LOGGER.info(
                    "Selected JWT authc realm settings: "
                        + " authcName="
                        + authcName
                        + ", principal="
                        + principal
                        + ", fullName="
                        + fullName
                        + ", claimGroups="
                        + String.join("", roleNames)
                        + ", dn="
                        + dn
                        + ", fullName="
                        + fullName
                        + ", email="
                        + email
                );

                // Test "this.initialized==false" state test. It is sufficient to only test one realm.
                final Exception exception = expectThrows(IllegalStateException.class, () -> {
                    authcRealm.token(this.threadContext); // calling token() before initialize() throws an exception
                });
                assertThat(exception.getMessage(), equalTo("Realm has not been initialized"));
            }
        }
        assertThat(selectedJwtRealm, is(notNullValue())); // debug check: ensure a JWT authc realm was selected

        // initialize all authc and authz realms, only the JWT authc realms actually need to do anything
        for (final Realm realm : allRealms) {
            LOGGER.info("Initializing realm " + realm.name());
            realm.initialize(allRealms, this.licenseState); // JWT authc realms need to verify references to their authz realms
        }

        // Select algorithm and key from SecretKey and PublicKey. If one is null, pick the other. If both are non-null, pick a random one.
        final boolean useSecretKey;
        if (selectedSignatureAlgorithmKeyPair == null) {
            useSecretKey = true;
        } else if (selectedSignatureAlgorithmSecretKey == null) {
            useSecretKey = false;
        } else {
            useSecretKey = randomBoolean();
        }
        final String signatureAlgorithm = useSecretKey ? selectedSignatureAlgorithmSecretKey : selectedSignatureAlgorithmKeyPair;
        final Object signatureSecretKeyOrPublicKey = useSecretKey ? selectedSignatureSecretKey : selectedSignatureKeyPair;

        // use selected algorithm and key to sign a JWT
        final Tuple<JWSSigner, JWSVerifier> jwsSignerAndVerifier = JwtUtil.createJwsSignerJwsVerifier(signatureSecretKeyOrPublicKey);
        final Tuple<JWSHeader, JWTClaimsSet> jwsHeaderAndJwtClaimsSet = JwtUtilTests.randomValidJwsHeaderAndJwtClaimsSet(
            signatureAlgorithm,
            selectedIssuer,
            selectedAudiences,
            selectedPrincipalClaim,
            selectedPrincipal,
            selectedGroupsClaim,
            List.of(selectedRoleNames),
            selectedDnClaim,
            selectedDn,
            selectedFullNameClaim,
            selectedFullName,
            selectedEmailClaim,
            selectedEmail
        );
        LOGGER.info("Using issuer [" + signatureSecretKeyOrPublicKey.getClass() + "]");
        final SignedJWT signedJWT = JwtUtil.signSignedJwt(
            jwsSignerAndVerifier.v1(),
            jwsHeaderAndJwtClaimsSet.v1(),
            jwsHeaderAndJwtClaimsSet.v2()
        );
        // verify the signature of the signed JWT
        assertThat(JwtUtil.verifySignedJWT(jwsSignerAndVerifier.v2(), signedJWT), is(equalTo(true)));

        // signed JWT corresponds to a SecretKey or PrivateKey in one-and-only-one of the JWT authc realms
        LOGGER.info("Using JWT [" + signedJWT.serialize() + "] and client secret [" + selectedClientAuthorizationSharedSecret + "]");
        super.threadContext.putHeader(
            JwtRealmSettings.HEADER_END_USER_AUTHORIZATION,
            JwtRealmSettings.HEADER_END_USER_AUTHORIZATION_SCHEME + " " + signedJWT.serialize()
        );
        if (selectedClientAuthorizationSharedSecret != null) {
            super.threadContext.putHeader(
                JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION,
                JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET + " " + selectedClientAuthorizationSharedSecret
            );
        }

        // Loop through all realms to see if any JWT authc realms can recognize and parse the 1-2 request headers
        AuthenticationToken authenticationToken = null;
        for (final Realm realm : allRealms) {
            authenticationToken = realm.token(super.threadContext);
            if (authenticationToken != null) {
                break;
            }
        }
        assertThat(authenticationToken, is(notNullValue()));
        assertThat(authenticationToken, isA(JwtAuthenticationToken.class));
        LOGGER.info("Realm chain returned a JWT authentication token [" + authenticationToken + "]");

        // Loop through all realms to see if any JWT authc realm can authenticate, and resolve roles via role-mapping or authz delegation
        User authenticatedUser = null;
        for (final Realm realm : allRealms) {
            final CapturingActionListener<AuthenticationResult<User>> capturingActionListener = new CapturingActionListener<>();
            realm.authenticate(authenticationToken, capturingActionListener);

            final Exception capturedException = capturingActionListener.getFailure();
            final AuthenticationResult<User> capturedAuthenticationResult = capturingActionListener.getResponse();
            if (capturedException != null) {
                LOGGER.info("Exception: ", capturedException);
                assertThat(capturedAuthenticationResult, is(nullValue()));
            } else if (capturedAuthenticationResult != null) {
                LOGGER.info(
                    "AuthenticationResult status="
                        + capturedAuthenticationResult.getStatus()
                        + ", authenticated="
                        + capturedAuthenticationResult.isAuthenticated()
                        + ", message="
                        + capturedAuthenticationResult.getMessage()
                        + ", metadata="
                        + capturedAuthenticationResult.getMetadata()
                        + ", user="
                        + capturedAuthenticationResult.getValue()
                );
                switch (capturedAuthenticationResult.getStatus()) {
                    case SUCCESS:
                        assertThat(capturedAuthenticationResult.isAuthenticated(), is(equalTo(true)));
                        assertThat(capturedAuthenticationResult.getException(), is(nullValue()));
                        assertThat(capturedAuthenticationResult.getStatus(), is(equalTo(AuthenticationResult.Status.SUCCESS)));
                        assertThat(capturedAuthenticationResult.getMessage(), is(nullValue()));
                        assertThat(capturedAuthenticationResult.getMetadata(), is(anEmptyMap()));
                        authenticatedUser = capturedAuthenticationResult.getValue();
                        assertThat(authenticatedUser, is(notNullValue()));
                        break;
                    case CONTINUE:
                        continue;
                    case TERMINATE:
                    default:
                        fail("Unexpected AuthenticationResult.Status=" + capturedAuthenticationResult.getStatus());
                        break;
                }
                if (authenticatedUser != null) {
                    break; // Break out of the loop
                }
            } else {
                fail("Exception and AuthenticationResult are null. Expected one of them to be non-null.");
            }
        }
        assertThat("AuthenticatedUser is null. Expected a realm to authenticate.", authenticatedUser, is(notNullValue()));
        assertThat(authenticatedUser.principal(), equalTo(selectedPrincipal));
        assertThat(new TreeSet<>(Arrays.asList(authenticatedUser.roles())), equalTo(new TreeSet<>(Arrays.asList(selectedRoleNames))));
        if (selectedPopulateUserMetadata) {
            assertThat(authenticatedUser.metadata(), is(not(anEmptyMap())));
        }
        LOGGER.info("Test succeeded");
    }
}
