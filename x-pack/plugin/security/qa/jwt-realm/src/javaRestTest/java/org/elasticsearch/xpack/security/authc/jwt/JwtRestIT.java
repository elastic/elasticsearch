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
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.test.TestMatchers.hasStatusCode;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class JwtRestIT extends ESRestTestCase {

    private static final Optional<String> VALID_SHARED_SECRET = Optional.of("test-secret");
    private static Path httpCertificateAuthority;
    private TestSecurityClient adminSecurityClient;

    @BeforeClass
    public static void findTrustStore() throws Exception {
        final URL resource = JwtRestIT.class.getResource("/ssl/ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.crt");
        }
        httpCertificateAuthority = PathUtils.get(resource.toURI());
    }

    @Override
    protected String getProtocol() {
        // Because this QA project uses https
        return "https";
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).put(restSslSettings()).build();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings()).put(restSslSettings()).build();
    }

    private Settings restSslSettings() {
        return Settings.builder().put(CERTIFICATE_AUTHORITIES, httpCertificateAuthority).build();
    }

    protected TestSecurityClient getAdminSecurityClient() {
        if (adminSecurityClient == null) {
            adminSecurityClient = new TestSecurityClient(adminClient());
        }
        return adminSecurityClient;
    }

    /**
     * Tests against realm "jwt1" in build.gradle
     * This realm
     * - use the "sub" claim as the principal
     * - performs role mapping
     * - supports RSA signed keys
     * - has no client authentication
     */
    public void testAuthenticateWithRsaSignedJWTAndRoleMapping() throws Exception {
        final String principal = randomPrincipal();
        final List<String> roles = randomRoles();
        final String roleMapping = createRoleMapping(roles, """
            { "all": [
                { "field": { "realm.name": "jwt1" } },
                { "field": { "username": "%s" } }
            ] }
            """.formatted(principal));

        try {
            final SignedJWT jwt = buildAndSignJwtForRealm1(principal);
            final TestSecurityClient client = getSecurityClient(jwt, Optional.empty());

            final Map<String, Object> response = client.authenticate();

            assertThat(response, hasEntry(User.Fields.USERNAME.getPreferredName(), principal));
            assertThat(assertMap(response, User.Fields.AUTHENTICATION_REALM), hasEntry(User.Fields.REALM_NAME.getPreferredName(), "jwt1"));
            assertThat(assertList(response, User.Fields.ROLES), Matchers.containsInAnyOrder(roles.toArray(String[]::new)));

            // The user has no real role (we never define them) so everything they try to do will be FORBIDDEN
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> client.getRoleDescriptors(new String[] { "*" })
            );
            assertThat(exception.getResponse(), hasStatusCode(RestStatus.FORBIDDEN));
        } finally {
            deleteRoleMapping(roleMapping);
        }
    }

    public void testFailureOnExpiredJwt() throws Exception {
        final String principal = randomPrincipal();
        { // Test with valid time
            final SignedJWT jwt = buildAndSignJwtForRealm1(principal, Instant.now());
            final TestSecurityClient client = getSecurityClient(jwt, Optional.empty());
            assertThat(client.authenticate(), hasEntry(User.Fields.USERNAME.getPreferredName(), principal));
        }

        { // Test with expired time
            final SignedJWT jwt = buildAndSignJwtForRealm1(principal, Instant.now().minus(12, ChronoUnit.HOURS));
            TestSecurityClient client = getSecurityClient(jwt, Optional.empty());

            // This fails because the JWT is expired
            final ResponseException exception = expectThrows(ResponseException.class, client::authenticate);
            assertThat(exception.getResponse(), hasStatusCode(RestStatus.UNAUTHORIZED));
        }
    }

    public void testFailureOnInvalidRsaSignature() throws Exception {
        final String originalPrincipal = randomPrincipal();
        final SignedJWT originalJwt = buildAndSignJwtForRealm1(originalPrincipal, Instant.now());
        {
            // Test with valid signed JWT
            final TestSecurityClient client = getSecurityClient(originalJwt, Optional.empty());
            assertThat(client.authenticate(), hasEntry(User.Fields.USERNAME.getPreferredName(), originalPrincipal));
        }

        final String alternatePrincipal = "not-" + originalPrincipal;
        final SignedJWT alternateJwt = buildAndSignJwtForRealm1(alternatePrincipal, Instant.now());
        {
            // Test with an alternate, valid signed JWT
            final TestSecurityClient client = getSecurityClient(alternateJwt, Optional.empty());
            assertThat(client.authenticate(), hasEntry(User.Fields.USERNAME.getPreferredName(), alternatePrincipal));
        }

        // Test that we get a failure if we combine the payload from JWT1 with the signature from JWT2
        {
            assertThat(
                getSecurityClient(alternateJwt, Optional.empty()).authenticate(),
                hasEntry(User.Fields.USERNAME.getPreferredName(), alternatePrincipal)
            );

            final Base64URL[] originalParts = SignedJWT.split(originalJwt.serialize());
            assertThat(originalParts, arrayWithSize(3));

            final Base64URL[] alternateParts = SignedJWT.split(alternateJwt.serialize());
            assertThat(alternateParts, arrayWithSize(3));

            assertThat("JWT Signatures should be different", originalParts[2], not(equalTo(alternateParts[2])));

            // Create a new JWT with the header + payload from the original JWT, but a signature from the alternate JWT
            final SignedJWT falsifiedJwt = new SignedJWT(originalParts[0], originalParts[1], alternateParts[2]);
            final TestSecurityClient client = getSecurityClient(falsifiedJwt, Optional.empty());

            // This fails because the JWT is not correctly signed
            final ResponseException exception = expectThrows(ResponseException.class, client::authenticate);
            assertThat(exception.getResponse(), hasStatusCode(RestStatus.UNAUTHORIZED));
        }
    }

    /**
     * Tests against realm "jwt2" in build.gradle
     * This realm
     * - use the "email" claim as the principal (with the domain removed)
     * - performs lookup on the native realm
     * - supports HMAC signed keys
     * - uses a shared-secret for client authentication
     */
    public void testAuthenticateWithHmacSignedJWTAndDelegatedAuthorization() throws Exception {
        final String principal = randomPrincipal();
        final List<String> roles = randomRoles();
        final String randomMetadata = randomAlphaOfLengthBetween(6, 18);
        createUser(principal, roles, Map.of("test_key", randomMetadata));

        try {
            final SignedJWT jwt = buildAndSignJwtForRealm2(principal);
            final TestSecurityClient client = getSecurityClient(jwt, VALID_SHARED_SECRET);

            final Map<String, Object> response = client.authenticate();

            assertThat(response.get(User.Fields.USERNAME.getPreferredName()), is(principal));
            assertThat(assertMap(response, User.Fields.AUTHENTICATION_REALM), hasEntry(User.Fields.REALM_NAME.getPreferredName(), "jwt2"));
            assertThat(assertList(response, User.Fields.ROLES), Matchers.containsInAnyOrder(roles.toArray(String[]::new)));
            assertThat(assertMap(response, User.Fields.METADATA), hasEntry("test_key", randomMetadata));

            // The user has no real role (we never define them) so everything they try to do will be FORBIDDEN
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> client.getRoleDescriptors(new String[] { "*" })
            );
            assertThat(exception.getResponse(), hasStatusCode(RestStatus.FORBIDDEN));
        } finally {
            deleteUser(principal);
        }
    }

    public void testFailureOnInvalidHMACSignature() throws Exception {
        final String principal = randomPrincipal();
        final List<String> roles = randomRoles();
        try {
            createUser(principal, roles, Map.of());

            final JWTClaimsSet claimsSet = buildJwtForRealm2(principal, Instant.now());

            {
                // This is the correct HMAC passphrase (from build.gradle)
                final SignedJWT jwt = signHmacJwt(claimsSet, "test-HMAC/secret passphrase-value");
                final TestSecurityClient client = getSecurityClient(jwt, VALID_SHARED_SECRET);
                assertThat(client.authenticate(), hasEntry(User.Fields.USERNAME.getPreferredName(), principal));
            }
            {
                // This is not the correct HMAC passphrase
                final SignedJWT invalidJwt = signHmacJwt(claimsSet, "invalid-HMAC-passphrase-" + randomAlphaOfLength(12));
                final TestSecurityClient client = getSecurityClient(invalidJwt, VALID_SHARED_SECRET);
                // This fails because the HMAC is wrong
                final ResponseException exception = expectThrows(ResponseException.class, client::authenticate);
                assertThat(exception.getResponse(), hasStatusCode(RestStatus.UNAUTHORIZED));
            }
        } finally {
            deleteUser(principal);
        }

    }

    public void testAuthenticationFailureIfDelegatedAuthorizationFails() throws Exception {
        final String principal = randomPrincipal();
        final SignedJWT jwt = buildAndSignJwtForRealm2(principal);
        final TestSecurityClient client = getSecurityClient(jwt, VALID_SHARED_SECRET);

        // This fails because we didn't create a native user
        final ResponseException exception = expectThrows(ResponseException.class, client::authenticate);
        assertThat(exception.getResponse(), hasStatusCode(RestStatus.UNAUTHORIZED));

        createUser(principal, List.of(), Map.of());
        try {
            // Now it works
            assertThat(client.authenticate(), hasEntry(User.Fields.USERNAME.getPreferredName(), principal));
        } finally {
            deleteUser(principal);
        }
    }

    public void testFailureOnInvalidClientAuthentication() throws Exception {
        final String principal = randomPrincipal();
        final List<String> roles = randomRoles();
        createUser(principal, roles, Map.of());

        try {
            final SignedJWT jwt = buildAndSignJwtForRealm2(principal);
            final TestSecurityClient client = getSecurityClient(jwt, Optional.of("not-the-correct-secret"));

            // This fails because we didn't use the correct shared-secret
            final ResponseException exception = expectThrows(ResponseException.class, client::authenticate);
            assertThat(exception.getResponse(), hasStatusCode(RestStatus.UNAUTHORIZED));

        } finally {
            deleteUser(principal);
        }
    }

    private String randomPrincipal() {
        // We append _test so that it cannot randomly conflict with builtin user
        return randomAlphaOfLengthBetween(4, 12) + "_test";
    }

    private List<String> randomRoles() {
        // We append _test so that it cannot randomly conflict with builtin roles
        return randomList(1, 3, () -> randomAlphaOfLengthBetween(4, 12) + "_test");
    }

    private SignedJWT buildAndSignJwtForRealm1(String principal) throws JOSEException, ParseException, IOException {
        return buildAndSignJwtForRealm1(principal, Instant.now());
    }

    private SignedJWT buildAndSignJwtForRealm1(String principal, Instant issueTime) throws JOSEException, ParseException, IOException {
        final JWTClaimsSet claimsSet = buildJwt(
            Map.ofEntries(
                Map.entry("iss", "https://issuer.example.com/"),
                Map.entry("aud", "https://audience.example.com/"),
                Map.entry("sub", principal)
            ),
            issueTime
        );
        return signRsaJwt(claimsSet);
    }

    private SignedJWT buildAndSignJwtForRealm2(String principal) throws JOSEException, ParseException {
        return buildAndSignJwtForRealm2(principal, Instant.now());
    }

    private SignedJWT buildAndSignJwtForRealm2(String principal, Instant issueTime) throws JOSEException, ParseException {
        final JWTClaimsSet claimsSet = buildJwtForRealm2(principal, issueTime);
        return signHmacJwt(claimsSet);
    }

    private JWTClaimsSet buildJwtForRealm2(String principal, Instant issueTime) {
        final String emailAddress = principal + "@" + randomAlphaOfLengthBetween(3, 6) + ".example.com";
        // The "jwt2" realm, supports 3 audiences (es01/02/03)
        final String audience = "es0" + randomIntBetween(1, 3);
        final JWTClaimsSet claimsSet = buildJwt(
            Map.ofEntries(Map.entry("iss", "my-issuer"), Map.entry("aud", audience), Map.entry("email", emailAddress)),
            issueTime
        );
        return claimsSet;
    }

    private SignedJWT signRsaJwt(JWTClaimsSet claimsSet) throws IOException, JOSEException, ParseException {
        final RSASSASigner signer = loadRsaSigner();
        return signJWt(signer, "RS256", claimsSet);
    }

    private RSASSASigner loadRsaSigner() throws IOException, ParseException, JOSEException {
        // The "jwt1" realm is configured using public JWKSet (in build.gradle)
        try (var in = getDataInputStream("/jwk/rsa-private-jwkset.json")) {
            final JWKSet jwkSet = JWKSet.load(in);
            final JWK key = jwkSet.getKeyByKeyId("test-rsa-key");
            assertThat(key, instanceOf(RSAKey.class));
            return new RSASSASigner((RSAKey) key);
        }
    }

    private SignedJWT signHmacJwt(JWTClaimsSet claimsSet) throws JOSEException, ParseException {
        // Input string is configured in build.gradle
        return signHmacJwt(claimsSet, "test-HMAC/secret passphrase-value");
    }

    private SignedJWT signHmacJwt(JWTClaimsSet claimsSet, String hmacPassphrase) throws JOSEException, ParseException {
        final OctetSequenceKey hmac = JwkValidateUtil.buildHmacKeyFromString(hmacPassphrase);
        final JWSSigner signer = new MACSigner(hmac);
        return signJWt(signer, "HS256", claimsSet);
    }

    // JWT construction
    private JWTClaimsSet buildJwt(Map<String, Object> claims, Instant issueTime) {
        final JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder();
        builder.issuer(randomAlphaOfLengthBetween(4, 24));
        builder.subject(randomAlphaOfLengthBetween(4, 24));
        builder.audience(randomList(1, 6, () -> randomAlphaOfLengthBetween(4, 12)));
        if (randomBoolean()) {
            builder.jwtID(UUIDs.randomBase64UUID(random()));
        }

        issueTime = issueTime.truncatedTo(ChronoUnit.SECONDS);
        builder.issueTime(Date.from(issueTime));
        builder.expirationTime(Date.from(issueTime.plusSeconds(randomLongBetween(180, 1800))));
        if (randomBoolean()) {
            builder.notBeforeTime(Date.from(issueTime.minusSeconds(randomLongBetween(10, 90))));
        }

        // This may overwrite the aud/sub/iss set above. That is an intended behaviour
        for (String key : claims.keySet()) {
            builder.claim(key, claims.get(key));
        }
        return builder.build();
    }

    private SignedJWT signJWt(JWSSigner signer, String algorithm, JWTClaimsSet claimsSet) throws JOSEException, ParseException {
        final JWSHeader jwtHeader = new JWSHeader.Builder(JWSAlgorithm.parse(algorithm)).build();
        final SignedJWT unsignedJwt = new SignedJWT(jwtHeader, claimsSet);
        return JwtValidateUtil.signJwt(signer, unsignedJwt);
    }

    private TestSecurityClient getSecurityClient(SignedJWT jwt, Optional<String> sharedSecret) {
        final String bearerHeader = "Bearer " + jwt.serialize();

        final RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", bearerHeader);
        sharedSecret.ifPresent(secret -> options.addHeader("X-Client-Authentication", "SharedSecret " + secret));

        return new TestSecurityClient(client(), options.build());
    }

    // Utility methods
    private Map<?, ?> assertMap(Map<String, ?> response, ParseField field) {
        assertThat(response, hasKey(field.getPreferredName()));
        assertThat(response, hasEntry(is(field.getPreferredName()), instanceOf(Map.class)));
        return (Map<?, ?>) response.get(field.getPreferredName());
    }

    private List<?> assertList(Map<String, ?> response, ParseField field) {
        assertThat(response, hasKey(field.getPreferredName()));
        assertThat(response, hasEntry(is(field.getPreferredName()), instanceOf(List.class)));
        return (List<?>) response.get(field.getPreferredName());
    }

    private void createUser(String principal, List<String> roles, Map<String, Object> metadata) throws IOException {
        createUser(principal, new SecureString(randomAlphaOfLength(12).toCharArray()), roles, metadata);
    }

    private void createUser(String username, SecureString password, List<String> roles, Map<String, Object> metadata) throws IOException {
        final String realName = randomAlphaOfLengthBetween(6, 18);
        final User user = new User(username, roles.toArray(String[]::new), realName, null, metadata, true);
        getAdminSecurityClient().putUser(user, password);
    }

    private void deleteUser(String username) throws IOException {
        getAdminSecurityClient().deleteUser(username);
    }

    private String createRoleMapping(List<String> roles, String rules) throws IOException {
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("enabled", true);
        mapping.put("roles", roles);
        mapping.put("rules", XContentHelper.convertToMap(XContentType.JSON.xContent(), rules, true));
        final String mappingName = "test-" + getTestName() + "-" + randomAlphaOfLength(8);
        getAdminSecurityClient().putRoleMapping(mappingName, mapping);
        return mappingName;
    }

    private void deleteRoleMapping(String name) throws IOException {
        getAdminSecurityClient().deleteRoleMapping(name);
    }

}
