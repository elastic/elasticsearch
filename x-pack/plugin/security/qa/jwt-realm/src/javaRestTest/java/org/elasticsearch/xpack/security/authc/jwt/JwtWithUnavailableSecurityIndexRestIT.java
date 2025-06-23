/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;

public class JwtWithUnavailableSecurityIndexRestIT extends ESRestTestCase {

    // Using this to first test without, then with caching. Since caching is controlled by a static setting, we need a
    // MutableSettingsProvider instance
    private static final MutableSettingsProvider mutableSettingsForLastLoadCache = new MutableSettingsProvider() {
        {
            put("xpack.security.authz.store.role_mappings.last_load_cache.enabled", "false");
        }
    };

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "true")
        .setting("xpack.security.http.ssl.certificate", "http.crt")
        .setting("xpack.security.http.ssl.key", "http.key")
        .setting("xpack.security.http.ssl.key_passphrase", "http-password")
        .setting("xpack.security.http.ssl.certificate_authorities", "ca.crt")
        .setting("xpack.security.http.ssl.client_authentication", "optional")
        .setting("xpack.security.authc.realms.jwt.jwt1.order", "1")
        .setting("xpack.security.authc.realms.jwt.jwt1.allowed_issuer", "https://issuer.example.com/")
        .setting("xpack.security.authc.realms.jwt.jwt1.allowed_audiences", "https://audience.example.com/")
        .setting("xpack.security.authc.realms.jwt.jwt1.claims.principal", "sub")
        .setting("xpack.security.authc.realms.jwt.jwt1.claims.dn", "dn")
        .setting("xpack.security.authc.realms.jwt.jwt1.required_claims.token_use", "id")
        .setting("xpack.security.authc.realms.jwt.jwt1.required_claims.version", "2.0")
        .setting("xpack.security.authc.realms.jwt.jwt1.client_authentication.type", "NONE")
        .setting("xpack.security.authc.realms.jwt.jwt1.pkc_jwkset_path", "rsa.jwkset")
        .settings(mutableSettingsForLastLoadCache)
        .configFile("http.key", Resource.fromClasspath("ssl/http.key"))
        .configFile("http.crt", Resource.fromClasspath("ssl/http.crt"))
        .configFile("ca.crt", Resource.fromClasspath("ssl/ca.crt"))
        .configFile("rsa.jwkset", Resource.fromClasspath("jwk/rsa-public-jwkset.json"))
        .user("admin_user", "admin-password")
        .build();

    private static Path httpCertificateAuthority;
    private TestSecurityClient adminSecurityClient;

    @BeforeClass
    public static void findTrustStore() throws Exception {
        httpCertificateAuthority = findResource("/ssl/ca.crt");
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static Path findResource(String name) throws FileNotFoundException, URISyntaxException {
        final URL resource = JwtWithUnavailableSecurityIndexRestIT.class.getResource(name);
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource " + name);
        }
        return PathUtils.get(resource.toURI());
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getProtocol() {
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

    public void testRoleMappingWithoutCacheFailsWithoutAccessToSecurityIndex() throws Exception {
        final String dn = randomDn();

        final String rules = Strings.format("""
            { "all": [
                { "field": { "realm.name": "jwt1" } },
                { "field": { "dn": "%s" } }
            ] }
            """, dn);

        final List<String> roles = randomRoles();
        final String roleMappingName = createRoleMapping(roles, rules);
        final String principal = randomPrincipal();

        try {
            {
                final SignedJWT jwt = buildAndSignJwt(principal, dn, Instant.now());

                final Map<String, Object> response = getSecurityClient(jwt).authenticate();

                assertAuthenticationHasUsernameAndRoles(response, principal, roles);
            }

            makeSecurityIndexUnavailable();

            {
                final SignedJWT jwt = buildAndSignJwt(principal, dn, Instant.now());

                final Map<String, Object> response = getSecurityClient(jwt).authenticate();

                assertAuthenticationHasUsernameAndRoles(response, principal, List.of());
            }

            // Now enable caching (since the setting is not dynamic, this requires a cluster restart), and test caching
            makeSecurityIndexAvailable();
            mutableSettingsForLastLoadCache.put("xpack.security.authz.store.role_mappings.last_load_cache.enabled", "true");
            restartClusterAndResetClients();

            {
                final SignedJWT jwt = buildAndSignJwt(principal, dn, Instant.now());

                final Map<String, Object> response = getSecurityClient(jwt).authenticate();

                assertAuthenticationHasUsernameAndRoles(response, principal, roles);
            }

            makeSecurityIndexUnavailable();

            {
                final SignedJWT jwt = buildAndSignJwt(principal, dn, Instant.now());

                final Map<String, Object> response = getSecurityClient(jwt).authenticate();

                assertAuthenticationHasUsernameAndRoles(response, principal, roles);
            }
        } finally {
            makeSecurityIndexAvailable();
            deleteRoleMapping(roleMappingName);
        }
    }

    private void restartClusterAndResetClients() throws IOException {
        cluster.restart(false);
        adminSecurityClient = null;
        closeClients();
        initClient();
    }

    private void assertAuthenticationHasUsernameAndRoles(
        Map<String, Object> response,
        String expectedUsername,
        List<String> expectedRoles
    ) {
        final String description = "Authentication response [" + response + "]";
        assertThat(description, response, hasEntry(User.Fields.USERNAME.getPreferredName(), expectedUsername));
        assertThat(
            description,
            JwtRestIT.assertList(response, User.Fields.ROLES),
            Matchers.containsInAnyOrder(expectedRoles.toArray(String[]::new))
        );
    }

    private void makeSecurityIndexUnavailable() throws IOException {
        Request closeRequest = new Request("POST", "/.security/_close");
        closeRequest.setOptions(systemIndexWarningHandlerOptions(".security-7"));
        assertOK(adminClient().performRequest(closeRequest));
    }

    private void makeSecurityIndexAvailable() throws IOException {
        Request openRequest = new Request("POST", "/.security/_open");
        openRequest.setOptions(systemIndexWarningHandlerOptions(".security-7"));
        assertOK(adminClient().performRequest(openRequest));
    }

    private RequestOptions.Builder systemIndexWarningHandlerOptions(String index) {
        return RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(
                w -> w.size() > 0
                    && w.contains(
                        "this request accesses system indices: ["
                            + index
                            + "], but in a future major "
                            + "version, direct access to system indices will be prevented by default"
                    ) == false
            );
    }

    private String randomPrincipal() {
        // We append _test so that it cannot randomly conflict with builtin user
        return randomAlphaOfLengthBetween(4, 12) + "_test";
    }

    private String randomDn() {
        return "CN=" + randomPrincipal();
    }

    private List<String> randomRoles() {
        // We append _test so that it cannot randomly conflict with builtin roles
        return randomList(1, 3, () -> randomAlphaOfLengthBetween(4, 12) + "_test");
    }

    private SignedJWT buildAndSignJwt(String principal, String dn, Instant issueTime) throws JOSEException, ParseException, IOException {
        final JWTClaimsSet claimsSet = JwtRestIT.buildJwt(
            Map.ofEntries(
                Map.entry("iss", "https://issuer.example.com/"),
                Map.entry("aud", "https://audience.example.com/"),
                Map.entry("sub", principal),
                Map.entry("dn", dn),
                Map.entry("token_use", "id"),
                Map.entry("version", "2.0")
            ),
            issueTime
        );
        final RSASSASigner signer = loadRsaSigner();
        return JwtRestIT.signJWT(signer, "RS256", claimsSet, false);
    }

    private RSASSASigner loadRsaSigner() throws IOException, ParseException, JOSEException {
        try (var in = getDataInputStream("/jwk/rsa-private-jwkset.json")) {
            final JWKSet jwkSet = JWKSet.load(in);
            final JWK key = jwkSet.getKeyByKeyId("test-rsa-key");
            assertThat(key, instanceOf(RSAKey.class));
            return new RSASSASigner((RSAKey) key);
        }
    }

    private TestSecurityClient getSecurityClient(SignedJWT jwt) {
        final String bearerHeader = "Bearer " + jwt.serialize();
        final RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("Authorization", bearerHeader);
        return new TestSecurityClient(client(), options.build());
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
