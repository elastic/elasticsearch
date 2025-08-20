/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

public class JwtMultiProjectIT extends ESRestTestCase {

    private static final String CLIENT_SECRET = "not-telling-you";
    private static final String ADMIN_PASSWORD = "hunter2";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.INTEG_TEST)
        .module("analysis-common")
        .setting("test.multi_project.enabled", "true")
        .configFile("jwkset.json", Resource.fromClasspath("jwk/jwkset.json"))
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.realms.file.admin_file.order", "0")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.order", "1")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.token_type", "id_token")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.allowed_issuer", "issuing-service")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.allowed_audiences", "elasticsearch")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.allowed_subjects", "tester")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.claims.principal", "sub")
        .setting("xpack.security.authc.realms.jwt.jwt_rsa.pkc_jwkset_path", "jwkset.json")
        .keystore("xpack.security.authc.realms.jwt.jwt_rsa.client_authentication.shared_secret", CLIENT_SECRET)
        .user("admin", ADMIN_PASSWORD)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString(ADMIN_PASSWORD.toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        return builder.build();
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    @After
    public void cleanup() throws IOException {
        cleanUpProjects();
    }

    @FixForMultiProject(description = "This should also test role mappings from file-based-settings (when they are project-scoped)")
    public void testSameJwtAuthenticatesToMultipleProjects() throws Exception {
        final String project1 = randomIdentifier();
        final String project2 = randomIdentifier();

        createProject(project1);
        createProject(project2);

        final JWTClaimsSet.Builder claims = buildJwtClaims();
        final JWSHeader jwtHeader = new JWSHeader.Builder(JWSAlgorithm.parse("RS256")).build();
        final SignedJWT jwt = signJwt(jwtHeader, claims.build());

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", "Bearer " + jwt.serialize())
            .addHeader("ES-Client-Authentication", "SharedSecret " + CLIENT_SECRET)
            .build();

        final Map<String, Object> authProject1 = authenticate(project1, requestOptions);
        assertThat(authProject1, Matchers.hasEntry("username", "tester"));

        final Map<String, Object> authProject2 = authenticate(project2, requestOptions);
        assertThat(authProject2, Matchers.hasEntry("username", "tester"));
    }

    private JWTClaimsSet.Builder buildJwtClaims() {
        final JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder();
        claims.subject("tester");
        claims.audience("elasticsearch");
        claims.issuer("issuing-service");
        claims.issueTime(Date.from(Instant.now().minus(30, ChronoUnit.SECONDS)));
        claims.expirationTime(Date.from(Instant.now().plus(90, ChronoUnit.MINUTES)));
        claims.claim("token_use", "id");
        return claims;
    }

    private SignedJWT signJwt(JWSHeader jwtHeader, JWTClaimsSet claims) throws IOException, GeneralSecurityException, JOSEException {
        final Path signingKey = getDataPath("/jwk/signer.key");
        final PrivateKey privateKey = PemUtils.readPrivateKey(signingKey, () -> null);
        var signer = new RSASSASigner(privateKey);
        final SignedJWT jwt = new SignedJWT(jwtHeader, claims);
        jwt.sign(signer);
        return jwt;
    }

    private Map<String, Object> authenticate(String projectId, RequestOptions requestOptions) throws IOException {
        final Request request = new Request("GET", "/_security/_authenticate");
        request.setOptions(requestOptions.toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId));
        return entityAsMap(client().performRequest(request));
    }
}
