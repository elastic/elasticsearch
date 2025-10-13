/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.Realms;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE;
import static org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.CLIENT_AUTH_SHARED_SECRET_ROTATION_GRACE_PERIOD;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class JwtRealmSingleNodeTests extends SecuritySingleNodeTestCase {

    private final String jwt0SharedSecret = "jwt0_shared_secret";
    private final String jwt1SharedSecret = "jwt1_shared_secret";
    private final String jwt2SharedSecret = "jwt2_shared_secret";
    private final String jwtHmacKey = "test-HMAC/secret passphrase-value";

    @Override
    protected Settings nodeSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings())
            // for testing invalid bearer JWTs
            .put("xpack.security.authc.anonymous.roles", "anonymous")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), randomBoolean())
            // 1st JWT realm
            .put("xpack.security.authc.realms.jwt.jwt0.order", 10)
            .put(
                randomBoolean()
                    ? Settings.builder().put("xpack.security.authc.realms.jwt.jwt0.token_type", "id_token").build()
                    : Settings.EMPTY
            )
            .put("xpack.security.authc.realms.jwt.jwt0.allowed_issuer", "my-issuer-01")
            .put("xpack.security.authc.realms.jwt.jwt0.allowed_audiences", "es-01")
            .put("xpack.security.authc.realms.jwt.jwt0.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt0.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt0.client_authentication.type", "shared_secret")
            .putList("xpack.security.authc.realms.jwt.jwt0.allowed_signature_algorithms", "HS256", "HS384")
            // 2nd JWT realm
            .put("xpack.security.authc.realms.jwt.jwt1.order", 20)
            .put("xpack.security.authc.realms.jwt.jwt1.token_type", "access_token")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_issuer", "my-issuer-02")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_subjects", "user-02")
            .put("xpack.security.authc.realms.jwt.jwt1.allowed_audiences", "es-02")
            .put("xpack.security.authc.realms.jwt.jwt1.fallback_claims.sub", "client_id")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.principal", "appid")
            .put("xpack.security.authc.realms.jwt.jwt1.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt1.client_authentication.type", "shared_secret")
            .put("xpack.security.authc.realms.jwt.jwt1.client_authentication.rotation_grace_period", "10m")
            .putList("xpack.security.authc.realms.jwt.jwt1.allowed_signature_algorithms", "HS256", "HS384")
            // 3rd JWT realm
            .put("xpack.security.authc.realms.jwt.jwt2.order", 30)
            .put("xpack.security.authc.realms.jwt.jwt2.token_type", "access_token")
            .put("xpack.security.authc.realms.jwt.jwt2.allowed_issuer", "my-issuer-03")
            .put("xpack.security.authc.realms.jwt.jwt2.allowed_subjects", "user-03")
            .put("xpack.security.authc.realms.jwt.jwt2.allowed_audiences", "es-03")
            .put("xpack.security.authc.realms.jwt.jwt2.fallback_claims.sub", "oid")
            .put("xpack.security.authc.realms.jwt.jwt2.claims.principal", "email")
            .put("xpack.security.authc.realms.jwt.jwt2.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt2.client_authentication.type", "shared_secret")
            .put("xpack.security.authc.realms.jwt.jwt2.client_authentication.rotation_grace_period", "0s")
            .putList("xpack.security.authc.realms.jwt.jwt2.allowed_signature_algorithms", "HS256", "HS384")
            // 4th JWT realm
            .put("xpack.security.authc.realms.jwt.jwt3.order", 40)
            .put("xpack.security.authc.realms.jwt.jwt3.token_type", "id_token")
            .put("xpack.security.authc.realms.jwt.jwt3.allowed_issuer", "my-issuer-04")
            .put("xpack.security.authc.realms.jwt.jwt3.allowed_subjects", "user-04")
            .put("xpack.security.authc.realms.jwt.jwt3.allowed_audiences", "es-04")
            .put("xpack.security.authc.realms.jwt.jwt3.claims.principal", "sub")
            .put("xpack.security.authc.realms.jwt.jwt3.claims.groups", "groups")
            .put("xpack.security.authc.realms.jwt.jwt3.client_authentication.type", "NONE")
            .put(
                "xpack.security.authc.realms.jwt.jwt3.pkc_jwkset_path",
                getDataPath("/org/elasticsearch/xpack/security/authc/apikey/rsa-public-jwkset.json")
            );

        SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt0.hmac_key", jwtHmacKey);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt0.client_authentication.shared_secret", jwt0SharedSecret);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt1.hmac_key", jwtHmacKey);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt1.client_authentication.shared_secret", jwt1SharedSecret);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt2.hmac_key", jwtHmacKey);
            secureSettings.setString("xpack.security.authc.realms.jwt.jwt2.client_authentication.shared_secret", jwt2SharedSecret);
        });

        return builder.build();
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + """
            anonymous:
              cluster:
                - monitor
            """;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @TestLogging(value = "org.elasticsearch.xpack.security.authc.jwt:DEBUG", reason = "failures can be very difficult to troubleshoot")
    public void testGrantApiKeyForJWT() throws Exception {
        final JWTClaimsSet.Builder jwtClaims = new JWTClaimsSet.Builder();
        final String subject;
        final String sharedSecret;
        // id_token or access_token
        if (randomBoolean()) {
            subject = "me";
            // JWT "id_token" valid for jwt0
            jwtClaims.audience("es-01")
                .issuer("my-issuer-01")
                .subject(subject)
                .claim("groups", "admin")
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                .build();
            sharedSecret = jwt0SharedSecret;
        } else {
            subject = "me@example.com";
            // JWT "access_token" valid for jwt2
            jwtClaims.audience("es-03")
                .issuer("my-issuer-03")
                .subject("user-03")
                .claim("groups", "admin")
                .claim("email", subject)
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(300)));
            sharedSecret = jwt2SharedSecret;
        }
        {
            // JWT is valid but the client authentication is NOT
            GrantApiKeyRequest grantApiKeyRequest = getGrantApiKeyForJWT(getSignedJWT(jwtClaims.build()), randomFrom("WRONG", null));
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet()
            );
            assertThat(e.getMessage(), containsString("unable to authenticate user"));
        }
        {
            // both JWT and client authentication are valid
            GrantApiKeyRequest grantApiKeyRequest = getGrantApiKeyForJWT(getSignedJWT(jwtClaims.build()), sharedSecret);
            CreateApiKeyResponse createApiKeyResponse = client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet();
            assertThat(createApiKeyResponse.getId(), notNullValue());
            assertThat(createApiKeyResponse.getKey(), notNullValue());
            assertThat(createApiKeyResponse.getName(), is(grantApiKeyRequest.getApiKeyRequest().getName()));
            final String base64ApiKeyKeyValue = Base64.getEncoder()
                .encodeToString((createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8));
            AuthenticateResponse authenticateResponse = client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE)
                .actionGet();
            assertThat(authenticateResponse.authentication().getEffectiveSubject().getUser().principal(), is(subject));
            assertThat(authenticateResponse.authentication().getAuthenticationType(), is(Authentication.AuthenticationType.API_KEY));
        }
        {
            // client authentication is valid but the JWT is not
            final SignedJWT wrongJWT;
            if (randomBoolean()) {
                wrongJWT = getSignedJWT(jwtClaims.build(), ("wrong key that's longer than 256 bits").getBytes(StandardCharsets.UTF_8));
            } else {
                wrongJWT = getSignedJWT(jwtClaims.audience("wrong audience claim value").build());
            }
            GrantApiKeyRequest grantApiKeyRequest = getGrantApiKeyForJWT(wrongJWT, sharedSecret);
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> client().execute(GrantApiKeyAction.INSTANCE, grantApiKeyRequest).actionGet()
            );
            assertThat(e.getMessage(), containsString("unable to authenticate user"));
        }
    }

    public void testActivateProfileForJWT() throws Exception {
        final JWTClaimsSet.Builder jwtClaims = new JWTClaimsSet.Builder();
        final String principal;
        final String sharedSecret;
        final String realmName;
        // id_token or access_token
        if (randomBoolean()) {
            principal = "me";
            // JWT "id_token" valid for jwt0
            jwtClaims.audience("es-01")
                .issuer("my-issuer-01")
                .subject(principal)
                .claim("groups", "admin")
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                .build();
            sharedSecret = jwt0SharedSecret;
            realmName = "jwt0";
        } else {
            principal = "me@example.com";
            // JWT "access_token" valid for jwt2
            jwtClaims.audience("es-03")
                .issuer("my-issuer-03")
                .subject("user-03")
                .claim("groups", "admin")
                .claim("email", principal)
                .issueTime(Date.from(Instant.now()))
                .expirationTime(Date.from(Instant.now().plusSeconds(300)));
            sharedSecret = jwt2SharedSecret;
            realmName = "jwt2";
        }
        {
            // JWT is valid but the client authentication is NOT
            ActivateProfileRequest activateProfileRequest = getActivateProfileForJWT(
                getSignedJWT(jwtClaims.build()),
                randomFrom("WRONG", null)
            );
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest).actionGet()
            );
            assertThat(e.getMessage(), containsString("unable to authenticate user"));
        }
        {
            // both JWT and client authentication are valid
            ActivateProfileRequest activateProfileRequest = getActivateProfileForJWT(getSignedJWT(jwtClaims.build()), sharedSecret);
            ActivateProfileResponse activateProfileResponse = client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest)
                .actionGet();
            assertThat(activateProfileResponse.getProfile(), notNullValue());
            assertThat(activateProfileResponse.getProfile().uid(), notNullValue());
            assertThat(activateProfileResponse.getProfile().user().username(), is(principal));
            assertThat(activateProfileResponse.getProfile().user().realmName(), is(realmName));
            // test to get the profile by uid
            GetProfilesRequest getProfilesRequest = new GetProfilesRequest(List.of(activateProfileResponse.getProfile().uid()), Set.of());
            GetProfilesResponse getProfilesResponse = client().execute(GetProfilesAction.INSTANCE, getProfilesRequest).actionGet();
            assertThat(getProfilesResponse.getProfiles().size(), is(1));
            assertThat(getProfilesResponse.getProfiles().get(0).uid(), is(activateProfileResponse.getProfile().uid()));
            assertThat(getProfilesResponse.getProfiles().get(0).enabled(), is(true));
            assertThat(getProfilesResponse.getProfiles().get(0).user().username(), is(principal));
            assertThat(getProfilesResponse.getProfiles().get(0).user().realmName(), is(realmName));
        }
        {
            // client authentication is valid but the JWT is not
            final SignedJWT wrongJWT;
            if (randomBoolean()) {
                wrongJWT = getSignedJWT(jwtClaims.build(), ("wrong key that's longer than 256 bits").getBytes(StandardCharsets.UTF_8));
            } else {
                wrongJWT = getSignedJWT(jwtClaims.audience("wrong audience claim value").build());
            }
            ActivateProfileRequest activateProfileRequest = getActivateProfileForJWT(wrongJWT, sharedSecret);
            ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest).actionGet()
            );
            assertThat(e.getMessage(), containsString("unable to authenticate user"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testInvalidJWTDoesNotFallbackToAnonymousAccess() throws Exception {
        // anonymous access works when no valid Bearer
        {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            // "Bearer" token missing or blank
            if (randomBoolean()) {
                options.addHeader("Authorization", "Bearer    ");
            }
            if (randomBoolean()) {
                options.addHeader(
                    "ES-Client-Authentication",
                    "SharedSecret " + randomFrom(jwt0SharedSecret, jwt1SharedSecret, jwt2SharedSecret)
                );
            }
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                try (InputStream content = entity.getContent()) {
                    XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                    Map<String, Object> result = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                    assertThat(result.get("username"), is("_anonymous"));
                    assertThat(result.get("roles"), instanceOf(Iterable.class));
                    assertThat((Iterable<String>) result.get("roles"), contains("anonymous"));
                }
            } else {
                throw new AssertionError(
                    "Unexpected _authenticate response status code [" + response.getStatusLine().getStatusCode() + "]"
                );
            }
        }
        // but invalid bearer JWT doesn't permit anonymous access
        {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader("Authorization", "Bearer obviously not a valid JWT token");
            if (randomBoolean()) {
                options.addHeader(
                    "ES-Client-Authentication",
                    "SharedSecret " + randomFrom(jwt0SharedSecret, jwt1SharedSecret, jwt2SharedSecret)
                );
            }
            request.setOptions(options);
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> getRestClient().performRequest(request).getStatusLine().getStatusCode()
            );
            assertEquals(401, exception.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testAnyJwtRealmWillExtractTheToken() throws ParseException {
        for (JwtRealm jwtRealm : getJwtRealms()) {
            final String sharedSecret = randomBoolean() ? randomAlphaOfLengthBetween(10, 20) : null;
            final String iss = randomAlphaOfLengthBetween(5, 18);
            final List<String> aud = List.of(randomAlphaOfLengthBetween(5, 18), randomAlphaOfLengthBetween(5, 18));
            final String sub = randomAlphaOfLengthBetween(5, 18);

            // JWT1 has all iss, sub, aud, principal claims.
            final SignedJWT signedJWT1 = getSignedJWT(Map.of("iss", iss, "aud", aud, "sub", sub));
            final ThreadContext threadContext1 = prepareThreadContext(signedJWT1, sharedSecret);
            final var token1 = (JwtAuthenticationToken) jwtRealm.token(threadContext1);
            final String principal1 = Strings.format("'aud:%s,%s' 'iss:%s' 'sub:%s'", aud.get(0), aud.get(1), iss, sub);
            assertJwtToken(token1, principal1, sharedSecret, signedJWT1);

            // JWT2, JWT3, and JWT4 don't have the sub claim.
            // Some realms define fallback claims for the sub claim (which themselves might not exist),
            // but that is not relevant for token building (it's used for user principal assembling).
            final String appId = randomAlphaOfLengthBetween(5, 18);
            final SignedJWT signedJWT2 = getSignedJWT(Map.of("iss", iss, "aud", aud, "client_id", sub, "appid", appId));
            final ThreadContext threadContext2 = prepareThreadContext(signedJWT2, sharedSecret);
            final var token2 = (JwtAuthenticationToken) jwtRealm.token(threadContext2);
            final String principal2 = Strings.format(
                "'appid:%s' 'aud:%s,%s' 'client_id:%s' 'iss:%s'",
                appId,
                aud.get(0),
                aud.get(1),
                sub,
                iss
            );
            assertJwtToken(token2, principal2, sharedSecret, signedJWT2);

            final String email = randomAlphaOfLengthBetween(5, 18) + "@example.com";
            final SignedJWT signedJWT3 = getSignedJWT(Map.of("iss", iss, "aud", aud, "oid", sub, "email", email));
            final ThreadContext threadContext3 = prepareThreadContext(signedJWT3, sharedSecret);
            final var token3 = (JwtAuthenticationToken) jwtRealm.token(threadContext3);
            final String principal3 = Strings.format("'aud:%s,%s' 'email:%s' 'iss:%s' 'oid:%s'", aud.get(0), aud.get(1), email, iss, sub);
            assertJwtToken(token3, principal3, sharedSecret, signedJWT3);

            final SignedJWT signedJWT4 = getSignedJWT(Map.of("iss", iss, "aud", aud, "azp", sub, "email", email));
            final ThreadContext threadContext4 = prepareThreadContext(signedJWT4, sharedSecret);
            final var token4 = (JwtAuthenticationToken) jwtRealm.token(threadContext4);
            final String principal4 = Strings.format("'aud:%s,%s' 'azp:%s' 'email:%s' 'iss:%s'", aud.get(0), aud.get(1), sub, email, iss);
            assertJwtToken(token4, principal4, sharedSecret, signedJWT4);

            // JWT5 does not have an issuer.
            final SignedJWT signedJWT5 = getSignedJWT(Map.of("aud", aud, "sub", sub));
            final ThreadContext threadContext5 = prepareThreadContext(signedJWT5, sharedSecret);
            final var token5 = (JwtAuthenticationToken) jwtRealm.token(threadContext5);
            final String principal5 = Strings.format("'aud:%s,%s' 'sub:%s'", aud.get(0), aud.get(1), sub);
            assertJwtToken(token5, principal5, sharedSecret, signedJWT5);
        }
    }

    public void testJwtRealmReturnsNullTokenWhenJwtCredentialIsAbsent() {
        final List<JwtRealm> jwtRealms = getJwtRealms();
        final JwtRealm jwtRealm = randomFrom(jwtRealms);
        final String sharedSecret = randomBoolean() ? randomAlphaOfLengthBetween(10, 20) : null;

        // Authorization header is absent
        final ThreadContext threadContext1 = prepareThreadContext(null, sharedSecret);
        assertThat(jwtRealm.token(threadContext1), nullValue());

        // Scheme is not Bearer
        final ThreadContext threadContext2 = prepareThreadContext(null, sharedSecret);
        threadContext2.putHeader("Authorization", "Basic foobar");
        assertThat(jwtRealm.token(threadContext2), nullValue());
    }

    public void testJwtRealmThrowsErrorOnJwtParsingFailure() throws ParseException {
        final List<JwtRealm> jwtRealms = getJwtRealms();
        final JwtRealm jwtRealm = randomFrom(jwtRealms);
        final String sharedSecret = randomBoolean() ? randomAlphaOfLengthBetween(10, 20) : null;

        // Not a JWT
        final ThreadContext threadContext1 = prepareThreadContext(null, sharedSecret);
        threadContext1.putHeader("Authorization", "Bearer " + randomAlphaOfLengthBetween(40, 60));
        assertThat(jwtRealm.token(threadContext1), nullValue());

        // Payload is not JSON
        final SignedJWT signedJWT2 = new SignedJWT(
            JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(5, 10))).toBase64URL(),
            Base64URL.encode("payload"),
            Base64URL.encode("signature")
        );
        final ThreadContext threadContext2 = prepareThreadContext(null, sharedSecret);
        threadContext2.putHeader("Authorization", "Bearer " + signedJWT2.serialize());
        assertThat(jwtRealm.token(threadContext2), nullValue());
    }

    @TestLogging(value = "org.elasticsearch.xpack.security.authc.jwt:DEBUG", reason = "failures can be very difficult to troubleshoot")
    public void testClientSecretRotation() throws Exception {
        final List<JwtRealm> jwtRealms = getJwtRealms();
        Map<String, JwtRealm> realmsByName = jwtRealms.stream().collect(Collectors.toMap(Realm::name, r -> r));
        JwtRealm realm0 = realmsByName.get("jwt0");
        JwtRealm realm1 = realmsByName.get("jwt1");
        JwtRealm realm2 = realmsByName.get("jwt2");
        // sanity check
        assertThat(getGracePeriod(realm0), equalTo(CLIENT_AUTH_SHARED_SECRET_ROTATION_GRACE_PERIOD.getDefault(Settings.EMPTY)));
        assertThat(getGracePeriod(realm1), equalTo(TimeValue.timeValueMinutes(10)));
        assertThat(getGracePeriod(realm2), equalTo(TimeValue.timeValueSeconds(0)));
        // create claims and test before rotation
        RestClient client = getRestClient();
        // valid jwt for realm0
        JWTClaimsSet.Builder jwt0Claims = new JWTClaimsSet.Builder();
        jwt0Claims.audience("es-01")
            .issuer("my-issuer-01")
            .subject("me")
            .claim("groups", "admin")
            .issueTime(Date.from(Instant.now()))
            .expirationTime(Date.from(Instant.now().plusSeconds(600)));
        assertEquals(
            200,
            client.performRequest(getAuthenticateRequest(getSignedJWT(jwt0Claims.build()), jwt0SharedSecret))
                .getStatusLine()
                .getStatusCode()
        );
        // valid jwt for realm1
        JWTClaimsSet.Builder jwt1Claims = new JWTClaimsSet.Builder();
        jwt1Claims.audience("es-02")
            .issuer("my-issuer-02")
            .subject("user-02")
            .claim("groups", "admin")
            .claim("appid", "X")
            .issueTime(Date.from(Instant.now()))
            .expirationTime(Date.from(Instant.now().plusSeconds(300)));
        assertEquals(
            200,
            client.performRequest(getAuthenticateRequest(getSignedJWT(jwt1Claims.build()), jwt1SharedSecret))
                .getStatusLine()
                .getStatusCode()
        );
        // valid jwt for realm2
        JWTClaimsSet.Builder jwt2Claims = new JWTClaimsSet.Builder();
        jwt2Claims.audience("es-03")
            .issuer("my-issuer-03")
            .subject("user-03")
            .claim("groups", "admin")
            .claim("email", "me@example.com")
            .issueTime(Date.from(Instant.now()))
            .expirationTime(Date.from(Instant.now().plusSeconds(300)));
        assertEquals(
            200,
            client.performRequest(getAuthenticateRequest(getSignedJWT(jwt2Claims.build()), jwt2SharedSecret))
                .getStatusLine()
                .getStatusCode()
        );
        final PluginsService plugins = getInstanceFromNode(PluginsService.class);
        final LocalStateSecurity localStateSecurity = plugins.filterPlugins(LocalStateSecurity.class).findFirst().get();
        // update the secret in the secure settings
        try {
            final MockSecureSettings newSecureSettings = new MockSecureSettings();
            newSecureSettings.setString(
                "xpack.security.authc.realms.jwt." + realm0.name() + ".client_authentication.shared_secret",
                "realm0updatedSecret"
            );
            newSecureSettings.setString(
                "xpack.security.authc.realms.jwt." + realm1.name() + ".client_authentication.shared_secret",
                "realm1updatedSecret"
            );
            newSecureSettings.setString(
                "xpack.security.authc.realms.jwt." + realm2.name() + ".client_authentication.shared_secret",
                "realm2updatedSecret"
            );
            // reload settings
            for (Plugin p : localStateSecurity.plugins()) {
                if (p instanceof Security securityPlugin) {
                    Settings.Builder newSettingsBuilder = Settings.builder().setSecureSettings(newSecureSettings);
                    securityPlugin.reload(newSettingsBuilder.build());
                }
            }
            // ensure the old value still works for realm 0 (default grace period)
            assertEquals(
                200,
                client.performRequest(getAuthenticateRequest(getSignedJWT(jwt0Claims.build()), jwt0SharedSecret))
                    .getStatusLine()
                    .getStatusCode()
            );
            assertEquals(
                200,
                client.performRequest(getAuthenticateRequest(getSignedJWT(jwt0Claims.build()), "realm0updatedSecret"))
                    .getStatusLine()
                    .getStatusCode()
            );
            // ensure the old value still works for realm 1 (explicit grace period)
            assertEquals(
                200,
                client.performRequest(getAuthenticateRequest(getSignedJWT(jwt1Claims.build()), jwt1SharedSecret))
                    .getStatusLine()
                    .getStatusCode()
            );
            assertEquals(
                200,
                client.performRequest(getAuthenticateRequest(getSignedJWT(jwt1Claims.build()), "realm1updatedSecret"))
                    .getStatusLine()
                    .getStatusCode()
            );
            // ensure the old value does not work for realm 2 (no grace period)
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> client.performRequest(getAuthenticateRequest(getSignedJWT(jwt2Claims.build()), jwt2SharedSecret))
                    .getStatusLine()
                    .getStatusCode()
            );
            assertEquals(401, exception.getResponse().getStatusLine().getStatusCode());
            assertEquals(
                200,
                client.performRequest(getAuthenticateRequest(getSignedJWT(jwt2Claims.build()), "realm2updatedSecret"))
                    .getStatusLine()
                    .getStatusCode()
            );
        } finally {
            // update them back to their original values
            final MockSecureSettings newSecureSettings = new MockSecureSettings();
            newSecureSettings.setString(
                "xpack.security.authc.realms.jwt." + realm0.name() + ".client_authentication.shared_secret",
                jwt0SharedSecret
            );
            newSecureSettings.setString(
                "xpack.security.authc.realms.jwt." + realm1.name() + ".client_authentication.shared_secret",
                jwt1SharedSecret
            );
            newSecureSettings.setString(
                "xpack.security.authc.realms.jwt." + realm2.name() + ".client_authentication.shared_secret",
                jwt2SharedSecret
            );
            // reload settings
            for (Plugin p : localStateSecurity.plugins()) {
                if (p instanceof Security securityPlugin) {
                    Settings.Builder newSettingsBuilder = Settings.builder().setSecureSettings(newSecureSettings);
                    securityPlugin.reload(newSettingsBuilder.build());
                }
            }
        }
    }

    public void testValidationDuringReloadingClientSecrets() {
        final Map<String, JwtRealm> realmsByName = getJwtRealms().stream().collect(Collectors.toMap(Realm::name, r -> r));
        final Set<JwtRealm> realmsWithSharedSecret = Set.of(realmsByName.get("jwt0"), realmsByName.get("jwt1"), realmsByName.get("jwt2"));
        final JwtRealm realmWithoutSharedSecret = realmsByName.get("jwt3");

        // Sanity check all client_authentication.type settings.
        for (JwtRealm realm : realmsWithSharedSecret) {
            assertThat(getClientAuthenticationType(realm), equalTo(JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET));
        }
        assertThat(getClientAuthenticationType(realmWithoutSharedSecret), equalTo(JwtRealmSettings.ClientAuthenticationType.NONE));

        // Randomly chose one JWT realm which requires shared secret and omit it.
        final MockSecureSettings newSecureSettings = new MockSecureSettings();
        final JwtRealm chosenRealmToRemoveSharedSecret = randomFrom(realmsWithSharedSecret);
        for (JwtRealm realm : realmsWithSharedSecret) {
            if (realm != chosenRealmToRemoveSharedSecret) {
                newSecureSettings.setString(
                    "xpack.security.authc.realms.jwt." + realm.name() + ".client_authentication.shared_secret",
                    realm.name() + "_shared_secret"
                );
            }
        }

        // Reload settings and check if validation prevented updating for randomly chosen realm.
        final PluginsService plugins = getInstanceFromNode(PluginsService.class);
        final LocalStateSecurity localStateSecurity = plugins.filterPlugins(LocalStateSecurity.class).findFirst().get();
        final Security securityPlugin = localStateSecurity.plugins()
            .stream()
            .filter(p -> p instanceof Security)
            .map(Security.class::cast)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Security plugin not found!"));

        Settings.Builder newSettingsBuilder = Settings.builder().setSecureSettings(newSecureSettings);
        {
            var e = expectThrows(ElasticsearchException.class, () -> securityPlugin.reload(newSettingsBuilder.build()));
            assertThat(e.getMessage(), containsString("secure settings reload failed for one or more security component"));

            var suppressedExceptions = e.getSuppressed();
            assertThat(suppressedExceptions.length, equalTo(1));
            assertThat(suppressedExceptions[0].getMessage(), containsString("secure settings reload failed for one or more realms"));

            var realmSuppressedExceptions = suppressedExceptions[0].getSuppressed();
            assertThat(realmSuppressedExceptions.length, equalTo(1));
            assertThat(
                realmSuppressedExceptions[0].getMessage(),
                containsString(
                    "Missing setting for [xpack.security.authc.realms.jwt."
                        + chosenRealmToRemoveSharedSecret.name()
                        + ".client_authentication.shared_secret]. It is required when setting [xpack.security.authc.realms.jwt."
                        + chosenRealmToRemoveSharedSecret.name()
                        + ".client_authentication.type] is ["
                        + JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET.value()
                        + "]"
                )
            );
        }

        // Add missing required shared secret setting in order
        // to avoid raising an exception for realm which has
        // client_authentication.type set to shared_secret.
        newSecureSettings.setString(
            "xpack.security.authc.realms.jwt." + chosenRealmToRemoveSharedSecret.name() + ".client_authentication.shared_secret",
            chosenRealmToRemoveSharedSecret.name() + "_shared_secret"
        );
        // Add shared secret for realm which does not require it,
        // because it has client_authentication.type set to NONE.
        newSecureSettings.setString(
            "xpack.security.authc.realms.jwt." + realmWithoutSharedSecret.name() + ".client_authentication.shared_secret",
            realmWithoutSharedSecret.name() + "_shared_secret"
        );

        {
            var e = expectThrows(ElasticsearchException.class, () -> securityPlugin.reload(newSettingsBuilder.build()));
            assertThat(e.getMessage(), containsString("secure settings reload failed for one or more security component"));

            var suppressedExceptions = e.getSuppressed();
            assertThat(suppressedExceptions.length, equalTo(1));
            assertThat(suppressedExceptions[0].getMessage(), containsString("secure settings reload failed for one or more realms"));

            var realmSuppressedExceptions = suppressedExceptions[0].getSuppressed();
            assertThat(realmSuppressedExceptions.length, equalTo(1));
            assertThat(
                realmSuppressedExceptions[0].getMessage(),
                containsString(
                    "Setting [xpack.security.authc.realms.jwt."
                        + realmWithoutSharedSecret.name()
                        + ".client_authentication.shared_secret] is not supported, because setting [xpack.security.authc.realms.jwt."
                        + realmWithoutSharedSecret.name()
                        + ".client_authentication.type] is ["
                        + JwtRealmSettings.ClientAuthenticationType.NONE.value()
                        + "]"
                )
            );
        }
    }

    static SignedJWT getSignedJWT(JWTClaimsSet claimsSet, byte[] hmacKeyBytes) throws Exception {
        JWSHeader jwtHeader = new JWSHeader.Builder(JWSAlgorithm.HS256).build();
        OctetSequenceKey.Builder jwt0signer = new OctetSequenceKey.Builder(hmacKeyBytes);
        jwt0signer.algorithm(JWSAlgorithm.HS256);
        SignedJWT jwt = new SignedJWT(jwtHeader, claimsSet);
        jwt.sign(new MACSigner(jwt0signer.build()));
        return jwt;
    }

    private SignedJWT getSignedJWT(JWTClaimsSet claimsSet) throws Exception {
        return getSignedJWT(claimsSet, jwtHmacKey.getBytes(StandardCharsets.UTF_8));
    }

    static Request getAuthenticateRequest(SignedJWT jwt, String sharedSecret) {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("Authorization", "Bearer  " + jwt.serialize());
        options.addHeader("ES-Client-Authentication", "SharedSecret " + sharedSecret);
        request.setOptions(options);
        return request;
    }

    private TimeValue getGracePeriod(JwtRealm realm) {
        return realm.getConfig().getConcreteSetting(CLIENT_AUTH_SHARED_SECRET_ROTATION_GRACE_PERIOD).get(realm.getConfig().settings());
    }

    private JwtRealmSettings.ClientAuthenticationType getClientAuthenticationType(JwtRealm realm) {
        return realm.getConfig().getConcreteSetting(CLIENT_AUTHENTICATION_TYPE).get(realm.getConfig().settings());
    }

    private void assertJwtToken(JwtAuthenticationToken token, String tokenPrincipal, String sharedSecret, SignedJWT signedJWT)
        throws ParseException {
        assertThat(token.principal(), equalTo(tokenPrincipal));
        assertThat(token.getClientAuthenticationSharedSecret(), equalTo(sharedSecret));
        assertThat(token.getJWTClaimsSet(), equalTo(signedJWT.getJWTClaimsSet()));
        assertThat(token.getSignedJWT().getHeader().toJSONObject(), equalTo(signedJWT.getHeader().toJSONObject()));
        assertThat(token.getSignedJWT().getSignature(), equalTo(signedJWT.getSignature()));
        assertThat(token.getSignedJWT().getJWTClaimsSet(), equalTo(token.getJWTClaimsSet()));
    }

    private List<JwtRealm> getJwtRealms() {
        final Realms realms = getInstanceFromNode(Realms.class);
        final List<JwtRealm> jwtRealms = realms.getActiveRealms()
            .stream()
            .filter(realm -> realm instanceof JwtRealm)
            .map(JwtRealm.class::cast)
            .toList();
        return jwtRealms;
    }

    private SignedJWT getSignedJWT(Map<String, Object> m) throws ParseException {
        final HashMap<String, Object> claimsMap = new HashMap<>(m);
        final Instant now = Instant.now();
        // timestamp does not matter for tokenExtraction
        claimsMap.put("iat", now.minus(randomIntBetween(-1, 1), ChronoUnit.DAYS).getEpochSecond());
        claimsMap.put("exp", now.plus(randomIntBetween(-1, 1), ChronoUnit.DAYS).getEpochSecond());

        final JWTClaimsSet claimsSet = JWTClaimsSet.parse(claimsMap);
        final SignedJWT signedJWT = new SignedJWT(
            JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(5, 10))).toBase64URL(),
            claimsSet.toPayload().toBase64URL(),
            Base64URL.encode("signature")
        );
        return signedJWT;
    }

    private ThreadContext prepareThreadContext(SignedJWT signedJWT, String clientSecret) {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        if (signedJWT != null) {
            threadContext.putHeader("Authorization", "Bearer " + signedJWT.serialize());
        }
        if (clientSecret != null) {
            threadContext.putHeader(
                JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + clientSecret
            );
        }
        return threadContext;
    }

    static GrantApiKeyRequest getGrantApiKeyForJWT(SignedJWT signedJWT, String sharedSecret) {
        GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.getGrant().setType("access_token");
        grantApiKeyRequest.getGrant().setAccessToken(new SecureString(signedJWT.serialize().toCharArray()));
        if (sharedSecret != null) {
            grantApiKeyRequest.getGrant()
                .setClientAuthentication(new Grant.ClientAuthentication("SharedSecret", new SecureString(sharedSecret.toCharArray())));
        }
        grantApiKeyRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        grantApiKeyRequest.getApiKeyRequest().setName(randomAlphaOfLength(8));
        return grantApiKeyRequest;
    }

    private static ActivateProfileRequest getActivateProfileForJWT(SignedJWT signedJWT, String sharedSecret) {
        ActivateProfileRequest activateProfileRequest = new ActivateProfileRequest();
        activateProfileRequest.getGrant().setType("access_token");
        activateProfileRequest.getGrant().setAccessToken(new SecureString(signedJWT.serialize().toCharArray()));
        if (sharedSecret != null) {
            activateProfileRequest.getGrant()
                .setClientAuthentication(new Grant.ClientAuthentication("SharedSecret", new SecureString(sharedSecret.toCharArray())));
        }
        return activateProfileRequest;
    }
}
