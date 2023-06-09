/// *
// * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// * or more contributor license agreements. Licensed under the Elastic License
// * 2.0; you may not use this file except in compliance with the Elastic License
// * 2.0.
// */
//
// package org.elasticsearch.xpack.security.authc.jwt;
//
// import com.nimbusds.oauth2.sdk.ResponseType;
// import com.nimbusds.oauth2.sdk.Scope;
// import com.nimbusds.oauth2.sdk.id.ClientID;
// import com.nimbusds.oauth2.sdk.id.State;
// import com.nimbusds.openid.connect.sdk.AuthenticationRequest;
// import com.nimbusds.openid.connect.sdk.Nonce;
// import com.nimbusds.openid.connect.sdk.OIDCScopeValue;
//
// import org.elasticsearch.client.RequestOptions;
// import org.elasticsearch.client.ResponseException;
// import org.elasticsearch.core.Strings;
// import org.elasticsearch.rest.RestStatus;
// import org.elasticsearch.rest.RestUtils;
// import org.elasticsearch.test.TestMatchers;
// import org.elasticsearch.test.TestSecurityClient;
// import org.elasticsearch.xpack.core.security.user.User;
// import org.elasticsearch.xpack.security.authc.oidc.C2IdOpTestCase;
// import org.hamcrest.Matchers;
// import org.junit.Before;
// import org.junit.BeforeClass;
//
// import java.io.IOException;
// import java.net.URI;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
//
// import static org.hamcrest.Matchers.contains;
// import static org.hamcrest.Matchers.greaterThan;
// import static org.hamcrest.Matchers.hasKey;
//
/// **
// * Tests that the Elasticsearch "jwt" realm ({@link JwtRealm}) can be used to authenticate using a JWT that is actually an ID Token
// * from a real OIDC Originating Party (OP).
// * This test is intentionally minimal - the bulk of the JWT functionality is tested elsewhere - the purpose of this test is purely to
// * verify successful integration an OIDC OP.
// */
// public class JwtWithOidcAuthIT extends C2IdOpTestCase {
//
// // configured in the Elasticearch node test fixture
// private static final List<String> ALLOWED_AUDIENCES = List.of("elasticsearch-jwt1", "elasticsearch-jwt2");
// private static final String JWT_REALM_NAME = "op-jwt";
//
// // Constants for role mapping
// private static final String ROLE_NAME = "jwt_role";
// private static final String SHARED_SECRET = "jwt-realm-shared-secret";
//
// // Randomised values
// private static String clientId;
// private static String redirectUri;
//
// /**
// * Register an OIDC client so we can generate a JWT in C2id (which only supports dynamic configuration).
// */
// @BeforeClass
// public static void registerClient() throws IOException {
// clientId = randomFrom(ALLOWED_AUDIENCES);
// redirectUri = "https://" + randomAlphaOfLength(4) + ".rp.example.com/" + randomAlphaOfLength(6);
// String clientSecret = randomAlphaOfLength(24);
// String clientSetup = Strings.format("""
// {
// "grant_types": [ "implicit" ],
// "response_types": [ "token id_token" ],
// "preferred_client_id": "%s",
// "preferred_client_secret": "%s",
// "redirect_uris": [ "%s" ]
// }""", clientId, clientSecret, redirectUri);
// registerClients(clientSetup);
// }
//
// @Before
// public void setupRoleMapping() throws Exception {
// try (var restClient = getElasticsearchClient()) {
// var client = new TestSecurityClient(restClient);
// final String mappingJson = Strings.format("""
// {
// "roles": [ "%s" ],
// "enabled": true,
// "rules": {
// "all": [
// { "field": { "realm.name": "%s" } },
// { "field": { "metadata.jwt_claim_sub": "%s" } }
// ]
// }
// }
// """, ROLE_NAME, JWT_REALM_NAME, TEST_SUBJECT_ID);
// client.putRoleMapping(getTestName(), mappingJson);
// }
// }
//
// public void testAuthenticateWithOidcIssuedJwt() throws Exception {
// final String state = randomAlphaOfLength(42);
// final String nonce = randomAlphaOfLength(42);
// final AuthenticationRequest oidcAuthRequest = new AuthenticationRequest.Builder(
// new ResponseType("id_token", "token"),
// new Scope(OIDCScopeValue.OPENID),
// new ClientID(clientId),
// new URI(redirectUri)
// ).endpointURI(new URI(C2ID_AUTH_ENDPOINT)).state(new State(state)).nonce(new Nonce(nonce)).build();
//
// final String implicitFlowURI = authenticateAtOP(oidcAuthRequest.toURI());
//
// assertThat("Hash value of URI should be a JWT", implicitFlowURI, Matchers.containsString("#"));
//
// /*
// * In OIDC's implicit flow, the JWT is provided in the URI as a hash fragment using form encoding
// * (See Section 4.2.2 of the OAuth2 spec - https://www.rfc-editor.org/rfc/rfc6749.html#section-4.2.2)
// * We're not trying to do OIDC - we're just trying to get an id_token shaped JWT from a real OIDC OP server (c2id) and use it
// * to authenticated against our JWT realm.
// * So, we extract the hash fragment, and decode it as a query string (which is not quite form encoding, but does the job).
// * The three-part-encoded JWT id_token will be in the "id_token" field
// */
// final int hashChar = implicitFlowURI.indexOf('#');
// final Map<String, String> hashParams = new HashMap<>();
// RestUtils.decodeQueryString(implicitFlowURI.substring(hashChar + 1), 0, hashParams);
//
// assertThat("Hash value of URI [" + implicitFlowURI + "] should be a JWT with an id Token", hashParams, hasKey("id_token"));
// String idJwt = hashParams.get("id_token");
//
// final Map<String, Object> authenticateResponse = authenticateWithJwtAndSharedSecret(idJwt, SHARED_SECRET);
// assertThat(authenticateResponse, Matchers.hasEntry(User.Fields.USERNAME.getPreferredName(), TEST_SUBJECT_ID));
// assertThat(authenticateResponse, Matchers.hasKey(User.Fields.ROLES.getPreferredName()));
// assertThat((List<?>) authenticateResponse.get(User.Fields.ROLES.getPreferredName()), contains(ROLE_NAME));
//
// // Use an incorrect shared secret and check it fails
// ResponseException ex = expectThrows(
// ResponseException.class,
// () -> authenticateWithJwtAndSharedSecret(idJwt, "not-" + SHARED_SECRET)
// );
// assertThat(ex.getResponse(), TestMatchers.hasStatusCode(RestStatus.UNAUTHORIZED));
//
// // Modify the JWT payload and check it fails
// final int dot = idJwt.indexOf('.');
// assertThat(dot, greaterThan(0));
// // change the first character of the payload section of the encoded JWT
// final String corruptToken = idJwt.substring(0, dot) + "." + transformChar(idJwt.charAt(dot + 1)) + idJwt.substring(dot + 2);
// ex = expectThrows(ResponseException.class, () -> authenticateWithJwtAndSharedSecret(corruptToken, SHARED_SECRET));
// assertThat(ex.getResponse(), TestMatchers.hasStatusCode(RestStatus.UNAUTHORIZED));
// }
//
// private Map<String, Object> authenticateWithJwtAndSharedSecret(String idJwt, String sharedSecret) throws IOException {
// final Map<String, Object> authenticateResponse = super.callAuthenticateApiUsingBearerToken(
// idJwt,
// RequestOptions.DEFAULT.toBuilder()
// .addHeader(JwtRealm.HEADER_CLIENT_AUTHENTICATION, JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + sharedSecret)
// .build()
// );
// return authenticateResponse;
// }
//
// private char transformChar(char c) {
// if (Character.isLowerCase(c)) {
// return Character.toUpperCase(c);
// }
// if (Character.isUpperCase(c)) {
// return Character.toLowerCase(c);
// }
// // For anything non-alphabetic we can just return a random alpha char
// return randomAlphaOfLength(1).charAt(0);
// }
//
// }
