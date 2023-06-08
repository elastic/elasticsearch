/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * An integration test for validating SAML authentication against a real Identity Provider (Shibboleth)
 */
public class SamlAuthenticationIT extends ESRestTestCase {

    private static final String SAML_RESPONSE_FIELD = "SAMLResponse";
    private static final String KIBANA_PASSWORD = "K1b@na K1b@na K1b@na";

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * We perform all requests to Elasticsearch as the "kibana_system" user, as this is the user that will be used
     * in a typical SAML deployment (where Kibana is providing the UI for the SAML Web SSO interactions).
     * Before we can use the Kibana user, we need to set its password to something we know.
     */
    @Before
    public void setKibanaPassword() throws IOException {
        Request request = new Request("PUT", "/_security/user/kibana_system/_password");
        request.setJsonEntity("{ \"password\" : \"" + KIBANA_PASSWORD + "\" }");
        adminClient().performRequest(request);
    }

    /**
     * This is a simple mapping that maps the "thor" user in the "shibboleth" realm to the "kibana_admin" role.
     * We could do something more complex, but we have unit tests for role-mapping - this is just to verify that
     * the mapping runs OK in a real environment.
     */
    @Before
    public void setupRoleMapping() throws IOException {
        Request request = new Request("PUT", "/_security/role_mapping/thor-kibana");
        request.setJsonEntity(
            Strings.toString(
                XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .array("roles", new String[] { "kibana_admin" })
                    .field("enabled", true)
                    .startObject("rules")
                    .startArray("all")
                    .startObject()
                    .startObject("field")
                    .field("username", "thor")
                    .endObject()
                    .endObject()
                    .startObject()
                    .startObject("field")
                    .field("realm.name", "shibboleth")
                    .endObject()
                    .endObject()
                    .endArray() // "all"
                    .endObject() // "rules"
                    .endObject()
            )
        );
        adminClient().performRequest(request);
    }

    /**
     * Create a native user for "thor" that is used for user-lookup (authorizing realms)
     */
    @Before
    public void setupNativeUser() throws IOException {
        final Map<String, Object> body = MapBuilder.<String, Object>newMapBuilder()
            .put("roles", Collections.singletonList("kibana_admin"))
            .put("full_name", "Thor Son of Odin")
            .put("password", randomAlphaOfLengthBetween(inFipsJvm() ? 14 : 8, 16))
            .put("metadata", Collections.singletonMap("is_native", true))
            .map();
        final Response response = adminClient().performRequest(buildRequest("PUT", "/_security/user/thor", body));
        assertOK(response);
    }

    /**
     * Tests that a user can login via a SAML idp:
     * It uses:
     * <ul>
     * <li>A real IdP (Shibboleth, running locally)</li>
     * <li>A fake web browser (apache http client)</li>
     * <li>A fake "UI" ( same apache http client)</li>
     * </ul>
     * It takes the following steps:
     * <ol>
     * <li>Walks through the login process at the IdP</li>
     * <li>Receives a JSON response that has a Bearer token</li>
     * <li>Uses that token to verify the user details</li>
     * </ol>
     */
    public void testLoginUserWithSamlRoleMapping() throws Exception {
        // this realm name comes from the config in build.gradle
        final Tuple<String, String> authTokens = loginViaSaml("shibboleth");
        verifyElasticsearchAccessTokenForRoleMapping(authTokens.v1());
        final String accessToken = verifyElasticsearchRefreshToken(authTokens.v2());
        verifyElasticsearchAccessTokenForRoleMapping(accessToken);
    }

    public void testLoginUserWithAuthorizingRealm() throws Exception {
        // this realm name comes from the config in build.gradle
        final Tuple<String, String> authTokens = loginViaSaml("shibboleth_native");
        verifyElasticsearchAccessTokenForAuthorizingRealms(authTokens.v1());
        final String accessToken = verifyElasticsearchRefreshToken(authTokens.v2());
        verifyElasticsearchAccessTokenForAuthorizingRealms(accessToken);
    }

    public void testLoginWithWrongRealmFails() throws Exception {
        final BasicHttpContext context = new BasicHttpContext();
        try (CloseableHttpClient client = getHttpClient()) {
            // this realm name comes from the config in build.gradle
            final Tuple<URI, String> idAndLoginUri = getIdpLoginPage(client, context, "shibboleth_negative");
            final URI consentUri = submitLoginForm(client, context, idAndLoginUri.v1());
            final String samlResponse = submitConsentForm(context, client, consentUri);
            submitSamlResponse(samlResponse, idAndLoginUri.v2(), "shibboleth", false);
        }
    }

    private Tuple<String, String> loginViaSaml(String realmName) throws Exception {
        final BasicHttpContext context = new BasicHttpContext();
        try (CloseableHttpClient client = getHttpClient()) {
            final Tuple<URI, String> loginAndId = getIdpLoginPage(client, context, realmName);
            final URI consentUri = submitLoginForm(client, context, loginAndId.v1());
            final String samlResponse = submitConsentForm(context, client, consentUri);
            final Map<String, Object> result = submitSamlResponse(samlResponse, loginAndId.v2(), realmName, true);
            assertThat(result.get("username"), equalTo("thor"));

            final Object expiresIn = result.get("expires_in");
            assertThat(expiresIn, instanceOf(Number.class));
            assertThat(((Number) expiresIn).longValue(), greaterThan(TimeValue.timeValueMinutes(15).seconds()));

            final Object accessToken = result.get("access_token");
            assertThat(accessToken, notNullValue());
            assertThat(accessToken, instanceOf(String.class));

            final Object refreshToken = result.get("refresh_token");
            assertThat(refreshToken, notNullValue());
            assertThat(refreshToken, instanceOf(String.class));

            final Object authentication = result.get("authentication");
            assertThat(authentication, notNullValue());
            assertThat(authentication, instanceOf(Map.class));
            assertEquals("thor", ((Map) authentication).get("username"));

            return new Tuple<>((String) accessToken, (String) refreshToken);
        }
    }

    /**
     * Verifies that the provided "Access Token" (see org.elasticsearch.xpack.security.authc.TokenService)
     * is for the expected user with the expected name and roles if the user was created from Role-Mapping
     */
    private void verifyElasticsearchAccessTokenForRoleMapping(String accessToken) throws IOException {
        final Map<String, Object> map = callAuthenticateApiUsingAccessToken(accessToken);
        assertThat(map.get("username"), equalTo("thor"));
        assertThat(map.get("full_name"), equalTo("Thor Odinson"));
        assertSingletonList(map.get("roles"), "kibana_admin");

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertSingletonList(metadata.get("saml_uid"), "thor");
        assertSingletonList(metadata.get("saml(urn:oid:0.9.2342.19200300.100.1.1)"), "thor");
        assertSingletonList(metadata.get("saml_displayName"), "Thor Odinson");
        assertSingletonList(metadata.get("saml(urn:oid:2.5.4.3)"), "Thor Odinson");
    }

    /**
     * Verifies that the provided "Access Token" (see org.elasticsearch.xpack.security.authc.TokenService)
     * is for the expected user with the expected name and roles if the user was retrieved from the native realm
     */
    private void verifyElasticsearchAccessTokenForAuthorizingRealms(String accessToken) throws IOException {
        final Map<String, Object> map = callAuthenticateApiUsingAccessToken(accessToken);
        assertThat(map.get("username"), equalTo("thor"));
        assertThat(map.get("full_name"), equalTo("Thor Son of Odin"));
        assertSingletonList(map.get("roles"), "kibana_admin");

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertThat(metadata.get("is_native"), equalTo(true));
    }

    private Map<String, Object> callAuthenticateApiUsingAccessToken(String accessToken) throws IOException {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", "Bearer " + accessToken);
        request.setOptions(options);
        return entityAsMap(client().performRequest(request));
    }

    private String verifyElasticsearchRefreshToken(String refreshToken) throws IOException {
        final Map<String, ?> body = MapBuilder.<String, Object>newMapBuilder()
            .put("grant_type", "refresh_token")
            .put("refresh_token", refreshToken)
            .map();
        final Response response = client().performRequest(buildRequest("POST", "/_security/oauth2/token", body, kibanaAuth()));
        assertOK(response);

        final Map<String, Object> result = entityAsMap(response);
        final Object newRefreshToken = result.get("refresh_token");
        assertThat(newRefreshToken, notNullValue());
        assertThat(newRefreshToken, instanceOf(String.class));

        final Object accessToken = result.get("access_token");
        assertThat(accessToken, notNullValue());
        assertThat(accessToken, instanceOf(String.class));
        return (String) accessToken;
    }

    /**
     * Gets the SingleSignOnService endpoint of the IDP by calling the appropriate ES API, navigates to that URL and parses the form
     * URI that we can use to login to the Shibboleth IDP
     *
     * @return a Tuple with the URL of the login form in the IDP and the ID of the authentication request
     */
    private Tuple<URI, String> getIdpLoginPage(CloseableHttpClient client, BasicHttpContext context, String realmNane) throws Exception {
        final Map<String, String> body = Collections.singletonMap("realm", realmNane);
        Request request = buildRequest("POST", "/_security/saml/prepare", body, kibanaAuth());
        final Response prepare = client().performRequest(request);
        assertOK(prepare);
        final Map<String, Object> responseBody = parseResponseAsMap(prepare.getEntity());
        logger.info("Created SAML authentication request {}", responseBody);
        HttpGet login = new HttpGet((String) responseBody.get("redirect"));
        String target = execute(client, login, context, response -> {
            assertHttpOk(response.getStatusLine());
            return getFormTarget(response.getEntity().getContent());
        });

        assertThat("Cannot find form target", target, Matchers.notNullValue());
        assertThat("Target must be an absolute path", target, startsWith("/"));
        final Object host = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
        assertThat(host, instanceOf(HttpHost.class));
        final String uri = ((HttpHost) host).toURI() + target;
        return Tuple.tuple(new URI(uri), (String) responseBody.get("id"));
    }

    /**
     * Submits a Shibboleth login form to the provided URI.
     *
     * @return A URI to which the "consent form" should be submitted.
     */
    private URI submitLoginForm(CloseableHttpClient client, BasicHttpContext context, URI formUri) throws IOException {
        final HttpPost form = new HttpPost(formUri);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("j_username", "thor"));
        params.add(new BasicNameValuePair("j_password", "NickFuryHeartsES"));
        params.add(new BasicNameValuePair("_eventId_proceed", ""));
        form.setEntity(new UrlEncodedFormEntity(params));

        final String redirect = execute(client, form, context, response -> {
            logger.info(EntityUtils.toString(response.getEntity()));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(302));
            return response.getFirstHeader("Location").getValue();
        });

        String target = execute(client, new HttpGet(formUri.resolve(redirect)), context, response -> {
            assertHttpOk(response.getStatusLine());
            return getFormTarget(response.getEntity().getContent());
        });
        assertThat("Cannot find form target", target, Matchers.notNullValue());
        return formUri.resolve(target);
    }

    /**
     * Submits a Shibboleth consent form to the provided URI.
     * The consent form is a step that Shibboleth inserts into the login flow to confirm that the user is willing to send their
     * personal details to the application (SP) that they are logging in to.
     *
     * @return The SAMLResponse to post to the service
     */
    private String submitConsentForm(BasicHttpContext context, CloseableHttpClient client, URI consentUri) throws IOException {
        final HttpPost form = new HttpPost(consentUri);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("_shib_idp_consentOptions", "_shib_idp_globalConsent"));
        params.add(new BasicNameValuePair("_eventId_proceed", "Accept"));
        form.setEntity(new UrlEncodedFormEntity(params));

        return execute(client, form, context, response -> parseSamlSubmissionForm(response.getEntity().getContent()));
    }

    /**
     * Submits a SAML Response to the _security/saml/authenticate endpoint.
     *
     * @param saml          The (deflated + base64 encoded) {@code SAMLResponse} parameter to post
     * @param id            The SAML authentication request ID this response is InResponseTo
     * @param shouldSucceed Whether we expect this authentication to succeed
     */
    private Map<String, Object> submitSamlResponse(String saml, String id, String realmName, boolean shouldSucceed) throws IOException {
        // By POSTing to the ES API directly, we miss the check that the IDP would post this to the ACS that we would expect them to, but
        // we implicitly check this while checking the `Destination` element of the SAML response in the SAML realm.
        final Map<String, Object> bodyBuilder = Map.of("content", saml, "realm", realmName, "ids", Collections.singletonList(id));
        try {
            final Response response = client().performRequest(
                buildRequest("POST", "/_security/saml/authenticate", bodyBuilder, kibanaAuth())
            );
            if (shouldSucceed) {
                assertHttpOk(response.getStatusLine());
            }
            return parseResponseAsMap(response.getEntity());
        } catch (ResponseException e) {
            if (shouldSucceed == false) {
                assertHttpUnauthorized(e.getResponse().getStatusLine());
            }
            return Map.of();
        }
    }

    /**
     * Finds the target URL for the HTML form within the provided content.
     */
    private String getFormTarget(InputStream content) throws IOException {
        // Yes this is seriously bad - but would you prefer I run a headless browser for this?
        return findLine(Streams.readAllLines(content), "<form action=\"([^\"]+)\"");
    }

    /**
     * Finds the {@code SAMLResponse} for the HTML form from the provided content.
     */
    private String parseSamlSubmissionForm(InputStream content) throws IOException {
        final List<String> lines = Streams.readAllLines(content);
        return findLine(lines, "name=\"" + SAML_RESPONSE_FIELD + "\" value=\"([^\"]+)\"");
    }

    private String findLine(List<String> lines, String regex) {
        Pattern pattern = Pattern.compile(regex);
        for (String line : lines) {
            final Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private Map<String, Object> parseResponseAsMap(HttpEntity entity) throws IOException {
        return convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
    }

    private <T> T execute(
        CloseableHttpClient client,
        HttpRequestBase request,
        HttpContext context,
        CheckedFunction<HttpResponse, T, IOException> body
    ) throws IOException {
        final int timeout = (int) TimeValue.timeValueSeconds(90).millis();
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(timeout)
            .setConnectTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();
        request.setConfig(requestConfig);
        logger.info("Execute HTTP " + request.getMethod() + ' ' + request.getURI());
        try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(request, context))) {
            return body.apply(response);
        } catch (Exception e) {
            logger.warn(() -> "HTTP Request [" + request.getURI() + "] failed", e);
            throw e;
        }
    }

    private void assertHttpOk(StatusLine status) {
        assertThat("Unexpected HTTP Response status: " + status, status.getStatusCode(), Matchers.equalTo(200));
    }

    private void assertHttpUnauthorized(StatusLine status) {
        assertThat("Unexpected HTTP Response status: " + status, status.getStatusCode(), Matchers.equalTo(401));
    }

    private static void assertSingletonList(Object value, String expectedElement) {
        assertThat(value, instanceOf(List.class));
        assertThat(((List<?>) value), contains(expectedElement));
    }

    private Request buildRequest(String method, String endpoint, Map<String, ?> body, Header... headers) throws IOException {
        Request request = new Request(method, endpoint);
        XContentBuilder builder = XContentFactory.jsonBuilder().map(body);
        if (body != null) {
            request.setJsonEntity(BytesReference.bytes(builder).utf8ToString());
        }
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options);
        return request;
    }

    private static BasicHeader kibanaAuth() {
        final String auth = UsernamePasswordToken.basicAuthHeaderValue("kibana_system", new SecureString(KIBANA_PASSWORD.toCharArray()));
        return new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, auth);
    }

    private CloseableHttpClient getHttpClient() throws Exception {
        return HttpClients.custom().setSSLContext(getClientSslContext()).build();
    }

    private SSLContext getClientSslContext() throws Exception {
        final Path pem = getDataPath("/idp-browser.pem");
        final X509ExtendedTrustManager trustManager = CertParsingUtils.getTrustManagerFromPEM(List.of(pem));
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[0], new TrustManager[] { trustManager }, new SecureRandom());
        return context;
    }

}
