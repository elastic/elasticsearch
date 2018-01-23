/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
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
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.cookie.Cookie;
import org.apache.http.cookie.CookieOrigin;
import org.apache.http.cookie.MalformedCookieException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.DefaultCookieSpec;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.CharArrayBuffer;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.ssl.CertUtils;
import org.elasticsearch.xpack.security.authc.Realms;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * An integration test for validating SAML authentication against a real Identity Provider (Shibboleth)
 */
@SuppressForbidden(reason = "uses sun http server")
public class SamlAuthenticationIntegTests extends SecurityIntegTestCase {

    public static final String SP_LOGIN_PATH = "/saml/login";
    public static final String SP_ACS_PATH = "/saml/acs";
    public static final String SAML_RESPONSE_FIELD = "SAMLResponse";
    public static final String REQUEST_ID_COOKIE = "saml-request-id";
    private static HttpServer httpServer;


    @BeforeClass
    public static void setupSaml() throws Exception {
        SamlTestCase.setupSaml();
    }

    @AfterClass
    public static void cleanupSaml() throws Exception {
        SamlTestCase.restoreLocale();
    }

    @BeforeClass
    public static void setupHttpServer() throws IOException {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0);
        httpServer = MockHttpServer.createHttp(address, 0);
        httpServer.start();
    }

    @AfterClass
    public static void shutdownHttpServer() {
        final Executor executor = httpServer.getExecutor();
        if (executor instanceof ExecutorService) {
            try {
                terminate((ExecutorService) executor);
            } catch (InterruptedException e) {
                // oh well
            }
        }
        httpServer.stop(0);
        httpServer = null;
    }

    @Before
    public void setupHttpContext() {
        httpServer.createContext(SP_LOGIN_PATH, wrapFailures(this::httpLogin));
        httpServer.createContext(SP_ACS_PATH, wrapFailures(this::httpAcs));
    }

    /**
     * Wraps a {@code HttpHandler} in a {@code try-catch} block that returns a
     * 500 server error if an exception or an {@link AssertionError} occurs.
     */
    private HttpHandler wrapFailures(HttpHandler handler) {
        return http -> {
            try {
                handler.handle(http);
            } catch (AssertionError | Exception e) {
                logger.warn(new ParameterizedMessage("Failure while handling {}", http.getRequestURI()), e);
                http.getResponseHeaders().add("x-test-failure", e.toString());
                http.sendResponseHeaders(500, 0);
                http.close();
                throw e;
            }
        };
    }

    @After
    public void clearHttpContext() {
        httpServer.removeContext(SP_LOGIN_PATH);
        httpServer.removeContext(SP_ACS_PATH);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put("xpack.security.http.ssl.enabled", false)
                .put("xpack.security.authc.token.enabled", true)
                .put("xpack.security.authc.realms.file.type", FileRealmSettings.TYPE)
                .put("xpack.security.authc.realms.file.order", "0")
                .put("xpack.security.authc.realms.shibboleth.type", SamlRealmSettings.TYPE)
                .put("xpack.security.authc.realms.shibboleth.order", "1")
                .put("xpack.security.authc.realms.shibboleth.idp.entity_id", "https://test.shibboleth.elastic.local/")
                .put("xpack.security.authc.realms.shibboleth.idp.metadata.path", getDataPath("/idp-metadata.xml"))
                .put("xpack.security.authc.realms.shibboleth.sp.entity_id", "http://mock.http.elastic.local/")
                .put("xpack.security.authc.realms.shibboleth.sp.acs", getUrl(SP_ACS_PATH))
                .put("xpack.security.authc.realms.shibboleth.attributes.principal", "uid")
                .put("xpack.security.authc.realms.shibboleth.attributes.name", "urn:oid:2.5.4.3");

        return builder.build();
    }

    @After
    public void cleanupSecurity() {
        for (Realms realms : internalCluster().getInstances(Realms.class)) {
            realms.stream()
                    .filter(SamlRealm.class::isInstance)
                    .map(SamlRealm.class::cast)
                    .forEach(r -> Releasables.closeWhileHandlingException(r));
        }
        deleteSecurityIndex();
    }

    /**
     * We perform all requests to Elasticsearch as the "kibana" user, as this is the user that will be used
     * in a typical SAML deployment (where Kibana is providing the UI for the SAML Web SSO interacttions).
     * Before we can use the Kibana user, we need to set its password to something we know.
     */
    @Before
    public void setKibanaPassword() throws IOException {
        new ChangePasswordRequestBuilder(client())
                .username("kibana")
                .password(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
                .get();
    }

    /**
     * This is a simple mapping that maps the "thor" user in the "shibboleth" realm to the "kibana_users" role.
     * We could do something more complex, but we have unit tests for role-mapping - this is just to verify that
     * the mapping runs OK in a real environment.
     */
    @Before
    public void setupRoleMapping() throws IOException {
        final String json = XContentBuilder.builder(XContentType.JSON.xContent())
                .startObject()
                .startArray("all")
                .startObject()
                .startObject("field").field("username", "thor").endObject()
                .endObject()
                .startObject()
                .startObject("field").field("realm.name", "shibboleth").endObject()
                .endObject()
                .endArray()
                .endObject()
                .string();
        final NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;
        try (XContentParser parser = XContentType.JSON.xContent().createParser(registry, json)) {
            final RoleMapperExpression expression = ExpressionParser.parseObject(parser, "thor-kibana");
            new PutRoleMappingRequestBuilder(client(), PutRoleMappingAction.INSTANCE)
                    .enabled(true)
                    .name("thor-kibana")
                    .expression(expression)
                    .roles("kibana_user")
                    .get();
        }
    }

    /**
     * Tests that a user can login via a SAML idp:
     * It uses:
     * <ul>
     * <li>A real IdP (Shibboleth, running locally)</li>
     * <li>A fake UI, running in this JVM, that roughly mimic Kibana (see {@link #httpLogin}, {@link #httpAcs})</li>
     * <li>A fake web browser (apache http client)</li>
     * </ul>
     * It takes the following steps:
     * <ol>
     * <li>Requests a "login" on the local UI</li>
     * <li>Walks through the login process at the IdP</li>
     * <li>Receives a JSON response from the local UI that has a Bearer token</li>
     * <li>Uses that token to verify the user details</li>
     * </ol>
     */
    public void testLoginUser() throws Exception {
        final BasicHttpContext context = new BasicHttpContext();
        try (CloseableHttpClient client = getHttpClient()) {
            final URI loginUri = goToLoginPage(client, context);
            final URI consentUri = submitLoginForm(client, context, loginUri);
            final Tuple<URI, String> tuple = submitConsentForm(context, client, consentUri);
            final Map<String, Object> result = submitSamlResponse(context, client, tuple.v1(), tuple.v2());
            assertThat(result.get("username"), equalTo("thor"));

            final Object expiresIn = result.get("expires_in");
            assertThat(expiresIn, instanceOf(Number.class));
            assertThat(((Number) expiresIn).longValue(), greaterThan(TimeValue.timeValueMinutes(15).seconds()));

            final Object accessToken = result.get("access_token");
            assertThat(accessToken, notNullValue());
            assertThat(accessToken, instanceOf(String.class));
            verifyElasticsearchAccessToken((String) accessToken);

            final Object refreshToken = result.get("refresh_token");
            assertThat(refreshToken, notNullValue());
            assertThat(refreshToken, instanceOf(String.class));
            verifyElasticsearchRefreshToken((String) refreshToken);
        }
    }

    /**
     * Verifies that the provided "Access Token" (see {@link org.elasticsearch.xpack.security.authc.TokenService})
     * is for the expected user with the expected name and roles.
     */
    private void verifyElasticsearchAccessToken(String accessToken) throws IOException {
        final BasicHeader authorization = new BasicHeader("Authorization", "Bearer " + accessToken);
        final Response response = getRestClient().performRequest("GET", "/_xpack/security/_authenticate", authorization);
        assertHttpOk(response);
        final Map<String, Object> map = parseResponseAsMap(response.getEntity());
        assertThat(map.get("username"), equalTo("thor"));
        assertThat(map.get("full_name"), equalTo("Thor Odinson"));
        assertSingletonList(map.get("roles"), "kibana_user");

        assertThat(map.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) map.get("metadata");
        assertSingletonList(metadata.get("saml_uid"), "thor");
        assertSingletonList(metadata.get("saml(urn:oid:0.9.2342.19200300.100.1.1)"), "thor");
        assertSingletonList(metadata.get("saml_displayName"), "Thor Odinson");
        assertSingletonList(metadata.get("saml(urn:oid:2.5.4.3)"), "Thor Odinson");
    }

    /**
     * Verifies that the provided "Refresh Token" (see {@link org.elasticsearch.xpack.security.authc.TokenService})
     * can be used to get a new valid access token and refresh token.
     */
    private void verifyElasticsearchRefreshToken(String refreshToken) throws IOException {
        final String body = "{ \"grant_type\":\"refresh_token\", \"refresh_token\":\"" + refreshToken + "\" }";
        final Response response = getRestClient().performRequest("POST", "/_xpack/security/oauth2/token",
                emptyMap(), new StringEntity(body, ContentType.APPLICATION_JSON), authHeader("kibana"));
        assertHttpOk(response);

        final Map<String, Object> result = parseResponseAsMap(response.getEntity());
        final Object newRefreshToken = result.get("refresh_token");
        assertThat(newRefreshToken, notNullValue());
        assertThat(newRefreshToken, instanceOf(String.class));

        final Object accessToken = result.get("access_token");
        assertThat(accessToken, notNullValue());
        assertThat(accessToken, instanceOf(String.class));
        verifyElasticsearchAccessToken((String) accessToken);
    }

    /**
     * Navigates to the login page on the local (in memory) HTTP UI.
     *
     * @return A URI to which the "login form" should be submitted.
     */
    private URI goToLoginPage(CloseableHttpClient client, BasicHttpContext context) throws IOException {
        HttpGet login = new HttpGet(getUrl(SP_LOGIN_PATH));
        String target = execute(client, login, context, response -> {
            assertHttpOk(response.getStatusLine());
            return getFormTarget(response.getEntity().getContent());
        });

        assertThat("Cannot find form target", target, Matchers.notNullValue());
        assertThat("Target must be an absolute path", target, startsWith("/"));
        final Object host = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
        assertThat(host, instanceOf(HttpHost.class));

        final String uri = ((HttpHost) host).toURI() + target;
        return toUri(uri);
    }

    /**
     * Submits a Shibboleth login form to the provided URI.
     *
     * @return A URI to which the "consent form" should be submitted.
     */
    private URI submitLoginForm(CloseableHttpClient client, BasicHttpContext context, URI formUri) throws IOException {
        final HttpPost form = new HttpPost(formUri);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("j_username", "Thor"));
        params.add(new BasicNameValuePair("j_password", "NickFuryHeartsES"));
        params.add(new BasicNameValuePair("_eventId_proceed", ""));
        form.setEntity(new UrlEncodedFormEntity(params));

        final String redirect = execute(client, form, context, response -> {
            assertThat(response.getStatusLine().getStatusCode(), equalTo(302));
            return response.getFirstHeader("Location").getValue();
        });
        assertThat(redirect, startsWith("/"));

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
     * @return A tuple of ( URI to SP's Assertion-Consumer-Service, SAMLResponse to post to the service )
     */
    private Tuple<URI, String> submitConsentForm(BasicHttpContext context, CloseableHttpClient client, URI consentUri) throws IOException {
        final HttpPost form = new HttpPost(consentUri);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("_shib_idp_consentOptions", "_shib_idp_globalConsent"));
        params.add(new BasicNameValuePair("_eventId_proceed", "Accept"));
        form.setEntity(new UrlEncodedFormEntity(params));

        return execute(client, form, context,
                response -> parseSamlSubmissionForm(response.getEntity().getContent()));
    }

    /**
     * Submits a SAML assertion to the ACS URI.
     *
     * @param acs  The URI to the Service Provider's Assertion-Consumer-Service.
     * @param saml The (deflated + base64 encoded) {@code SAMLResponse} parameter to post the ACS
     */
    private Map<String, Object> submitSamlResponse(BasicHttpContext context, CloseableHttpClient client, URI acs, String saml)
            throws IOException {
        assertThat("SAML submission target", acs, notNullValue());
        assertThat(acs.getPath(), equalTo(SP_ACS_PATH));
        assertThat("SAML submission content", saml, notNullValue());

        final HttpPost form = new HttpPost(acs);
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair(SAML_RESPONSE_FIELD, saml));
        form.setEntity(new UrlEncodedFormEntity(params));

        return execute(client, form, context, response -> {
            assertHttpOk(response.getStatusLine());
            return parseResponseAsMap(response.getEntity());
        });
    }

    /**
     * Finds the target URL for the HTML form within the provided content.
     */
    private String getFormTarget(InputStream content) throws IOException {
        // Yes this is seriously bad - but would you prefer I run a headless browser for this?
        return findLine(Streams.readAllLines(content), "<form action=\"([^\"]+)\"");
    }

    /**
     * Finds the target URL and {@code SAMLResponse} for the HTML form from the provided content.
     */
    private Tuple<URI, String> parseSamlSubmissionForm(InputStream content) throws IOException {
        final List<String> lines = Streams.readAllLines(content);
        return new Tuple<>(
                toUri(htmlDecode(findLine(lines, "<form action=\"([^\"]+)\""))),
                findLine(lines, "name=\"" + SAML_RESPONSE_FIELD + "\" value=\"([^\"]+)\"")
        );
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

    private String htmlDecode(String text) {
        final Pattern hexEntity = Pattern.compile("&#x([0-9a-f]{2});");
        while (true) {
            final Matcher matcher = hexEntity.matcher(text);
            if (matcher.find() == false) {
                return text;
            }
            char ch = (char) Integer.parseInt(matcher.group(1), 16);
            text = matcher.replaceFirst(Character.toString(ch));
        }
    }

    private URI toUri(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            fail("Cannot parse URI " + uri + " - " + e);
            return null;
        }
    }

    private Map<String, Object> parseResponseAsMap(HttpEntity entity) throws IOException {
        return convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
    }

    private <T> T execute(CloseableHttpClient client, HttpRequestBase request,
                          HttpContext context, CheckedFunction<HttpResponse, T, IOException> body)
            throws IOException {
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
            logger.warn(new ParameterizedMessage("HTTP Request [{}] failed", request.getURI()), e);
            throw e;
        }
    }

    private String getUrl(String path) {
        return getWebServerUri().resolve(path).toString();
    }

    /**
     * Provides the "login" handler for the fake WebApp.
     * This interacts with Elasticsearch (using the rest client) to find the login page for the IdP, and then
     * sends a redirect to that page.
     */
    private void httpLogin(HttpExchange http) throws IOException {
        final Response prepare = getRestClient().performRequest("POST", "/_xpack/security/saml/prepare",
                emptyMap(), new StringEntity("{}", ContentType.APPLICATION_JSON), authHeader("kibana"));
        assertHttpOk(prepare);
        final Map<String, Object> body = parseResponseAsMap(prepare.getEntity());
        logger.info("Created SAML authentication request {}", body);
        http.getResponseHeaders().add("Set-Cookie", REQUEST_ID_COOKIE + "=" + body.get("id"));
        http.getResponseHeaders().add("Location", (String) body.get("redirect"));
        http.sendResponseHeaders(302, 0);
        http.close();
    }

    /**
     * Provides the "Assertion-Consumer-Service" handler for the fake WebApp.
     * This interacts with Elasticsearch (using the rest client) to perform a SAML login, and just
     * forwards the JSON response back to the client.
     */
    private void httpAcs(HttpExchange http) throws IOException {
        final Response saml = samlAuthenticate(http);
        assertHttpOk(saml);
        final byte[] content = Streams.copyToString(new InputStreamReader(saml.getEntity().getContent())).getBytes();
        http.getResponseHeaders().add("Content-Type", "application/json");
        http.sendResponseHeaders(200, content.length);
        http.getResponseBody().write(content);
        http.close();
    }

    private Response samlAuthenticate(HttpExchange http) throws IOException {
        final List<NameValuePair> pairs = parseRequestForm(http);
        assertThat(pairs, iterableWithSize(1));
        final String saml = pairs.stream()
                .filter(p -> SAML_RESPONSE_FIELD.equals(p.getName()))
                .map(p -> p.getValue())
                .findFirst()
                .orElseGet(() -> {
                    fail("Cannot find " + SAML_RESPONSE_FIELD + " in form fields");
                    return null;
                });

        final String id = getCookie(REQUEST_ID_COOKIE, http);
        assertThat(id, notNullValue());

        final String body = "{ \"content\" : \"" + saml + "\", \"ids\": [\"" + id + "\"] }";
        return getRestClient().performRequest("POST", "/_xpack/security/saml/authenticate",
                emptyMap(), new StringEntity(body, ContentType.APPLICATION_JSON), authHeader("kibana"));
    }

    private List<NameValuePair> parseRequestForm(HttpExchange http) throws IOException {
        String reqContent = Streams.copyToString(new InputStreamReader(http.getRequestBody()));
        final CharArrayBuffer buffer = new CharArrayBuffer(reqContent.length());
        buffer.append(reqContent);
        return URLEncodedUtils.parse(buffer, HTTP.DEF_CONTENT_CHARSET, '&');
    }

    private String getCookie(String name, HttpExchange http) throws IOException {
        try {
            final String cookies = http.getRequestHeaders().getFirst("Cookie");
            if (cookies == null) {
                return null;
            }
            Header header = new BasicHeader("Cookie", cookies);
            final URI serverUri = getWebServerUri();
            final URI requestURI = http.getRequestURI();
            final CookieOrigin origin = new CookieOrigin(serverUri.getHost(), serverUri.getPort(), requestURI.getPath(), false);
            final List<Cookie> parsed = new DefaultCookieSpec().parse(header, origin);
            return parsed.stream().filter(c -> name.equals(c.getName())).map(c -> c.getValue()).findFirst().orElse(null);
        } catch (MalformedCookieException e) {
            throw new IOException("Cannot read cookies", e);
        }
    }

    private void assertHttpOk(Response response) {
        assertHttpOk(response.getStatusLine());
    }

    private void assertHttpOk(StatusLine status) {
        assertThat("Unexpected HTTP Response status: " + status, status.getStatusCode(), Matchers.equalTo(200));
    }

    private static void assertSingletonList(Object value, String expectedElement) {
        assertThat(value, instanceOf(List.class));
        assertThat(((List<?>) value), contains(expectedElement));
    }

    private static BasicHeader authHeader(String userName) {
        return new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(userName, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
    }

    private CloseableHttpClient getHttpClient() throws Exception {
        return HttpClients.custom().setSSLContext(getClientSslContext()).build();
    }

    private SSLContext getClientSslContext() throws Exception {
        final Path pem = getDataPath("/ca_server.pem");
        final Certificate[] certificates = CertUtils.readCertificates(Collections.singletonList(pem));
        final X509ExtendedTrustManager trustManager = CertUtils.trustManager(certificates);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[0], new TrustManager[] { trustManager }, new SecureRandom());
        return context;
    }

    private URI getWebServerUri() {
        final InetSocketAddress address = httpServer.getAddress();
        final String host = address.getHostString();
        final int port = address.getPort();
        try {
            return new URI("http", null, host, port, "/", null, null);
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("Cannot construct URI for httpServer @ {}:{}", e, host, port);
        }
    }

}
