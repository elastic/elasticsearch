/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginContext;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration test to demonstrate authentication against a real MIT Kerberos
 * instance.
 * <p>
 * Demonstrates login by keytab and login by password for given user principal
 * name using rest client.
 */
public class KerberosAuthenticationIT extends ESRestTestCase {
    private static final String ENABLE_KERBEROS_DEBUG_LOGS_KEY = "test.krb.debug";
    private static final String TEST_USER_WITH_KEYTAB_KEY = "test.userkt";
    private static final String TEST_USER_WITH_KEYTAB_PATH_KEY = "test.userkt.keytab";
    private static final String TEST_USER_WITH_PWD_KEY = "test.userpwd";
    private static final String TEST_USER_WITH_PWD_PASSWD_KEY = "test.userpwd.password";
    private static final String TEST_KERBEROS_REALM_NAME = "kerberos";

    @Override
    protected Settings restAdminSettings() {
        final String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Creates simple mapping that maps the users from 'kerberos' realm to
     * the 'kerb_test' role.
     */
    @Before
    public void setupRoleMapping() throws IOException {
        final String json = Strings // top-level
                .toString(XContentBuilder.builder(XContentType.JSON.xContent()).startObject()
                        .array("roles", new String[] { "kerb_test" })
                        .field("enabled", true)
                        .startObject("rules")
                        .startArray("all")
                        .startObject().startObject("field").field("realm.name", TEST_KERBEROS_REALM_NAME).endObject().endObject()
                        .endArray() // "all"
                        .endObject() // "rules"
                        .endObject());

        final Request request = new Request("POST", "/_xpack/security/role_mapping/kerberosrolemapping");
        request.setJsonEntity(json);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
    }

    public void testLoginByKeytab() throws IOException, PrivilegedActionException {
        final String userPrincipalName = System.getProperty(TEST_USER_WITH_KEYTAB_KEY);
        final String keytabPath = System.getProperty(TEST_USER_WITH_KEYTAB_PATH_KEY);
        final boolean enabledDebugLogs = Boolean.parseBoolean(System.getProperty(ENABLE_KERBEROS_DEBUG_LOGS_KEY));
        final SpnegoHttpClientConfigCallbackHandler callbackHandler = new SpnegoHttpClientConfigCallbackHandler(userPrincipalName,
                keytabPath, enabledDebugLogs);
        executeRequestAndVerifyResponse(userPrincipalName, callbackHandler);
    }

    public void testLoginByUsernamePassword() throws IOException, PrivilegedActionException {
        final String userPrincipalName = System.getProperty(TEST_USER_WITH_PWD_KEY);
        final String password = System.getProperty(TEST_USER_WITH_PWD_PASSWD_KEY);
        final boolean enabledDebugLogs = Boolean.parseBoolean(System.getProperty(ENABLE_KERBEROS_DEBUG_LOGS_KEY));
        final SpnegoHttpClientConfigCallbackHandler callbackHandler = new SpnegoHttpClientConfigCallbackHandler(userPrincipalName,
                new SecureString(password.toCharArray()), enabledDebugLogs);
        executeRequestAndVerifyResponse(userPrincipalName, callbackHandler);
    }

    @Override
    @SuppressForbidden(reason = "SPNEGO relies on hostnames and we need to ensure host isn't a IP address")
    protected HttpHost buildHttpHost(String host, int port) {
        try {
            InetAddress inetAddress = InetAddress.getByName(host);
            return super.buildHttpHost(inetAddress.getCanonicalHostName(), port);
        } catch (UnknownHostException e) {
            assumeNoException("failed to resolve host [" + host + "]", e);
        }
        throw new IllegalStateException("DNS not resolved and assume did not trip");
    }

    private void executeRequestAndVerifyResponse(final String userPrincipalName,
            final SpnegoHttpClientConfigCallbackHandler callbackHandler) throws PrivilegedActionException, IOException {
        final Request request = new Request("GET", "/_xpack/security/_authenticate");
        try (RestClient restClient = buildRestClientForKerberos(callbackHandler)) {
            final AccessControlContext accessControlContext = AccessController.getContext();
            final LoginContext lc = callbackHandler.login();
            Response response = SpnegoHttpClientConfigCallbackHandler.doAsPrivilegedWrapper(lc.getSubject(),
                    (PrivilegedExceptionAction<Response>) () -> {
                        return restClient.performRequest(request);
                    }, accessControlContext);

            assertOK(response);
            final Map<String, Object> map = parseResponseAsMap(response.getEntity());
            assertThat(map.get("username"), equalTo(userPrincipalName));
            assertThat(map.get("roles"), instanceOf(List.class));
            assertThat(((List<?>) map.get("roles")), contains("kerb_test"));
        }
    }

    private Map<String, Object> parseResponseAsMap(final HttpEntity entity) throws IOException {
        return convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
    }

    private RestClient buildRestClientForKerberos(final SpnegoHttpClientConfigCallbackHandler callbackHandler) throws IOException {
        final Settings settings = restAdminSettings();
        final HttpHost[] hosts = getClusterHosts().toArray(new HttpHost[getClusterHosts().size()]);

        final RestClientBuilder restClientBuilder = RestClient.builder(hosts);
        configureRestClientBuilder(restClientBuilder, settings);
        restClientBuilder.setHttpClientConfigCallback(callbackHandler);
        return restClientBuilder.build();
    }

    private static void configureRestClientBuilder(final RestClientBuilder restClientBuilder, final Settings settings)
            throws IOException {
        final String requestTimeoutString = settings.get(CLIENT_RETRY_TIMEOUT);
        if (requestTimeoutString != null) {
            final TimeValue maxRetryTimeout = TimeValue.parseTimeValue(requestTimeoutString, CLIENT_RETRY_TIMEOUT);
            restClientBuilder.setMaxRetryTimeoutMillis(Math.toIntExact(maxRetryTimeout.getMillis()));
        }
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        if (socketTimeoutString != null) {
            final TimeValue socketTimeout = TimeValue.parseTimeValue(socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
            restClientBuilder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        }
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            restClientBuilder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }
}
