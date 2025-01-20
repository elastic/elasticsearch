/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.krb5kdc.Krb5kDcContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test to demonstrate authentication against a real MIT Kerberos
 * instance.
 * <p>
 * Demonstrates login by keytab and login by password for given user principal
 * name using rest client.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class KerberosAuthenticationIT extends ESRestTestCase {
    private static final String ENABLE_KERBEROS_DEBUG_LOGS_KEY = "test.krb.debug";
    private static final String TEST_USER_WITH_KEYTAB_KEY = "peppa@BUILD.ELASTIC.CO";
    private static final String TEST_USER_WITH_PWD_KEY = "george@BUILD.ELASTIC.CO";
    private static final String TEST_USER_WITH_PWD_PASSWD_KEY = "dino_but_longer_than_14_chars";
    private static final String TEST_KERBEROS_REALM_NAME = "kerberos";

    public static Krb5kDcContainer krb5Fixture = new Krb5kDcContainer(Krb5kDcContainer.ProvisioningId.PEPPA);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        // force localhost IPv4 otherwise it is a chicken and egg problem where we need the keytab for the hostname when starting the
        // cluster but do not know the exact address that is first in the http ports file
        .setting("http.host", "127.0.0.1")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.realms.file.file1.order", "0")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        // Kerberos realm
        .setting("xpack.security.authc.realms.kerberos.kerberos.order", "1")
        .setting("xpack.security.authc.realms.kerberos.kerberos.keytab.path", "es.keytab")
        .setting("xpack.security.authc.realms.kerberos.kerberos.krb.debug", "true")
        .setting("xpack.security.authc.realms.kerberos.kerberos.remove_realm_name", "false")
        .systemProperty("java.security.krb5.conf", () -> krb5Fixture.getConfPath().toString())
        .systemProperty("sun.security.krb5.debug", "true")
        .user("test_admin", "x-pack-test-password")
        .user("test_kibana_user", "x-pack-test-password", "kibana_system", false)
        .configFile("es.keytab", Resource.fromFile(() -> krb5Fixture.getEsKeytab()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(krb5Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

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
            .toString(
                XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .array("roles", new String[] { "kerb_test" })
                    .field("enabled", true)
                    .startObject("rules")
                    .startArray("all")
                    .startObject()
                    .startObject("field")
                    .field("realm.name", TEST_KERBEROS_REALM_NAME)
                    .endObject()
                    .endObject()
                    .endArray() // "all"
                    .endObject() // "rules"
                    .endObject()
            );

        final Request request = new Request("POST", "/_security/role_mapping/kerberosrolemapping");
        request.setJsonEntity(json);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
    }

    public void testLoginByKeytab() throws IOException, PrivilegedActionException {
        final String keytabPath = krb5Fixture.getKeytab().toString();
        final boolean enabledDebugLogs = Boolean.parseBoolean(ENABLE_KERBEROS_DEBUG_LOGS_KEY);
        final SpnegoHttpClientConfigCallbackHandler callbackHandler = new SpnegoHttpClientConfigCallbackHandler(
            krb5Fixture.getPrincipal(),
            keytabPath,
            enabledDebugLogs
        );
        executeRequestAndVerifyResponse(krb5Fixture.getPrincipal(), callbackHandler);
    }

    public void testLoginByUsernamePassword() throws IOException, PrivilegedActionException {
        final String userPrincipalName = TEST_USER_WITH_PWD_KEY;
        final String password = TEST_USER_WITH_PWD_PASSWD_KEY;
        final boolean enabledDebugLogs = Boolean.parseBoolean(System.getProperty(ENABLE_KERBEROS_DEBUG_LOGS_KEY));
        final SpnegoHttpClientConfigCallbackHandler callbackHandler = new SpnegoHttpClientConfigCallbackHandler(
            userPrincipalName,
            new SecureString(password.toCharArray()),
            enabledDebugLogs
        );
        executeRequestAndVerifyResponse(userPrincipalName, callbackHandler);
    }

    public void testGetOauth2TokenInExchangeForKerberosTickets() throws PrivilegedActionException, GSSException, IOException {
        final String userPrincipalName = TEST_USER_WITH_PWD_KEY;
        final String password = TEST_USER_WITH_PWD_PASSWD_KEY;
        final boolean enabledDebugLogs = Boolean.parseBoolean(System.getProperty(ENABLE_KERBEROS_DEBUG_LOGS_KEY));
        final SpnegoHttpClientConfigCallbackHandler callbackHandler = new SpnegoHttpClientConfigCallbackHandler(
            userPrincipalName,
            new SecureString(password.toCharArray()),
            enabledDebugLogs
        );
        final String host = getClusterHosts().get(0).getHostName();
        final String kerberosTicket = callbackHandler.getBase64EncodedTokenForSpnegoHeader(host);

        final Request request = new Request("POST", "/_security/oauth2/token");
        String json = Strings.format("""
            { "grant_type" : "_kerberos", "kerberos_ticket" : "%s"}
            """, kerberosTicket);
        request.setJsonEntity(json);

        try (RestClient client = buildClientForUser("test_kibana_user")) {
            final Response response = client.performRequest(request);
            assertOK(response);
            final Map<String, Object> map = parseResponseAsMap(response.getEntity());
            assertThat(map.get("access_token"), notNullValue());
            assertThat(map.get("type"), is("Bearer"));
            assertThat(map.get("refresh_token"), notNullValue());
            final Object base64OutToken = map.get("kerberos_authentication_response_token");
            assertThat(base64OutToken, notNullValue());
            final String outToken = callbackHandler.handleResponse((String) base64OutToken);
            assertThat(outToken, is(nullValue()));
            assertThat(callbackHandler.isEstablished(), is(true));
        }
    }

    @Override
    @SuppressForbidden(reason = "SPNEGO relies on hostnames and we need to ensure host isn't a IP address")
    protected HttpHost buildHttpHost(String host, int port) {
        try {
            final InetAddress address = InetAddress.getByName(host);
            final String hostname = address.getCanonicalHostName();
            // InetAddress#getCanonicalHostName depends on the system configuration (e.g. /etc/hosts) to return the FQDN.
            // In case InetAddress cannot resolve the FQDN it will return the textual representation of the IP address.
            if (hostname.equals(address.getHostAddress())) {
                if (address.isLoopbackAddress()) {
                    // Fall-back and return "localhost" for loopback address if it's not resolved.
                    // This is safe because InetAddress implements a reverse fall-back to loopback address
                    // in case the resolution of "localhost" hostname fails.
                    return super.buildHttpHost("localhost", port);
                } else {
                    throw new IllegalStateException("failed to resolve [" + host + "] to FQDN");
                }
            } else {
                return super.buildHttpHost(hostname, port);
            }
        } catch (UnknownHostException e) {
            assumeNoException("failed to resolve host [" + host + "]", e);
        }
        throw new IllegalStateException("DNS not resolved and assume did not trip");
    }

    private void executeRequestAndVerifyResponse(
        final String userPrincipalName,
        final SpnegoHttpClientConfigCallbackHandler callbackHandler
    ) throws PrivilegedActionException, IOException {
        final Request request = new Request("GET", "/_security/_authenticate");
        try (RestClient restClient = buildRestClientForKerberos(callbackHandler)) {
            final AccessControlContext accessControlContext = AccessController.getContext();
            final LoginContext lc = callbackHandler.login();
            Response response = SpnegoHttpClientConfigCallbackHandler.doAsPrivilegedWrapper(
                lc.getSubject(),
                (PrivilegedExceptionAction<Response>) () -> {
                    return restClient.performRequest(request);
                },
                accessControlContext
            );

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

    private RestClient buildClientForUser(String user) throws IOException {
        final String token = basicAuthHeaderValue(user, new SecureString("x-pack-test-password".toCharArray()));
        Settings settings = Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        final HttpHost[] hosts = getClusterHosts().toArray(new HttpHost[getClusterHosts().size()]);
        return buildClient(settings, hosts);
    }

    private RestClient buildRestClientForKerberos(final SpnegoHttpClientConfigCallbackHandler callbackHandler) throws IOException {
        final Settings settings = restAdminSettings();
        final HttpHost[] hosts = getClusterHosts().toArray(new HttpHost[getClusterHosts().size()]);

        final RestClientBuilder restClientBuilder = RestClient.builder(hosts);
        configureRestClientBuilder(restClientBuilder, settings);
        restClientBuilder.setHttpClientConfigCallback(callbackHandler);
        return restClientBuilder.build();
    }

    private static void configureRestClientBuilder(final RestClientBuilder restClientBuilder, final Settings settings) {
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
