/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.TestXPackTransportClient;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class RunAsIntegTests extends SecurityIntegTestCase {

    private static final String RUN_AS_USER = "run_as_user";
    private static final String TRANSPORT_CLIENT_USER = "transport_user";
    private static final String ROLES =
            "run_as_role:\n" +
            "  run_as: [ '" + SecuritySettingsSource.TEST_USER_NAME + "', 'idontexist' ]\n";

    // indicates whether the RUN_AS_USER that is being authenticated is also a superuser
    private static boolean runAsHasSuperUserRole;

    @BeforeClass
    public static void configureRunAsHasSuperUserRole() {
        runAsHasSuperUserRole = randomBoolean();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    public String configRoles() {
        return ROLES + super.configRoles();
    }

    @Override
    public String configUsers() {
        return super.configUsers()
                + RUN_AS_USER + ":" + SecuritySettingsSource.TEST_PASSWORD_HASHED + "\n"
                + TRANSPORT_CLIENT_USER + ":" + SecuritySettingsSource.TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    public String configUsersRoles() {
        String roles = super.configUsersRoles()
                + "run_as_role:" + RUN_AS_USER + "\n"
                + "transport_client:" + TRANSPORT_CLIENT_USER;
        if (runAsHasSuperUserRole) {
            roles = roles + "\n"
                    + "superuser:" + RUN_AS_USER;
        }
        return roles;
    }

    @Override
    protected boolean transportSSLEnabled() {
        return false;
    }

    public void testUserImpersonation() throws Exception {
        try (TransportClient client = getTransportClient(Settings.builder()
                .put(SecurityField.USER_SETTING.getKey(), TRANSPORT_CLIENT_USER + ":" +
                     SecuritySettingsSourceField.TEST_PASSWORD).build())) {
            //ensure the client can connect
            assertBusy(() -> assertThat(client.connectedNodes().size(), greaterThan(0)));

            // make sure the client can't get health
            try {
                client.admin().cluster().prepareHealth().get();
                fail("the client user should not have privileges to get the health");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unauthorized"));
            }

            // let's run as without authorization
            try {
                Map<String, String> headers = Collections.singletonMap(AuthenticationServiceField.RUN_AS_USER_HEADER,
                        SecuritySettingsSource.TEST_USER_NAME);
                client.filterWithHeader(headers)
                        .admin().cluster().prepareHealth().get();
                fail("run as should be unauthorized for the transport client user");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unauthorized"));
                assertThat(e.getMessage(), containsString("run as"));
            }

            Map<String, String> headers = new HashMap<>();
            headers.put("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                    new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
            headers.put(AuthenticationServiceField.RUN_AS_USER_HEADER, SecuritySettingsSource.TEST_USER_NAME);
            // lets set the user
            ClusterHealthResponse response = client.filterWithHeader(headers).admin().cluster().prepareHealth().get();
            assertThat(response.isTimedOut(), is(false));
        }
    }

    public void testUserImpersonationUsingHttp() throws Exception {
        // use the transport client user and try to run as
        try {
            Request request = new Request("GET", "/_nodes");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(TRANSPORT_CLIENT_USER, TEST_PASSWORD_SECURE_STRING));
            options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, SecuritySettingsSource.TEST_USER_NAME);
            request.setOptions(options);
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch(ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }

        if (runAsHasSuperUserRole == false) {
            try {
                //the run as user shouldn't have access to the nodes api
                Request request = new Request("GET", "/_nodes");
                RequestOptions.Builder options = request.getOptions().toBuilder();
                options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING));
                request.setOptions(options);
                getRestClient().performRequest(request);
                fail("request should have failed");
            } catch (ResponseException e) {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
            }
        }

        // but when running as a different user it should work
        getRestClient().performRequest(requestForUserRunAsUser(SecuritySettingsSource.TEST_USER_NAME));
    }

    public void testEmptyUserImpersonationHeader() throws Exception {
        try (TransportClient client = getTransportClient(Settings.builder()
                .put(SecurityField.USER_SETTING.getKey(), TRANSPORT_CLIENT_USER + ":"
                     + SecuritySettingsSourceField.TEST_PASSWORD).build())) {
            //ensure the client can connect
            awaitBusy(() -> {
                return client.connectedNodes().size() > 0;
            });

            try {
                Map<String, String> headers = new HashMap<>();
                headers.put("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
                headers.put(AuthenticationServiceField.RUN_AS_USER_HEADER, "");

                client.filterWithHeader(headers).admin().cluster().prepareHealth().get();
                fail("run as header should not be allowed to be empty");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unable to authenticate"));
            }
        }
    }

    public void testEmptyHeaderUsingHttp() throws Exception {
        try {
            getRestClient().performRequest(requestForUserRunAsUser(""));
            fail("request should have failed");
        } catch(ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        }
    }

    public void testNonExistentRunAsUser() throws Exception {
        try (TransportClient client = getTransportClient(Settings.builder()
                .put(SecurityField.USER_SETTING.getKey(), TRANSPORT_CLIENT_USER + ":" +
                     SecuritySettingsSourceField.TEST_PASSWORD).build())) {
            //ensure the client can connect
            awaitBusy(() -> {
                return client.connectedNodes().size() > 0;
            });

            try {
                Map<String, String> headers = new HashMap<>();
                headers.put("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
                headers.put(AuthenticationServiceField.RUN_AS_USER_HEADER, "idontexist");

                client.filterWithHeader(headers).admin().cluster().prepareHealth().get();
                fail("run as header should not accept non-existent users");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unauthorized"));
            }
        }
    }

    public void testNonExistentRunAsUserUsingHttp() throws Exception {
        try {
            getRestClient().performRequest(requestForUserRunAsUser("idontexist"));
            fail("request should have failed");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    private static Request requestForUserRunAsUser(String user) {
        Request request = new Request("GET", "/_nodes");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING));
        options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, user);
        request.setOptions(options);
        return request;
    }

    // build our own here to better mimic an actual client...
    TransportClient getTransportClient(Settings extraSettings) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
        List<NodeInfo> nodes = nodeInfos.getNodes();
        assertTrue(nodes.isEmpty() == false);
        TransportAddress publishAddress = randomFrom(nodes).getTransport().address().publishAddress();
        String clusterName = nodeInfos.getClusterName().value();

        Settings settings = Settings.builder()
                .put(extraSettings)
                .put("cluster.name", clusterName)
                .build();

        return new TestXPackTransportClient(settings, LocalStateSecurity.class)
                .addTransportAddress(publishAddress);
    }
}
