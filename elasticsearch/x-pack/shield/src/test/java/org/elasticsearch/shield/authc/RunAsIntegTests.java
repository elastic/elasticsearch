/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class RunAsIntegTests extends ShieldIntegTestCase {
    static final String RUN_AS_USER = "run_as_user";
    static final String TRANSPORT_CLIENT_USER = "transport_user";
    static final String ROLES =
            "transport_client:\n" +
            "  cluster: [ 'cluster:monitor/nodes/liveness' ]\n" +
            "run_as_role:\n" +
            "  run_as: [ '" + ShieldSettingsSource.DEFAULT_USER_NAME + "', 'idontexist' ]\n";

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    public boolean sslTransportEnabled() {
        return false;
    }

    @Override
    public String configRoles() {
        return ROLES + super.configRoles();
    }

    @Override
    public String configUsers() {
        return super.configUsers()
                + RUN_AS_USER + ":" + ShieldSettingsSource.DEFAULT_PASSWORD_HASHED + "\n"
                + TRANSPORT_CLIENT_USER + ":" + ShieldSettingsSource.DEFAULT_PASSWORD_HASHED + "\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles()
                + "run_as_role:" + RUN_AS_USER + "\n"
                + "transport_client:" + TRANSPORT_CLIENT_USER;
    }

    public void testUserImpersonation() throws Exception {
        try (TransportClient client = getTransportClient(Settings.builder()
                .put(Security.USER_SETTING.getKey(), TRANSPORT_CLIENT_USER + ":" + ShieldSettingsSource.DEFAULT_PASSWORD).build())) {
            //ensure the client can connect
            awaitBusy(() -> {
                return client.connectedNodes().size() > 0;
            });

            // make sure the client can't get health
            try {
                client.admin().cluster().prepareHealth().get();
                fail("the client user should not have privileges to get the health");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unauthorized"));
            }

            // let's run as without authorization
            try {
                Map<String, String> headers = Collections.singletonMap(InternalAuthenticationService.RUN_AS_USER_HEADER,
                        ShieldSettingsSource.DEFAULT_USER_NAME);
                client.filterWithHeader(headers)
                        .admin().cluster().prepareHealth().get();
                fail("run as should be unauthorized for the transport client user");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unauthorized"));
                assertThat(e.getMessage(), containsString("run as"));
            }

            Map<String, String> headers = new HashMap<>();
            headers.put("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                    new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())));
            headers.put(InternalAuthenticationService.RUN_AS_USER_HEADER, ShieldSettingsSource.DEFAULT_USER_NAME);
            // lets set the user
            ClusterHealthResponse response = client.filterWithHeader(headers).admin().cluster().prepareHealth().get();
            assertThat(response.isTimedOut(), is(false));
        }
    }

    public void testUserImpersonationUsingHttp() throws Exception {
        // use the transport client user and try to run as
        HttpResponse response = httpClient().method("GET")
                .path("/_nodes")
                .addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(TRANSPORT_CLIENT_USER,
                        SecuredStringTests.build(ShieldSettingsSource.DEFAULT_PASSWORD)))
                .addHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, ShieldSettingsSource.DEFAULT_USER_NAME)
                .execute();
        assertThat(response.getStatusCode(), is(403));

        //the run as user shouldn't have access to the nodes api
        response = httpClient().method("GET")
                .path("/_nodes")
                .addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        SecuredStringTests.build(ShieldSettingsSource.DEFAULT_PASSWORD)))
                .execute();
        assertThat(response.getStatusCode(), is(403));

        // but when running as a different user it should work
        response = httpClient().method("GET")
                .path("/_nodes")
                .addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        SecuredStringTests.build(ShieldSettingsSource.DEFAULT_PASSWORD)))
                .addHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, ShieldSettingsSource.DEFAULT_USER_NAME)
                .execute();
        assertThat(response.getStatusCode(), is(200));
    }

    public void testEmptyUserImpersonationHeader() throws Exception {
        try (TransportClient client = getTransportClient(Settings.builder()
                .put(Security.USER_SETTING.getKey(), TRANSPORT_CLIENT_USER + ":" + ShieldSettingsSource.DEFAULT_PASSWORD).build())) {
            //ensure the client can connect
            awaitBusy(() -> {
                return client.connectedNodes().size() > 0;
            });

            try {
                Map<String, String> headers = new HashMap<>();
                headers.put("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())));
                headers.put(InternalAuthenticationService.RUN_AS_USER_HEADER, "");

                client.filterWithHeader(headers).admin().cluster().prepareHealth().get();
                fail("run as header should not be allowed to be empty");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unable to authenticate"));
            }
        }
    }

    public void testEmptyHeaderUsingHttp() throws Exception {
        HttpResponse response = httpClient().method("GET")
                .path("/_nodes")
                .addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        SecuredStringTests.build(ShieldSettingsSource.DEFAULT_PASSWORD)))
                .addHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "")
                .execute();
        assertThat(response.getStatusCode(), is(401));
    }

    public void testNonExistentRunAsUser() throws Exception {
        try (TransportClient client = getTransportClient(Settings.builder()
                .put(Security.USER_SETTING.getKey(), TRANSPORT_CLIENT_USER + ":" + ShieldSettingsSource.DEFAULT_PASSWORD).build())) {
            //ensure the client can connect
            awaitBusy(() -> {
                return client.connectedNodes().size() > 0;
            });

            try {
                Map<String, String> headers = new HashMap<>();
                headers.put("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())));
                headers.put(InternalAuthenticationService.RUN_AS_USER_HEADER, "idontexist");

                client.filterWithHeader(headers).admin().cluster().prepareHealth().get();
                fail("run as header should not accept non-existent users");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.getMessage(), containsString("unauthorized"));
            }
        }
    }

    public void testNonExistentRunAsUserUsingHttp() throws Exception {
        HttpResponse response = httpClient().method("GET")
                .path("/_nodes")
                .addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER,
                        SecuredStringTests.build(ShieldSettingsSource.DEFAULT_PASSWORD)))
                .addHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "idontexist")
                .execute();
        assertThat(response.getStatusCode(), is(403));
    }

    // build our own here to better mimic an actual client...
    TransportClient getTransportClient(Settings extraSettings) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
        NodeInfo[] nodes = nodeInfos.getNodes();
        assertTrue(nodes.length > 0);
        TransportAddress publishAddress = randomFrom(nodes).getTransport().address().publishAddress();
        String clusterName = nodeInfos.getClusterNameAsString();

        Settings settings = Settings.builder()
                .put(extraSettings)
                .put("cluster.name", clusterName)
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), false)
                .build();

        return TransportClient.builder()
                .settings(settings)
                .addPlugin(XPackPlugin.class)
                .build()
                .addTransportAddress(publishAddress);
    }
}
