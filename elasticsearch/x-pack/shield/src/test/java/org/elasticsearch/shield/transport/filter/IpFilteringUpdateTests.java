/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = TEST, numDataNodes = 1)
public class IpFilteringUpdateTests extends ShieldIntegTestCase {

    private static int randomClientPort;

    private final boolean httpEnabled = randomBoolean();

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), httpEnabled)
                .put("xpack.security.transport.filter.deny", "127.0.0.200")
                .put("transport.profiles.client.port", randomClientPortRange)
                .build();
    }

    public void testThatIpFilterConfigurationCanBeChangedDynamically() throws Exception {
        // ensure this did not get overwritten by the listener during startup
        assertConnectionRejected("default", "127.0.0.200");

        // allow all by default
        assertConnectionAccepted("default", "127.0.0.8");
        assertConnectionAccepted(".http", "127.0.0.8");
        assertConnectionAccepted("client", "127.0.0.8");

        Settings settings = Settings.builder()
                .put("xpack.security.transport.filter.allow", "127.0.0.1")
                .put("xpack.security.transport.filter.deny", "127.0.0.8")
                .build();
        updateSettings(settings);
        assertConnectionRejected("default", "127.0.0.8");

        settings = Settings.builder()
                .putArray("xpack.security.http.filter.allow", "127.0.0.1")
                .putArray("xpack.security.http.filter.deny", "127.0.0.8")
                .build();
        updateSettings(settings);
        assertConnectionRejected("default", "127.0.0.8");
        assertConnectionRejected(".http", "127.0.0.8");

        settings = Settings.builder()
                .put("transport.profiles.client.xpack.security.filter.allow", "127.0.0.1")
                .put("transport.profiles.client.xpack.security.filter.deny", "127.0.0.8")
                .build();
        updateSettings(settings);
        assertConnectionRejected("default", "127.0.0.8");
        assertConnectionRejected(".http", "127.0.0.8");
        assertConnectionRejected("client", "127.0.0.8");

        // check that all is in cluster state
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metaData().settings().get("xpack.security.transport.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("xpack.security.transport.filter.deny"), is("127.0.0.8"));
        assertThat(clusterState.metaData().settings().get("xpack.security.http.filter.allow.0"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("xpack.security.http.filter.deny.0"), is("127.0.0.8"));
        assertThat(clusterState.metaData().settings().get("transport.profiles.client.xpack.security.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("transport.profiles.client.xpack.security.filter.deny"), is("127.0.0.8"));

        // now disable ip filtering dynamically and make sure nothing is rejected
        settings = Settings.builder()
                .put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), false)
                .put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING.getKey(), true)
                .build();
        updateSettings(settings);
        assertConnectionAccepted("default", "127.0.0.8");
        assertConnectionAccepted("client", "127.0.0.8");

        // disabling should not have any effect on the cluster state settings
        clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metaData().settings().get("xpack.security.transport.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("xpack.security.transport.filter.deny"), is("127.0.0.8"));
        assertThat(clusterState.metaData().settings().get("xpack.security.http.filter.allow.0"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("xpack.security.http.filter.deny.0"), is("127.0.0.8"));
        assertThat(clusterState.metaData().settings().get("transport.profiles.client.xpack.security.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("transport.profiles.client.xpack.security.filter.deny"), is("127.0.0.8"));

        // now also disable for HTTP
        if (httpEnabled) {
            assertConnectionRejected(".http", "127.0.0.8");
            settings = Settings.builder()
                    .put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING.getKey(), false)
                    .build();
            // as we permanently switch between persistent and transient settings, just set both here to make sure we overwrite
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
            assertConnectionAccepted(".http", "127.0.0.8");
        }
    }

    // issue #762, occured because in the above test we use HTTP and transport
    public void testThatDisablingIpFilterWorksAsExpected() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.transport.filter.deny", "127.0.0.8")
                .build();
        updateSettings(settings);
        assertConnectionRejected("default", "127.0.0.8");

        settings = Settings.builder()
                .put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), false)
                .build();
        updateSettings(settings);
        assertConnectionAccepted("default", "127.0.0.8");
    }

    public void testThatDisablingIpFilterForProfilesWorksAsExpected() throws Exception {
        Settings settings = Settings.builder()
                .put("transport.profiles.client.xpack.security.filter.deny", "127.0.0.8")
                .build();
        updateSettings(settings);
        assertConnectionRejected("client", "127.0.0.8");

        settings = Settings.builder()
                .put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), false)
                .build();
        updateSettings(settings);
        assertConnectionAccepted("client", "127.0.0.8");
    }


    private void updateSettings(Settings settings) {
        if (randomBoolean()) {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        } else {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        }
    }

    private void assertConnectionAccepted(String profile, String host) throws UnknownHostException {
        // HTTP is not applied if disabled
        if (!httpEnabled && IPFilter.HTTP_PROFILE_NAME.equals(profile)) {
            return;
        }

        IPFilter ipFilter = internalCluster().getDataNodeInstance(IPFilter.class);
        String message = String.format(Locale.ROOT, "Expected allowed connection for profile %s against host %s", profile, host);
        assertThat(message, ipFilter.accept(profile, InetAddress.getByName(host)), is(true));
    }

    private void assertConnectionRejected(String profile, String host) throws UnknownHostException {
        // HTTP is not applied if disabled
        if (!httpEnabled && IPFilter.HTTP_PROFILE_NAME.equals(profile)) {
            return;
        }

        IPFilter ipFilter = internalCluster().getDataNodeInstance(IPFilter.class);
        String message = String.format(Locale.ROOT, "Expected rejection for profile %s against host %s", profile, host);
        assertThat(message, ipFilter.accept(profile, InetAddress.getByName(host)), is(false));
    }
}
