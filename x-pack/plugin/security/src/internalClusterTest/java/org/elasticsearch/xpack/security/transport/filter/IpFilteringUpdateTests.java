/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;

import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class IpFilteringUpdateTests extends SecurityIntegTestCase {

    private static final int NUMBER_OF_CLIENT_PORTS = Constants.WINDOWS ? 300 : 100;

    private static int randomClientPort;

    private final boolean httpEnabled = randomBoolean();

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49152, 65535 - NUMBER_OF_CLIENT_PORTS);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return httpEnabled == false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort + NUMBER_OF_CLIENT_PORTS);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
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

        updateClusterSettings(
            Settings.builder()
                .put("xpack.security.transport.filter.allow", "127.0.0.1")
                .put("xpack.security.transport.filter.deny", "127.0.0.8")
        );
        assertConnectionRejected("default", "127.0.0.8");

        updateClusterSettings(
            Settings.builder()
                .putList("xpack.security.http.filter.allow", "127.0.0.1")
                .putList("xpack.security.http.filter.deny", "127.0.0.8")
        );
        assertConnectionRejected("default", "127.0.0.8");
        assertConnectionRejected(".http", "127.0.0.8");

        updateClusterSettings(
            Settings.builder()
                .put("transport.profiles.client.xpack.security.filter.allow", "127.0.0.1")
                .put("transport.profiles.client.xpack.security.filter.deny", "127.0.0.8")
        );
        assertConnectionRejected("default", "127.0.0.8");
        assertConnectionRejected(".http", "127.0.0.8");
        assertConnectionRejected("client", "127.0.0.8");

        // check that all is in cluster state
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        assertThat(clusterState.metadata().settings().get("xpack.security.transport.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metadata().settings().get("xpack.security.transport.filter.deny"), is("127.0.0.8"));
        assertEquals(Arrays.asList("127.0.0.1"), clusterState.metadata().settings().getAsList("xpack.security.http.filter.allow"));
        assertEquals(Arrays.asList("127.0.0.8"), clusterState.metadata().settings().getAsList("xpack.security.http.filter.deny"));
        assertThat(clusterState.metadata().settings().get("transport.profiles.client.xpack.security.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metadata().settings().get("transport.profiles.client.xpack.security.filter.deny"), is("127.0.0.8"));

        // now disable ip filtering dynamically and make sure nothing is rejected
        updateClusterSettings(
            Settings.builder()
                .put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), false)
                .put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING.getKey(), true)
        );
        assertConnectionAccepted("default", "127.0.0.8");
        assertConnectionAccepted("client", "127.0.0.8");

        // disabling should not have any effect on the cluster state settings
        clusterState = clusterAdmin().prepareState().get().getState();
        assertThat(clusterState.metadata().settings().get("xpack.security.transport.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metadata().settings().get("xpack.security.transport.filter.deny"), is("127.0.0.8"));
        assertEquals(Arrays.asList("127.0.0.1"), clusterState.metadata().settings().getAsList("xpack.security.http.filter.allow"));
        assertEquals(Arrays.asList("127.0.0.8"), clusterState.metadata().settings().getAsList("xpack.security.http.filter.deny"));
        assertThat(clusterState.metadata().settings().get("transport.profiles.client.xpack.security.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metadata().settings().get("transport.profiles.client.xpack.security.filter.deny"), is("127.0.0.8"));

        // now also disable for HTTP
        if (httpEnabled) {
            assertConnectionRejected(".http", "127.0.0.8");
            updateClusterSettings(Settings.builder().put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING.getKey(), false));
            assertConnectionAccepted(".http", "127.0.0.8");
        }
    }

    public void testThatInvalidDynamicIpFilterConfigurationIsRejected() {
        final Settings.Builder initialSettingsBuilder = Settings.builder();
        if (randomBoolean()) {
            initialSettingsBuilder.put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), randomBoolean());
        }
        if (randomBoolean()) {
            initialSettingsBuilder.put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING.getKey(), randomBoolean());
        }
        if (initialSettingsBuilder.build().isEmpty() == false) {
            updateClusterSettings(initialSettingsBuilder);
        }

        final String invalidValue = "http://";

        for (final String settingPrefix : new String[] {
            "xpack.security.transport.filter",
            "xpack.security.http.filter",
            "transport.profiles.default.xpack.security.filter",
            "transport.profiles.anotherprofile.xpack.security.filter" }) {
            for (final String settingSuffix : new String[] { "allow", "deny" }) {
                final String settingName = settingPrefix + "." + settingSuffix;
                final Settings settings = Settings.builder().put(settingName, invalidValue).build();
                assertThat(
                    settingName,
                    expectThrows(
                        IllegalArgumentException.class,
                        settingName,
                        () -> clusterAdmin().prepareUpdateSettings().setPersistentSettings(settings).get()
                    ).getMessage(),
                    allOf(containsString("invalid IP filter"), containsString(invalidValue))
                );
            }
        }
    }

    // issue #762, occurred because in the above test we use HTTP and transport
    public void testThatDisablingIpFilterWorksAsExpected() throws Exception {
        updateClusterSettings(Settings.builder().put("xpack.security.transport.filter.deny", "127.0.0.8"));
        assertConnectionRejected("default", "127.0.0.8");

        updateClusterSettings(Settings.builder().put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), false));
        assertConnectionAccepted("default", "127.0.0.8");
    }

    public void testThatDisablingIpFilterForProfilesWorksAsExpected() throws Exception {
        updateClusterSettings(Settings.builder().put("transport.profiles.client.xpack.security.filter.deny", "127.0.0.8"));
        assertConnectionRejected("client", "127.0.0.8");

        updateClusterSettings(Settings.builder().put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), false));
        assertConnectionAccepted("client", "127.0.0.8");
    }

    private void assertConnectionAccepted(String profile, String host) throws UnknownHostException {
        // HTTP is not applied if disabled
        if (httpEnabled == false && IPFilter.HTTP_PROFILE_NAME.equals(profile)) {
            return;
        }

        IPFilter ipFilter = internalCluster().getDataNodeInstance(IPFilter.class);
        String message = Strings.format("Expected allowed connection for profile %s against host %s", profile, host);
        assertThat(message, ipFilter.accept(profile, new InetSocketAddress(InetAddress.getByName(host), 0)), is(true));
    }

    private void assertConnectionRejected(String profile, String host) throws UnknownHostException {
        // HTTP is not applied if disabled
        if (httpEnabled == false && IPFilter.HTTP_PROFILE_NAME.equals(profile)) {
            return;
        }

        IPFilter ipFilter = internalCluster().getDataNodeInstance(IPFilter.class);
        String message = Strings.format("Expected rejection for profile %s against host %s", profile, host);
        assertThat(message, ipFilter.accept(profile, new InetSocketAddress(InetAddress.getByName(host), 0)), is(false));
    }
}
