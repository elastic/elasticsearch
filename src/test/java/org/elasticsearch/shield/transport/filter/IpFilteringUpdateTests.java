/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = TEST, numDataNodes = 1)
public class IpFilteringUpdateTests extends ShieldIntegrationTest {

    private boolean httpEnabled = false;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        httpEnabled = randomBoolean();

        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, httpEnabled)
                .put("shield.transport.filter.deny", "127.0.0.200")
                .build();
    }

    @Test
    public void testThatIpFilterConfigurationCanBeChangedDynamically() throws Exception {
        // ensure this did not get overwritten by the listener during startup
        assertConnectionRejected("default", "127.0.0.200");

        // allow all by default
        assertConnectionAccepted("default", "127.0.0.8");
        assertConnectionAccepted(".http", "127.0.0.8");
        assertConnectionAccepted("client", "127.0.0.8");

        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "127.0.0.1")
                .put("shield.transport.filter.deny", "127.0.0.8")
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        assertConnectionRejected("default", "127.0.0.8");

        settings = settingsBuilder()
                .putArray("shield.http.filter.allow", "127.0.0.1")
                .putArray("shield.http.filter.deny", "127.0.0.8")
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        assertConnectionRejected("default", "127.0.0.8");
        assertConnectionRejected(".http", "127.0.0.8");

        settings = settingsBuilder()
                .put("transport.profiles.client.shield.filter.allow", "127.0.0.1")
                .put("transport.profiles.client.shield.filter.deny", "127.0.0.8")
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        assertConnectionRejected("default", "127.0.0.8");
        assertConnectionRejected(".http", "127.0.0.8");
        assertConnectionRejected("client", "127.0.0.8");

        // check that all is in cluster state
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metaData().settings().get("shield.transport.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("shield.transport.filter.deny"), is("127.0.0.8"));
        assertThat(clusterState.metaData().persistentSettings().get("shield.http.filter.allow.0"), is("127.0.0.1"));
        assertThat(clusterState.metaData().persistentSettings().get("shield.http.filter.deny.0"), is("127.0.0.8"));
        assertThat(clusterState.metaData().settings().get("transport.profiles.client.shield.filter.allow"), is("127.0.0.1"));
        assertThat(clusterState.metaData().settings().get("transport.profiles.client.shield.filter.deny"), is("127.0.0.8"));

        // now disable ip filtering dynamically and make sure nothing is rejected
        settings = settingsBuilder()
                .put(IPFilter.IP_FILTER_ENABLED_SETTING, false)
                .put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING, true)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        assertConnectionAccepted("default", "127.0.0.8");
        assertConnectionAccepted("client", "127.0.0.8");

        // now also disable for HTTP
        assertConnectionRejected(".http", "127.0.0.8");
        settings = settingsBuilder()
                .put(IPFilter.IP_FILTER_ENABLED_HTTP_SETTING, false)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
        assertConnectionAccepted(".http", "127.0.0.8");
    }

    private void assertConnectionAccepted(String profile, String host) throws UnknownHostException {
        // HTTP is not applied if disabled
        if (!httpEnabled && IPFilter.HTTP_PROFILE_NAME.equals(profile)) {
            return;
        }

        IPFilter ipFilter = internalCluster().getInstance(IPFilter.class);
        String message = String.format(Locale.ROOT, "Expected allowed connection for profile %s against host %s", profile, host);
        assertThat(message, ipFilter.accept(profile, InetAddress.getByName(host)), is(true));
    }

    private void assertConnectionRejected(String profile, String host) throws UnknownHostException {
        // HTTP is not applied if disabled
        if (!httpEnabled && IPFilter.HTTP_PROFILE_NAME.equals(profile)) {
            return;
        }

        IPFilter ipFilter = internalCluster().getInstance(IPFilter.class);
        String message = String.format(Locale.ROOT, "Expected rejection for profile %s against host %s", profile, host);
        assertThat(message, ipFilter.accept(profile, InetAddress.getByName(host)), is(false));
    }
}
