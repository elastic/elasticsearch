/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;

import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ClusterAlertsUtil}.
 */
public class ClusterAlertsUtilTests extends ESTestCase {

    private final ClusterService clusterService = mock(ClusterService.class);
    private final ClusterState clusterState = mock(ClusterState.class);
    private final MetaData metaData = mock(MetaData.class);
    private final String clusterUuid = randomAlphaOfLength(16);

    @Before
    public void setup() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(metaData.clusterUUID()).thenReturn(clusterUuid);
    }

    public void testWatchIdsAreAllUnique() {
        final List<String> watchIds = Arrays.asList(ClusterAlertsUtil.WATCH_IDS);

        assertThat(watchIds, hasSize(new HashSet<>(watchIds).size()));
    }

    public void testCreateUniqueWatchId() {
        final String watchId = randomFrom(ClusterAlertsUtil.WATCH_IDS);

        final String uniqueWatchId = ClusterAlertsUtil.createUniqueWatchId(clusterService, watchId);

        assertThat(uniqueWatchId, equalTo(clusterUuid + "_" + watchId));
    }

    public void testLoadWatch() {
        for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
            final String watch = ClusterAlertsUtil.loadWatch(clusterService, watchId);

            assertThat(watch, notNullValue());
            assertThat(watch, containsString(clusterUuid));
            assertThat(watch, containsString(watchId));
            assertThat(watch, containsString(String.valueOf(ClusterAlertsUtil.LAST_UPDATED_VERSION)));

            if ("elasticsearch_nodes".equals(watchId) == false) {
                assertThat(watch, containsString(clusterUuid + "_" + watchId));
            }

            // validate that it's well formed JSON
            assertThat(XContentHelper.convertToMap(XContentType.JSON.xContent(), watch, false), notNullValue());
        }
    }

    public void testLoadWatchFails() {
        expectThrows(RuntimeException.class, () -> ClusterAlertsUtil.loadWatch(clusterService, "watch-does-not-exist"));
    }

    public void testGetClusterAlertsBlacklistThrowsForUnknownWatchId() {
        final List<String> watchIds = Arrays.asList(ClusterAlertsUtil.WATCH_IDS);
        final List<String> blacklist = randomSubsetOf(watchIds);

        blacklist.add("fake1");

        if (randomBoolean()) {
            blacklist.add("fake2");

            if (rarely()) {
                blacklist.add("fake3");
            }
        }

        final Set<String> unknownIds = blacklist.stream().filter(id -> watchIds.contains(id) == false).collect(Collectors.toSet());
        final String unknownIdsString = String.join(", ", unknownIds);

        final SettingsException exception =
                expectThrows(SettingsException.class,
                             () -> ClusterAlertsUtil.getClusterAlertsBlacklist(createConfigWithBlacklist("_random", blacklist)));

        assertThat(exception.getMessage(),
                   equalTo("[xpack.monitoring.exporters._random.cluster_alerts.management.blacklist] contains unrecognized Cluster " +
                           "Alert IDs [" + unknownIdsString + "]"));
    }

    public void testGetClusterAlertsBlacklist() {
        final List<String> blacklist = randomSubsetOf(Arrays.asList(ClusterAlertsUtil.WATCH_IDS));

        assertThat(blacklist, equalTo(ClusterAlertsUtil.getClusterAlertsBlacklist(createConfigWithBlacklist("any", blacklist))));
    }

    private Exporter.Config createConfigWithBlacklist(final String name, final List<String> blacklist) {
        final Settings settings = Settings.builder()
                .putList("xpack.monitoring.exporters." + name + ".cluster_alerts.management.blacklist", blacklist)
                .build();
        final ClusterService clusterService = mock(ClusterService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);

        return new Exporter.Config(name, "local", settings, clusterService, licenseState);
    }

}
