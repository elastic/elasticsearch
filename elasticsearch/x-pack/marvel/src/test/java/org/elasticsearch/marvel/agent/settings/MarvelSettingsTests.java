/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

//test is just too slow, please fix it to not be sleep-based
//@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class MarvelSettingsTests extends MarvelIntegTestCase {
    private final TimeValue interval = newRandomTimeValue();
    private final TimeValue indexStatsTimeout = newRandomTimeValue();
    private final TimeValue indicesStatsTimeout = newRandomTimeValue();
    private final String[] indices = randomStringArray();
    private final TimeValue clusterStateTimeout = newRandomTimeValue();
    private final TimeValue clusterStatsTimeout = newRandomTimeValue();
    private final TimeValue recoveryTimeout = newRandomTimeValue();
    private final Boolean recoveryActiveOnly = randomBoolean();
    private final String[] collectors = randomStringArray();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(marvelSettings())
                .build();
    }

    private Settings marvelSettings() {
        return Settings.builder()
                .put(MonitoringSettings.INTERVAL.getKey(), interval)
                .put(MonitoringSettings.INDEX_STATS_TIMEOUT.getKey(), indexStatsTimeout)
                .put(MonitoringSettings.INDICES_STATS_TIMEOUT.getKey(), indicesStatsTimeout)
                .putArray(MonitoringSettings.INDICES.getKey(), indices)
                .put(MonitoringSettings.CLUSTER_STATE_TIMEOUT.getKey(), clusterStateTimeout)
                .put(MonitoringSettings.CLUSTER_STATS_TIMEOUT.getKey(), clusterStatsTimeout)
                .put(MonitoringSettings.INDEX_RECOVERY_TIMEOUT.getKey(), recoveryTimeout)
                .put(MonitoringSettings.INDEX_RECOVERY_ACTIVE_ONLY.getKey(), recoveryActiveOnly)
                .putArray(MonitoringSettings.COLLECTORS.getKey(), collectors)
                .build();
    }

    public void testMarvelSettings() throws Exception {
        logger.info("--> testing monitoring settings service initialization");
        for (final MonitoringSettings monitoringSettings : internalCluster().getInstances(MonitoringSettings.class)) {
            assertThat(monitoringSettings.indexStatsTimeout().millis(), equalTo(indexStatsTimeout.millis()));
            assertThat(monitoringSettings.indicesStatsTimeout().millis(), equalTo(indicesStatsTimeout.millis()));
            assertArrayEquals(monitoringSettings.indices(), indices);
            assertThat(monitoringSettings.clusterStateTimeout().millis(), equalTo(clusterStateTimeout.millis()));
            assertThat(monitoringSettings.clusterStatsTimeout().millis(), equalTo(clusterStatsTimeout.millis()));
            assertThat(monitoringSettings.recoveryTimeout().millis(), equalTo(recoveryTimeout.millis()));
            assertThat(monitoringSettings.recoveryActiveOnly(), equalTo(recoveryActiveOnly));
        }

        for (final AgentService service : internalCluster().getInstances(AgentService.class)) {
            assertThat(service.getSamplingInterval().millis(), equalTo(interval.millis()));
            assertArrayEquals(service.collectors(), collectors);

        }

        logger.info("--> testing monitoring dynamic settings update");
        Settings.Builder transientSettings = Settings.builder();
        final Setting[] marvelSettings = new Setting[] {
                MonitoringSettings.INDICES,
        MonitoringSettings.INTERVAL,
        MonitoringSettings.INDEX_RECOVERY_TIMEOUT,
        MonitoringSettings.INDEX_STATS_TIMEOUT,
        MonitoringSettings.INDICES_STATS_TIMEOUT,
        MonitoringSettings.INDEX_RECOVERY_ACTIVE_ONLY,
        MonitoringSettings.COLLECTORS,
        MonitoringSettings.CLUSTER_STATE_TIMEOUT,
        MonitoringSettings.CLUSTER_STATS_TIMEOUT };
        for (Setting<?> setting : marvelSettings) {
            if (setting.isDynamic()) {
                Object updated = null;
                if (setting.get(Settings.EMPTY) instanceof TimeValue) {
                    updated = newRandomTimeValue();
                    transientSettings.put(setting.getKey(), updated);
                } else if (setting.get(Settings.EMPTY) instanceof Boolean) {
                    updated = randomBoolean();
                    transientSettings.put(setting.getKey(), updated);
                } else if (setting.get(Settings.EMPTY) instanceof List) {
                    updated = randomStringArray();
                    transientSettings.putArray(setting.getKey(), (String[]) updated);
                } else {
                    fail("unknown dynamic setting [" + setting + "]");
                }
            }
        }

        logger.error("--> updating settings");
        final Settings updatedSettings = transientSettings.build();
        assertAcked(prepareRandomUpdateSettings(updatedSettings).get());

        logger.error("--> checking that the value has been correctly updated on all monitoring settings services");
        for (Setting<?> setting : marvelSettings) {
            if (setting.isDynamic() == false) {
                continue;
            }
            if (setting == MonitoringSettings.INTERVAL) {
                for (final AgentService service : internalCluster().getInstances(AgentService.class)) {
                    assertEquals(service.getSamplingInterval(), setting.get(updatedSettings));
                }
            } else {
                for (final MonitoringSettings monitoringSettings1 : internalCluster().getInstances(MonitoringSettings.class)) {
                    if (setting == MonitoringSettings.INDEX_STATS_TIMEOUT) {
                        assertEquals(monitoringSettings1.indexStatsTimeout(), setting.get(updatedSettings));
                    } else if (setting == MonitoringSettings.INDICES_STATS_TIMEOUT) {
                        assertEquals(monitoringSettings1.indicesStatsTimeout(), setting.get(updatedSettings));
                    } else if (setting == MonitoringSettings.CLUSTER_STATS_TIMEOUT) {
                        assertEquals(monitoringSettings1.clusterStatsTimeout(), setting.get(updatedSettings));
                    } else if (setting == MonitoringSettings.CLUSTER_STATE_TIMEOUT) {
                        assertEquals(monitoringSettings1.clusterStateTimeout(), setting.get(updatedSettings));
                    } else if (setting == MonitoringSettings.INDEX_RECOVERY_TIMEOUT) {
                        assertEquals(monitoringSettings1.recoveryTimeout(), setting.get(updatedSettings));
                    } else if (setting == MonitoringSettings.INDEX_RECOVERY_ACTIVE_ONLY) {
                        assertEquals(Boolean.valueOf(monitoringSettings1.recoveryActiveOnly()), setting.get(updatedSettings));
                    } else if (setting == MonitoringSettings.INDICES) {
                        assertEquals(Arrays.asList(monitoringSettings1.indices()), setting.get(updatedSettings));
                    } else {
                        fail("unable to check value for unknown dynamic setting [" + setting + "]");
                    }
                }
            }
        }
    }

    private ClusterUpdateSettingsRequestBuilder prepareRandomUpdateSettings(Settings updateSettings) {
        ClusterUpdateSettingsRequestBuilder requestBuilder = client().admin().cluster().prepareUpdateSettings();
        if (randomBoolean()) {
            requestBuilder.setTransientSettings(updateSettings);
        } else {
            requestBuilder.setPersistentSettings(updateSettings);
        }
        return requestBuilder;
    }

    private TimeValue newRandomTimeValue() {
        return TimeValue.parseTimeValue(randomFrom("30m", "1h", "3h", "5h", "7h", "10h", "1d"), null, getClass().getSimpleName());
    }

    private String[] randomStringArray() {
        final int size = scaledRandomIntBetween(1, 10);
        String[] items = new String[size];

        for (int i = 0; i < size; i++) {
            items[i] = randomAsciiOfLength(5);
        }
        return items;
    }
}
