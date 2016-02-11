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
import org.elasticsearch.marvel.MarvelSettings;
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
                .put(MarvelSettings.INTERVAL.getKey(), interval)
                .put(MarvelSettings.INDEX_STATS_TIMEOUT.getKey(), indexStatsTimeout)
                .put(MarvelSettings.INDICES_STATS_TIMEOUT.getKey(), indicesStatsTimeout)
                .putArray(MarvelSettings.INDICES.getKey(), indices)
                .put(MarvelSettings.CLUSTER_STATE_TIMEOUT.getKey(), clusterStateTimeout)
                .put(MarvelSettings.CLUSTER_STATS_TIMEOUT.getKey(), clusterStatsTimeout)
                .put(MarvelSettings.INDEX_RECOVERY_TIMEOUT.getKey(), recoveryTimeout)
                .put(MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY.getKey(), recoveryActiveOnly)
                .putArray(MarvelSettings.COLLECTORS.getKey(), collectors)
                .build();
    }

    public void testMarvelSettings() throws Exception {
        logger.info("--> testing monitoring settings service initialization");
        for (final MarvelSettings marvelSettings : internalCluster().getInstances(MarvelSettings.class)) {
            assertThat(marvelSettings.indexStatsTimeout().millis(), equalTo(indexStatsTimeout.millis()));
            assertThat(marvelSettings.indicesStatsTimeout().millis(), equalTo(indicesStatsTimeout.millis()));
            assertArrayEquals(marvelSettings.indices(), indices);
            assertThat(marvelSettings.clusterStateTimeout().millis(), equalTo(clusterStateTimeout.millis()));
            assertThat(marvelSettings.clusterStatsTimeout().millis(), equalTo(clusterStatsTimeout.millis()));
            assertThat(marvelSettings.recoveryTimeout().millis(), equalTo(recoveryTimeout.millis()));
            assertThat(marvelSettings.recoveryActiveOnly(), equalTo(recoveryActiveOnly));
        }

        for (final AgentService service : internalCluster().getInstances(AgentService.class)) {
            assertThat(service.getSamplingInterval().millis(), equalTo(interval.millis()));
            assertArrayEquals(service.collectors(), collectors);

        }

        logger.info("--> testing monitoring dynamic settings update");
        Settings.Builder transientSettings = Settings.builder();
        final Setting[] marvelSettings = new Setting[] {
                MarvelSettings.INDICES,
        MarvelSettings.INTERVAL,
        MarvelSettings.INDEX_RECOVERY_TIMEOUT,
        MarvelSettings.INDEX_STATS_TIMEOUT,
        MarvelSettings.INDICES_STATS_TIMEOUT,
        MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY,
        MarvelSettings.COLLECTORS,
        MarvelSettings.CLUSTER_STATE_TIMEOUT,
        MarvelSettings.CLUSTER_STATS_TIMEOUT };
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
            if (setting == MarvelSettings.INTERVAL) {
                for (final AgentService service : internalCluster().getInstances(AgentService.class)) {
                    assertEquals(service.getSamplingInterval(), setting.get(updatedSettings));
                }
            } else {
                for (final MarvelSettings marvelSettings1 : internalCluster().getInstances(MarvelSettings.class)) {
                    if (setting == MarvelSettings.INDEX_STATS_TIMEOUT) {
                        assertEquals(marvelSettings1.indexStatsTimeout(), setting.get(updatedSettings));
                    } else if (setting == MarvelSettings.INDICES_STATS_TIMEOUT) {
                        assertEquals(marvelSettings1.indicesStatsTimeout(), setting.get(updatedSettings));
                    } else if (setting == MarvelSettings.CLUSTER_STATS_TIMEOUT) {
                        assertEquals(marvelSettings1.clusterStatsTimeout(), setting.get(updatedSettings));
                    } else if (setting == MarvelSettings.CLUSTER_STATE_TIMEOUT) {
                        assertEquals(marvelSettings1.clusterStateTimeout(), setting.get(updatedSettings));
                    } else if (setting == MarvelSettings.INDEX_RECOVERY_TIMEOUT) {
                        assertEquals(marvelSettings1.recoveryTimeout(), setting.get(updatedSettings));
                    } else if (setting == MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY) {
                        assertEquals(Boolean.valueOf(marvelSettings1.recoveryActiveOnly()), setting.get(updatedSettings));
                    } else if (setting == MarvelSettings.INDICES) {
                        assertEquals(Arrays.asList(marvelSettings1.indices()), setting.get(updatedSettings));
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
