/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import org.apache.lucene.util.LuceneTestCase.BadApple;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
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
                .put(Node.HTTP_ENABLED, false)
                .put(marvelSettings())
                .build();
    }

    private Settings marvelSettings() {
        return Settings.builder()
                .put(MarvelSettings.INTERVAL, interval)
                .put(MarvelSettings.INDEX_STATS_TIMEOUT, indexStatsTimeout)
                .put(MarvelSettings.INDICES_STATS_TIMEOUT, indicesStatsTimeout)
                .putArray(MarvelSettings.INDICES, indices)
                .put(MarvelSettings.CLUSTER_STATE_TIMEOUT, clusterStateTimeout)
                .put(MarvelSettings.CLUSTER_STATS_TIMEOUT, clusterStatsTimeout)
                .put(MarvelSettings.INDEX_RECOVERY_TIMEOUT, recoveryTimeout)
                .put(MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY, recoveryActiveOnly)
                .putArray(MarvelSettings.COLLECTORS, collectors)
                .build();
    }

    public void testMarvelSettings() throws Exception {
        logger.info("--> testing marvel settings service initialization");
        for (final MarvelSettings marvelSettings : internalCluster().getInstances(MarvelSettings.class)) {
            assertThat(marvelSettings.interval().millis(), equalTo(interval.millis()));
            assertThat(marvelSettings.indexStatsTimeout().millis(), equalTo(indexStatsTimeout.millis()));
            assertThat(marvelSettings.indicesStatsTimeout().millis(), equalTo(indicesStatsTimeout.millis()));
            assertArrayEquals(marvelSettings.indices(), indices);
            assertThat(marvelSettings.clusterStateTimeout().millis(), equalTo(clusterStateTimeout.millis()));
            assertThat(marvelSettings.clusterStatsTimeout().millis(), equalTo(clusterStatsTimeout.millis()));
            assertThat(marvelSettings.recoveryTimeout().millis(), equalTo(recoveryTimeout.millis()));
            assertThat(marvelSettings.recoveryActiveOnly(), equalTo(recoveryActiveOnly));
            assertArrayEquals(marvelSettings.collectors(), collectors);
        }

        logger.info("--> testing marvel dynamic settings update");
        Settings.Builder transientSettings = Settings.builder();

        for (String setting : MarvelSettings.dynamicSettings().keySet()) {
            Object updated = null;

            if (setting.endsWith(".*")) {
                setting = setting.substring(0, setting.lastIndexOf('.'));
            }

            switch (setting) {
                case MarvelSettings.INTERVAL:
                case MarvelSettings.INDEX_STATS_TIMEOUT:
                case MarvelSettings.INDICES_STATS_TIMEOUT:
                case MarvelSettings.CLUSTER_STATE_TIMEOUT:
                case MarvelSettings.CLUSTER_STATS_TIMEOUT:
                case MarvelSettings.INDEX_RECOVERY_TIMEOUT:
                    updated = newRandomTimeValue();
                    transientSettings.put(setting, updated);
                    break;
                case MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY:
                    updated = randomBoolean();
                    transientSettings.put(setting, updated);
                    break;
                case MarvelSettings.INDICES:
                    updated = randomStringArray();
                    transientSettings.putArray(setting, (String[]) updated);
                    break;
                default:
                    fail("unknown dynamic setting [" + setting + "]");
            }
        }

        logger.error("--> updating settings");
        final Settings updatedSettings = transientSettings.build();
        assertAcked(prepareRandomUpdateSettings(updatedSettings).get());

        logger.error("--> checking that the value has been correctly updated on all marvel settings services");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (String setting : MarvelSettings.dynamicSettings().keySet()) {
                    for (final MarvelSettings marvelSettings : internalCluster().getInstances(MarvelSettings.class)) {
                        MarvelSetting current = null;
                        Object value = null;

                        switch (setting) {
                            case MarvelSettings.INTERVAL:
                            case MarvelSettings.INDEX_STATS_TIMEOUT:
                            case MarvelSettings.INDICES_STATS_TIMEOUT:
                            case MarvelSettings.CLUSTER_STATE_TIMEOUT:
                            case MarvelSettings.CLUSTER_STATS_TIMEOUT:
                            case MarvelSettings.INDEX_RECOVERY_TIMEOUT:
                                current = marvelSettings.getSetting(setting);
                                value = current.getValue();
                                assertThat(value, instanceOf(TimeValue.class));
                                assertThat(((TimeValue) value).millis(), equalTo(updatedSettings.getAsTime(setting, null).millis()));
                                break;

                            case MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY:
                                current = marvelSettings.getSetting(setting);
                                value = current.getValue();
                                assertThat(value, instanceOf(Boolean.class));
                                assertThat(((Boolean) value), equalTo(updatedSettings.getAsBoolean(setting, null)));
                                break;

                            default:
                                if (setting.startsWith(MarvelSettings.INDICES)) {
                                    current = marvelSettings.getSetting(MarvelSettings.INDICES);
                                    value = current.getValue();
                                    assertArrayEquals((String[]) value, updatedSettings.getAsArray(MarvelSettings.INDICES));
                                } else {
                                    fail("unable to check value for unknown dynamic setting [" + setting + "]");
                                }
                        }
                    }
                }
            }
        });
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
