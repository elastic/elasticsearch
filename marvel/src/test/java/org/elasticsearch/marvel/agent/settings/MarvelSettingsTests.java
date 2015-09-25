/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MarvelSettingsTests extends ESIntegTestCase {

    private final TimeValue startUp = randomParsedTimeValue();
    private final TimeValue interval = randomParsedTimeValue();
    private final TimeValue indexStatsTimeout = randomParsedTimeValue();
    private final String[] indices = randomStringArray();
    private final TimeValue clusterStateTimeout = randomParsedTimeValue();
    private final TimeValue clusterStatsTimeout = randomParsedTimeValue();
    private final TimeValue recoveryTimeout = randomParsedTimeValue();
    private final Boolean recoveryActiveOnly = randomBoolean();
    private final String[] collectors = randomStringArray();
    private final TimeValue licenseGracePeriod = randomExpirationDelay();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put(marvelSettings())
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LicensePlugin.class, MarvelPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    private Settings marvelSettings() {
        return Settings.builder()
                .put(MarvelSettings.STARTUP_DELAY, startUp)
                .put(MarvelSettings.INTERVAL, interval)
                .put(MarvelSettings.INDEX_STATS_TIMEOUT, indexStatsTimeout)
                .putArray(MarvelSettings.INDICES, indices)
                .put(MarvelSettings.CLUSTER_STATE_TIMEOUT, clusterStateTimeout)
                .put(MarvelSettings.CLUSTER_STATS_TIMEOUT, clusterStatsTimeout)
                .put(MarvelSettings.INDEX_RECOVERY_TIMEOUT, recoveryTimeout)
                .put(MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY, recoveryActiveOnly)
                .putArray(MarvelSettings.COLLECTORS, collectors)
                .put(MarvelSettings.LICENSE_GRACE_PERIOD, licenseGracePeriod)
                .build();
    }

    @Test
    public void testMarvelSettingService() throws Exception {
        logger.info("--> printing marvel settings values");
        for (MarvelSetting setting : MarvelSettings.settings()) {
            logger.info("\t{}", setting);
        }

        logger.info("--> testing marvel settings service initialization");
        for (final MarvelSettings marvelSettings : internalCluster().getInstances(MarvelSettings.class)) {
            assertThat(marvelSettings.startUpDelay().millis(), equalTo(startUp.millis()));
            assertThat(marvelSettings.interval().millis(), equalTo(interval.millis()));
            assertThat(marvelSettings.indexStatsTimeout().millis(), equalTo(indexStatsTimeout.millis()));
            assertArrayEquals(marvelSettings.indices(), indices);
            assertThat(marvelSettings.clusterStateTimeout().millis(), equalTo(clusterStateTimeout.millis()));
            assertThat(marvelSettings.clusterStatsTimeout().millis(), equalTo(clusterStatsTimeout.millis()));
            assertThat(marvelSettings.recoveryTimeout().millis(), equalTo(recoveryTimeout.millis()));
            assertThat(marvelSettings.recoveryActiveOnly(), equalTo(recoveryActiveOnly));
            assertArrayEquals(marvelSettings.collectors(), collectors);
            assertThat(marvelSettings.licenseExpirationGracePeriod().millis(), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(MarvelSettings.MAX_LICENSE_GRACE_PERIOD.millis())));

            for (final MarvelSetting setting : MarvelSettings.dynamicSettings()) {
                assertThat(marvelSettings.getSettingValue(setting.getName()), equalTo(setting.getValue()));
            }
        }

        logger.info("--> testing marvel dynamic settings update");
        for (final MarvelSetting setting : MarvelSettings.dynamicSettings()) {
            Object updated = null;
            Settings.Builder transientSettings = Settings.builder();
            if (setting instanceof MarvelSetting.TimeValueSetting) {
                updated = randomParsedTimeValue();
                transientSettings.put(setting.getName(), updated);

            } else if (setting instanceof MarvelSetting.BooleanSetting) {
                updated = randomBoolean();
                transientSettings.put(setting.getName(), updated);

            } else if (setting instanceof MarvelSetting.StringSetting) {
                updated = randomAsciiOfLength(10);
                transientSettings.put(setting.getName(), updated);

            } else if (setting instanceof MarvelSetting.StringArraySetting) {
                updated = randomStringArray();
                transientSettings.putArray(setting.getName(), (String[]) updated);
            }

            logger.info("--> updating {} to value [{}]", setting, updated);
            assertAcked(prepareRandomUpdateSettings(transientSettings.build()).get());

            // checking that the value has been correctly updated on all marvel settings services
            final Object expected = updated;
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    for (final MarvelSettings marvelSettings : internalCluster().getInstances(MarvelSettings.class)) {
                        MarvelSetting current = marvelSettings.getSetting(setting.getName());
                        Object value = current.getValue();

                        logger.info("--> {} in {}", current, marvelSettings);
                        if (setting instanceof MarvelSetting.TimeValueSetting) {
                            assertThat(((TimeValue) value).millis(), equalTo(((TimeValue) expected).millis()));

                        } else if (setting instanceof MarvelSetting.BooleanSetting) {
                            assertThat((Boolean) value, equalTo((Boolean) expected));

                        } else if (setting instanceof MarvelSetting.StringSetting) {
                            assertThat((String) value, equalTo((String) expected));

                        } else if (setting instanceof MarvelSetting.StringArraySetting) {
                            assertArrayEquals((String[]) value, (String[]) expected);
                        }
                    }
                }
            });
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

    private TimeValue randomParsedTimeValue() {
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

    private TimeValue randomExpirationDelay() {
        return randomBoolean() ? randomParsedTimeValue() : TimeValue.timeValueHours(randomIntBetween(-10, 10) * 24);
    }
}
