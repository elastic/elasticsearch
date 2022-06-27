/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class HealthDiskThresholdSettingsTests extends ESTestCase {

    public void testDefaults() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        HealthDiskThresholdSettings healthDiskThresholdSettings = new HealthDiskThresholdSettings(Settings.EMPTY, nss);

        ByteSizeValue zeroBytes = ByteSizeValue.parseBytesSizeValue("0b", "test");
        assertFalse(healthDiskThresholdSettings.getYellowThreshold().isAbsolute());
        assertEquals(85.0D, healthDiskThresholdSettings.getYellowThreshold().getRatio().getAsPercent(), 0.0D);
        assertFalse(healthDiskThresholdSettings.getRedThreshold().isAbsolute());
        assertEquals(90.0D, healthDiskThresholdSettings.getRedThreshold().getRatio().getAsPercent(), 0.0D);
    }

    public void testUpdate() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        HealthDiskThresholdSettings healthDiskThresholdSettings = new HealthDiskThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "1000mb")
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "500mb")
            .build();
        nss.applySettings(newSettings);

        assertEquals(ByteSizeValue.parseBytesSizeValue("1000mb", "test"), healthDiskThresholdSettings.getYellowThreshold().getAbsolute());
        assertTrue(healthDiskThresholdSettings.getYellowThreshold().isAbsolute());
        assertEquals(ByteSizeValue.parseBytesSizeValue("500mb", "test"), healthDiskThresholdSettings.getRedThreshold().getAbsolute());
        assertTrue(healthDiskThresholdSettings.getRedThreshold().isAbsolute());
    }

    public void testInvalidConstruction() {
        final Settings settings = Settings.builder()
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "90%")
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "80%")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IllegalArgumentException cause = expectThrows(
            IllegalArgumentException.class,
            () -> new HealthDiskThresholdSettings(settings, clusterSettings)
        );
        final String expectedCause = "setting ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey()
            + "=90%] cannot be greater than ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey()
            + "=80%]";
        assertThat(cause, hasToString(containsString(expectedCause)));
    }

    public void testInvalidYellowRedPercentageUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new HealthDiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "90%")
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "80%")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.health.disk.threshold.yellow] from [85%] to [90%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String expectedCause = "setting ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey()
            + "=90%] cannot be greater than ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey()
            + "=80%]";
        assertThat(cause, hasToString(containsString(expectedCause)));
    }

    public void testInvalidYellowRedBytesUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new HealthDiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "500m")
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "1000m")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.health.disk.threshold.yellow] from [85%] to [500m]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String expectedCause = "setting ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey()
            + "=500m] cannot be less than ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey()
            + "=1000m]";
        assertThat(cause, hasToString(containsString(expectedCause)));
    }

    public void testIncompatibleThresholdUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new HealthDiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "90%")
            .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "1000m")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.health.disk.threshold.yellow] from [85%] to [90%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String incompatibleExpected = "unable to consistently parse ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey()
            + "=90%], ["
            + HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey()
            + "=1000m] as percentage or bytes";
        assertThat(cause, hasToString(containsString(incompatibleExpected)));
    }

    public void testSequenceOfUpdates() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new HealthDiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings.Builder target = Settings.builder();

        {
            final Settings settings = Settings.builder()
                .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "99%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "persistent"));
            assertNull(target.get(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey()));
            assertThat(target.get(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey()), equalTo("99%"));
        }

        {
            final Settings settings = Settings.builder()
                .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "97%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "persistent"));
            assertThat(target.get(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey()), equalTo("97%"));
            assertThat(target.get(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey()), equalTo("99%"));
        }
    }

    public void testThresholdDescriptions() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        HealthDiskThresholdSettings healthDiskThresholdSettings = new HealthDiskThresholdSettings(Settings.EMPTY, clusterSettings);
        assertThat(healthDiskThresholdSettings.getYellowThreshold().getStringRep(), equalTo("85.0%"));
        assertThat(healthDiskThresholdSettings.getRedThreshold().getStringRep(), equalTo("90.0%"));

        healthDiskThresholdSettings = new HealthDiskThresholdSettings(
            Settings.builder()
                .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "91.2%")
                .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "91.3%")
                .build(),
            clusterSettings
        );

        assertThat(healthDiskThresholdSettings.getYellowThreshold().getStringRep(), equalTo("91.2%"));
        assertThat(healthDiskThresholdSettings.getRedThreshold().getStringRep(), equalTo("91.3%"));

        healthDiskThresholdSettings = new HealthDiskThresholdSettings(
            Settings.builder()
                .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), "1GB")
                .put(HealthDiskThresholdSettings.CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), "10MB")
                .build(),
            clusterSettings
        );

        assertThat(healthDiskThresholdSettings.getYellowThreshold().getStringRep(), equalTo("1gb"));
        assertThat(healthDiskThresholdSettings.getRedThreshold().getStringRep(), equalTo("10mb"));
    }
}
