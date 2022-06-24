/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.DiskThresholdParser;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A container to keep the settings for health disk thresholds up to date with cluster setting changes.
 * These thresholds are used to determine if a node that does not hold data has enough disk space.
 */
public class HealthDiskThresholdSettings {

    public static final Setting<String> CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING = new Setting<>(
        "cluster.health.disk.threshold.yellow",
        "85%",
        (s) -> DiskThresholdParser.validThresholdSetting(s, "cluster.health.disk.threshold.yellow"),
        new YellowThresholdValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING = new Setting<>(
        "cluster.health.disk.threshold.red",
        "90%",
        (s) -> DiskThresholdParser.validThresholdSetting(s, "cluster.health.disk.threshold.red"),
        new RedThresholdValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile HealthMetadata.DiskHealthThresholds.Threshold yellowThreshold;
    private volatile HealthMetadata.DiskHealthThresholds.Threshold redThreshold;
    private final Collection<Listener> listeners = new CopyOnWriteArrayList<>();

    public HealthDiskThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        setYellowThreshold(CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.get(settings));
        setRedThreshold(CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING, this::setYellowThreshold);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING, this::setRedThreshold);
    }

    private void setYellowThreshold(String yellowThreshold) {
        this.yellowThreshold = new HealthMetadata.DiskHealthThresholds.Threshold(
            DiskThresholdParser.thresholdPercentageFromThreshold(yellowThreshold),
            DiskThresholdParser.thresholdBytesFromThreshold(yellowThreshold, CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey())
        );
        listeners.forEach(Listener::onChange);
    }

    private void setRedThreshold(String redThreshold) {
        this.redThreshold = new HealthMetadata.DiskHealthThresholds.Threshold(
            DiskThresholdParser.thresholdPercentageFromThreshold(redThreshold),
            DiskThresholdParser.thresholdBytesFromThreshold(redThreshold, CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey())
        );
        listeners.forEach(Listener::onChange);
    }

    public HealthMetadata.DiskHealthThresholds.Threshold getYellowThreshold() {
        return yellowThreshold;
    }

    public HealthMetadata.DiskHealthThresholds.Threshold getRedThreshold() {
        return redThreshold;
    }

    static final class YellowThresholdValidator implements Setting.Validator<String> {

        @Override
        public void validate(String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String redThreshold = (String) settings.get(CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING);
            DiskThresholdParser.doValidate(
                value,
                List.of(
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), value),
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), redThreshold)
                )
            );
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING);
            return settings.iterator();
        }

    }

    static final class RedThresholdValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String yellowThreshold = (String) settings.get(CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING);
            DiskThresholdParser.doValidate(
                value,
                List.of(
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING.getKey(), yellowThreshold),
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_HEALTH_DISK_RED_THRESHOLD_SETTING.getKey(), value)
                )
            );
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(CLUSTER_HEALTH_DISK_YELLOW_THRESHOLD_SETTING);
            return settings.iterator();
        }
    }

    public boolean addListener(Listener listener) {
        return this.listeners.add(listener);
    }

    public interface Listener {

        void onChange();
    }
}
