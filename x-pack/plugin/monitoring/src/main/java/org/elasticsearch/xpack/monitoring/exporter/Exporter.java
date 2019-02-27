/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.license.XPackLicenseState;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class Exporter implements AutoCloseable {

    private static final Setting.AffixSetting<Boolean> ENABLED_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","enabled",
                    key -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope));

    private static final Setting.AffixSetting<String> TYPE_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","type",
                    key -> Setting.simpleString(key, v -> {
                        switch (v) {
                            case "":
                            case "http":
                            case "local":
                                break;
                            default:
                                throw new IllegalArgumentException("only exporter types [http] and [local] are allowed [" + v +
                                        "] is invalid");
                        }
                    }, Property.Dynamic, Property.NodeScope));
    /**
     * Every {@code Exporter} adds the ingest pipeline to bulk requests, but they should, at the exporter level, allow that to be disabled.
     * <p>
     * Note: disabling it obviously loses any benefit of using it, but it does allow clusters that don't run with ingest to not use it.
     */
    public static final Setting.AffixSetting<Boolean> USE_INGEST_PIPELINE_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","use_ingest",
                    key -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope));
    /**
     * Every {@code Exporter} allows users to explicitly disable cluster alerts.
     */
    public static final Setting.AffixSetting<Boolean> CLUSTER_ALERTS_MANAGEMENT_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.", "cluster_alerts.management.enabled",
                    key -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope));
    /**
     * Every {@code Exporter} allows users to explicitly disable specific cluster alerts.
     * <p>
     * When cluster alerts management is enabled, this should delete anything blacklisted here in addition to not creating it.
     */
    public static final Setting.AffixSetting<List<String>> CLUSTER_ALERTS_BLACKLIST_SETTING = Setting
                .affixKeySetting("xpack.monitoring.exporters.", "cluster_alerts.management.blacklist",
                    key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Property.Dynamic, Property.NodeScope));

    /**
     * Every {@code Exporter} allows users to use a different index time format.
     */
    private static final Setting.AffixSetting<String> INDEX_NAME_TIME_FORMAT_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","index.name.time_format",
                    key -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope));

    private static final String INDEX_FORMAT = "yyyy.MM.dd";

    protected final Config config;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public Exporter(Config config) {
        this.config = config;
    }

    public String name() {
        return config.name;
    }

    public Config config() {
        return config;
    }

    /** Returns true if only one instance of this exporter should be allowed. */
    public boolean isSingleton() {
        return false;
    }

    /**
     * Opens up a new export bulk.
     *
     * @param listener Returns {@code null} to indicate that this exporter is not ready to export the docs.
     */
    public abstract void openBulk(ActionListener<ExportBulk> listener);

    protected final boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected abstract void doClose();

    protected static DateFormatter dateTimeFormatter(final Config config) {
        Setting<String> setting = INDEX_NAME_TIME_FORMAT_SETTING.getConcreteSettingForNamespace(config.name);
        String format = setting.exists(config.settings()) ? setting.get(config.settings()) : INDEX_FORMAT;
        try {
            return DateFormatter.forPattern(format).withZone(ZoneOffset.UTC);
        } catch (IllegalArgumentException e) {
            throw new SettingsException("[" + INDEX_NAME_TIME_FORMAT_SETTING.getKey() + "] invalid index name time format: ["
                    + format + "]", e);
        }
    }

    public static List<Setting.AffixSetting<?>> getSettings() {
        return Arrays.asList(USE_INGEST_PIPELINE_SETTING, CLUSTER_ALERTS_MANAGEMENT_SETTING, TYPE_SETTING, ENABLED_SETTING,
                INDEX_NAME_TIME_FORMAT_SETTING, CLUSTER_ALERTS_BLACKLIST_SETTING);
    }

    public static class Config {

        private final String name;
        private final String type;
        private final boolean enabled;
        private final Settings settings;
        private final ClusterService clusterService;
        private final XPackLicenseState licenseState;

        public Config(String name, String type, Settings settings,
                      ClusterService clusterService, XPackLicenseState licenseState) {
            this.name = name;
            this.type = type;
            this.settings = settings;
            this.clusterService = clusterService;
            this.licenseState = licenseState;
            this.enabled = ENABLED_SETTING.getConcreteSettingForNamespace(name).get(settings);
        }

        public String name() {
            return name;
        }

        public String type() {
            return type;
        }

        public boolean enabled() {
            return enabled;
        }

        public Settings settings() {
            return settings;
        }

        public ClusterService clusterService() {
            return clusterService;
        }

        public XPackLicenseState licenseState() {
            return licenseState;
        }

    }

    /** A factory for constructing {@link Exporter} instances.*/
    public interface Factory {

        /** Create an exporter with the given configuration. */
        Exporter create(Config config);
    }
}
