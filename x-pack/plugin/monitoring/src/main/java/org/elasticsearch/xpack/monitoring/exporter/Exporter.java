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
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class Exporter implements AutoCloseable {

    private static final Setting.AffixSetting<Boolean> ENABLED_SETTING =
            Setting.affixKeySetting("xpack.monitoring.exporters.","enabled",
                    key -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope));

    public static final Setting.AffixSetting<String> TYPE_SETTING = Setting.affixKeySetting(
        "xpack.monitoring.exporters.",
        "type",
        key -> Setting.simpleString(
            key,
            new Setting.Validator<>() {

                @Override
                public void validate(final String value) {

                }

                @Override
                public void validate(final String value, final Map<Setting<?>, Object> settings) {
                    switch (value) {
                        case "":
                            break;
                        case "http":
                            // if the type is http, then hosts must be set
                            final String namespace = TYPE_SETTING.getNamespace(TYPE_SETTING.getConcreteSetting(key));
                            final Setting<List<String>> hostsSetting = HttpExporter.HOST_SETTING.getConcreteSettingForNamespace(namespace);
                            @SuppressWarnings("unchecked") final List<String> hosts = (List<String>) settings.get(hostsSetting);
                            if (hosts.isEmpty()) {
                                throw new SettingsException("host list for [" + hostsSetting.getKey() + "] is empty");
                            }
                            break;
                        case "local":
                            break;
                        default:
                            throw new SettingsException(
                                "type [" + value + "] for key [" + key + "] is invalid, only [http] and [local] are allowed");
                    }

                }

                @Override
                public Iterator<Setting<?>> settings() {
                    final String namespace =
                        Exporter.TYPE_SETTING.getNamespace(Exporter.TYPE_SETTING.getConcreteSetting(key));
                    final List<Setting<?>> settings = List.of(HttpExporter.HOST_SETTING.getConcreteSettingForNamespace(namespace));
                    return settings.iterator();
                }

            },
            Property.Dynamic,
            Property.NodeScope));
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
    static final Setting.AffixSetting<DateFormatter> INDEX_NAME_TIME_FORMAT_SETTING =
                Setting.affixKeySetting("xpack.monitoring.exporters.","index.name.time_format",
                        key -> new Setting<DateFormatter>(
                                key,
                                Exporter.INDEX_FORMAT,
                                DateFormatter::forPattern,
                                Property.Dynamic,
                                Property.NodeScope));

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
        Setting<DateFormatter> setting = INDEX_NAME_TIME_FORMAT_SETTING.getConcreteSettingForNamespace(config.name);
        return setting.get(config.settings());
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
