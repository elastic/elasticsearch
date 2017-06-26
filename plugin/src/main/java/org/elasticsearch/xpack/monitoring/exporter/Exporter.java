/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Exporter implements AutoCloseable {

    /**
     * Every {@code Exporter} adds the ingest pipeline to bulk requests, but they should, at the exporter level, allow that to be disabled.
     * <p>
     * Note: disabling it obviously loses any benefit of using it, but it does allow clusters that don't run with ingest to not use it.
     */
    public static final String USE_INGEST_PIPELINE_SETTING = "use_ingest";
    /**
     * Every {@code Exporter} allows users to explicitly disable cluster alerts.
     */
    public static final String CLUSTER_ALERTS_MANAGEMENT_SETTING = "cluster_alerts.management.enabled";

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
     * Opens up a new export bulk. May return {@code null} indicating this exporter is not ready
     * yet to export the docs
     */
    public abstract ExportBulk openBulk();

    protected final boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected abstract void doClose();

    protected static String settingFQN(final Config config) {
        return MonitoringSettings.EXPORTERS_SETTINGS.getKey() + config.name;
    }

    protected static String settingFQN(final Config config, final String setting) {
        return MonitoringSettings.EXPORTERS_SETTINGS.getKey() + config.name + "." + setting;
    }

    public static class Config {

        private final String name;
        private final String type;
        private final boolean enabled;
        private final Settings globalSettings;
        private final Settings settings;
        private final ClusterService clusterService;
        private final XPackLicenseState licenseState;

        public Config(String name, String type, Settings globalSettings, Settings settings,
                      ClusterService clusterService, XPackLicenseState licenseState) {
            this.name = name;
            this.type = type;
            this.globalSettings = globalSettings;
            this.settings = settings;
            this.clusterService = clusterService;
            this.licenseState = licenseState;
            this.enabled = settings.getAsBoolean("enabled", true);
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

        public Settings globalSettings() {
            return globalSettings;
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
