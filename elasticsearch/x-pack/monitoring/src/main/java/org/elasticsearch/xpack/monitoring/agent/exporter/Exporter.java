/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.exporter;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Exporter implements AutoCloseable {

    public static final String INDEX_NAME_TIME_FORMAT_SETTING = "index.name.time_format";
    public static final String BULK_TIMEOUT_SETTING = "bulk.timeout";

    protected final Config config;
    protected final ESLogger logger;

    @Nullable protected final TimeValue bulkTimeout;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public Exporter(Config config) {
        this.config = config;
        this.logger = config.logger(getClass());
        this.bulkTimeout = config.settings().getAsTime(BULK_TIMEOUT_SETTING, null);
    }

    public String name() {
        return config.name;
    }

    public Config config() {
        return config;
    }

    public boolean masterOnly() {
        return false;
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

    protected String settingFQN(String setting) {
        return MonitoringSettings.EXPORTERS_SETTINGS.getKey() + config.name + "." + setting;
    }

    public static class Config {

        private final String name;
        private final String type;
        private final boolean enabled;
        private final Settings globalSettings;
        private final Settings settings;

        public Config(String name, String type, Settings globalSettings, Settings settings) {
            this.name = name;
            this.type = type;
            this.globalSettings = globalSettings;
            this.settings = settings;
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

        public Settings settings() {
            return settings;
        }

        public ESLogger logger(Class clazz) {
            return Loggers.getLogger(clazz, globalSettings, name);
        }
    }

    /** A factory for constructing {@link Exporter} instances.*/
    public interface Factory {

        /** Create an exporter with the given configuration. */
        Exporter create(Config config);
    }
}
