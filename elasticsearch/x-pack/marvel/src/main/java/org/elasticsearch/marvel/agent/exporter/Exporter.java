/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.MarvelSettings;

import java.util.Collection;

public abstract class Exporter  {

    public static final String INDEX_NAME_TIME_FORMAT_SETTING = "index.name.time_format";
    public static final String BULK_TIMEOUT_SETTING = "bulk.timeout";

    protected final String type;
    protected final Config config;
    protected final ESLogger logger;

    protected final @Nullable TimeValue bulkTimeout;

    public Exporter(String type, Config config) {
        this.type = type;
        this.config = config;
        this.logger = config.logger(getClass());
        this.bulkTimeout = config.settings().getAsTime(BULK_TIMEOUT_SETTING, null);
    }

    public String type() {
        return type;
    }

    public String name() {
        return config.name;
    }

    public boolean masterOnly() {
        return false;
    }

    /**
     * Opens up a new export bulk. May return {@code null} indicating this exporter is not ready
     * yet to export the docs
     */
    public abstract ExportBulk openBulk();

    public void export(Collection<MonitoringDoc> monitoringDocs) throws Exception {
        ExportBulk bulk = openBulk();
        if (bulk != null) {
            bulk.add(monitoringDocs).flush();
        }
    }

    public abstract void close();

    protected String settingFQN(String setting) {
        return MarvelSettings.EXPORTERS_SETTINGS.getKey() + config.name + "." + setting;
    }

    public static class Config {

        private final String name;
        private final boolean enabled;
        private final Settings globalSettings;
        private final Settings settings;

        public Config(String name, Settings globalSettings, Settings settings) {
            this.name = name;
            this.globalSettings = globalSettings;
            this.settings = settings;
            this.enabled = settings.getAsBoolean("enabled", true);
        }

        public String name() {
            return name;
        }

        public boolean enabled() {
            return enabled;
        }

        public Settings settings() {
            return settings;
        }

        public ESLogger logger(Class clazz) {
            return Loggers.getLogger(clazz, globalSettings);
        }
    }

    public static abstract class Factory<E extends Exporter> {

        private final String type;
        private final boolean singleton;

        public Factory(String type, boolean singleton) {
            this.type = type;
            this.singleton = singleton;
        }

        public String type() {
            return type;
        }

        public boolean singleton() {
            return singleton;
        }

        public abstract E create(Config config);
    }
}
