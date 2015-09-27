/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.shield.MarvelSettingsFilter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collection;

public abstract class Exporter  {

    public static final String INDEX_NAME_TIME_FORMAT_SETTING = "index.name.time_format";
    public static final String BULK_TIMEOUT_SETTING = "bulk.timeout";

    public static final Version MIN_SUPPORTED_TEMPLATE_VERSION = Version.V_2_0_0_beta2;
    public static final String DEFAULT_INDEX_NAME_TIME_FORMAT = "YYYY.MM.dd";

    protected final String type;
    protected final Config config;
    protected final ESLogger logger;
    protected final IndexNameResolver indexNameResolver;
    protected final @Nullable TimeValue bulkTimeout;

    public Exporter(String type, Config config) {
        this.type = type;
        this.config = config;
        this.logger = config.logger(getClass());
        this.indexNameResolver = new DefaultIndexNameResolver(config.settings);
        bulkTimeout = config.settings().getAsTime(BULK_TIMEOUT_SETTING, null);
    }

    public String type() {
        return type;
    }

    public String name() {
        return config.name;
    }

    public IndexNameResolver indexNameResolver() {
        return indexNameResolver;
    }

    public boolean masterOnly() {
        return false;
    }

    /**
     * Opens up a new export bulk. May return {@code null} indicating this exporter is not ready
     * yet to export the docs
     */
    public abstract ExportBulk openBulk();

    public void export(Collection<MarvelDoc> marvelDocs) throws Exception {
        ExportBulk bulk = openBulk();
        if (bulk != null) {
            bulk.add(marvelDocs).flush();
        }
    }

    public abstract void close();

    protected String settingFQN(String setting) {
        return Exporters.EXPORTERS_SETTING + "." + config.name + "." + setting;
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

        public void filterOutSensitiveSettings(String prefix, MarvelSettingsFilter filter) {
        }

        public abstract E create(Config config);
    }

    /**
     *
     */
    public class DefaultIndexNameResolver implements IndexNameResolver {

        private final DateTimeFormatter indexTimeFormatter;

        public DefaultIndexNameResolver(Settings settings) {
            String indexTimeFormat = settings.get(INDEX_NAME_TIME_FORMAT_SETTING, DEFAULT_INDEX_NAME_TIME_FORMAT);
            try {
                indexTimeFormatter = DateTimeFormat.forPattern(indexTimeFormat).withZoneUTC();
            } catch (IllegalArgumentException e) {
                throw new SettingsException("invalid marvel index name time format [" + indexTimeFormat + "] set for [" + settingFQN(INDEX_NAME_TIME_FORMAT_SETTING) + "]", e);
            }
        }

        @Override
        public String resolve(MarvelDoc doc) {
            if (doc.index() != null) {
                return doc.index();
            }
            return resolve(doc.timestamp());
        }

        @Override
        public String resolve(long timestamp) {
            return MarvelSettings.MARVEL_INDICES_PREFIX + indexTimeFormatter.print(timestamp);
        }

        @Override
        public String toString() {
            return indexTimeFormatter.toString();
        }
    }
}