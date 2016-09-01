/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.exporter;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Exporter implements AutoCloseable {

    /**
     * The pipeline name passed with any <em>direct</em> indexing operation in order to support future API revisions.
     */
    public static final String EXPORT_PIPELINE_NAME = "xpack_monitoring_" + MonitoringTemplateUtils.TEMPLATE_VERSION;

    public static final String INDEX_NAME_TIME_FORMAT_SETTING = "index.name.time_format";
    public static final String BULK_TIMEOUT_SETTING = "bulk.timeout";
    /**
     * Every {@code Exporter} adds the ingest pipeline to bulk requests, but they should, at the exporter level, allow that to be disabled.
     * <p>
     * Note: disabling it obviously loses any benefit of using it, but it does allow clusters that don't run with ingest to not use it.
     */
    public static final String USE_INGEST_PIPELINE_SETTING = "use_ingest";

    protected final Config config;
    protected final Logger logger;

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

    /**
     * Create an empty pipeline.
     * <pre><code>
     * {
     *   "description" : "2: This is a placeholder pipeline ...",
     *   "processors": [ ]
     * }
     * </code></pre>
     * The expectation is that you will call either {@link XContentBuilder#string()} or {@link XContentBuilder#bytes()}}.
     *
     * @param type The type of data you want to format for the request
     * @return Never {@code null}. Always an ended-object.
     */
    public static XContentBuilder emptyPipeline(XContentType type) {
        try {
            // For now: We prepend the API version to the string so that it's easy to parse in the future; if we ever add metadata
            //  to pipelines, then it would better serve this use case
            return XContentBuilder.builder(type.xContent()).startObject()
                .field("description", MonitoringTemplateUtils.TEMPLATE_VERSION +
                                      ": This is a placeholder pipeline for Monitoring API version " +
                                      MonitoringTemplateUtils.TEMPLATE_VERSION + " so that future versions may fix breaking changes.")
                .startArray("processors").endArray()
            .endObject();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create empty pipeline", e);
        }
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

        public Logger logger(Class clazz) {
            return Loggers.getLogger(clazz, globalSettings, name);
        }
    }

    /** A factory for constructing {@link Exporter} instances.*/
    public interface Factory {

        /** Create an exporter with the given configuration. */
        Exporter create(Config config);
    }
}
