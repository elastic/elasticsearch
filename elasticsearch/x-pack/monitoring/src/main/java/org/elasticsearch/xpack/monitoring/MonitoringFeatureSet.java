/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MonitoringFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final MonitoringLicensee licensee;
    private final Exporters exporters;

    @Inject
    public MonitoringFeatureSet(Settings settings, @Nullable MonitoringLicensee licensee, @Nullable Exporters exporters,
                                NamedWriteableRegistry namedWriteableRegistry) {
        this.enabled = MonitoringSettings.ENABLED.get(settings);
        this.licensee = licensee;
        this.exporters = exporters;
        namedWriteableRegistry.register(Usage.class, Usage.writeableName(Monitoring.NAME), Usage::new);
    }

    @Override
    public String name() {
        return Monitoring.NAME;
    }

    @Override
    public String description() {
        return "Monitoring for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licensee != null && licensee.isAvailable();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public XPackFeatureSet.Usage usage() {
        return new Usage(available(), enabled(), exportersUsage(exporters));
    }

    static Map<String, Object> exportersUsage(Exporters exporters) {
        if (exporters == null) {
            return null;
        }
        Map<String, Object> usage = new HashMap<>();
        for (Exporter exporter : exporters) {
            if (exporter.config().enabled()) {
                String type = exporter.type();
                int count = (Integer) usage.getOrDefault(type, 0);
                usage.put(type, count + 1);
            }
        }
        return usage;
    }

    static class Usage extends XPackFeatureSet.Usage {

        private static final String ENABLED_EXPORTERS_XFIELD = "enabled_exporters";

        private @Nullable Map<String, Object> exporters;

        public Usage(StreamInput in) throws IOException {
            super(in);
            exporters = in.readMap();
        }

        public Usage(boolean available, boolean enabled, Map<String, Object> exporters) {
            super(Monitoring.NAME, available, enabled);
            this.exporters = exporters;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(exporters);
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            if (exporters != null) {
                builder.field(ENABLED_EXPORTERS_XFIELD, exporters);
            }
        }
    }

}
