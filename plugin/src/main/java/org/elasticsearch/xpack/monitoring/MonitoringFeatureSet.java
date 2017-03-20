/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;

public class MonitoringFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Exporters exporters;

    @Inject
    public MonitoringFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, @Nullable Exporters exporters) {
        this.enabled = XPackSettings.MONITORING_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.exporters = exporters;
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
        return licenseState != null && licenseState.isMonitoringAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        listener.onResponse(new Usage(available(), enabled(), exportersUsage(exporters)));
    }

    static Map<String, Object> exportersUsage(Exporters exporters) {
        if (exporters == null) {
            return null;
        }
        Map<String, Object> usage = new HashMap<>();
        for (Exporter exporter : exporters) {
            if (exporter.config().enabled()) {
                String type = exporter.config().type();
                int count = (Integer) usage.getOrDefault(type, 0);
                usage.put(type, count + 1);
            }
        }
        return usage;
    }

    public static class Usage extends XPackFeatureSet.Usage {

        private static final String ENABLED_EXPORTERS_XFIELD = "enabled_exporters";

        @Nullable private Map<String, Object> exporters;

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
