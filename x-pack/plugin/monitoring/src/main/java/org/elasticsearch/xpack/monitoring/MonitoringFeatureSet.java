/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;

import java.util.HashMap;
import java.util.Map;

public class MonitoringFeatureSet implements XPackFeatureSet {

    private final MonitoringService monitoring;
    private final Exporters exporters;

    @Inject
    public MonitoringFeatureSet(@Nullable MonitoringService monitoring, @Nullable Exporters exporters) {
        this.monitoring = monitoring;
        this.exporters = exporters;
    }

    @Override
    public String name() {
        return XPackField.MONITORING;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        final boolean collectionEnabled = monitoring != null && monitoring.isMonitoringActive();

        listener.onResponse(new MonitoringFeatureSetUsage(collectionEnabled, exportersUsage(exporters)));
    }

    static Map<String, Object> exportersUsage(Exporters exporters) {
        if (exporters == null) {
            return null;
        }
        Map<String, Object> usage = new HashMap<>();
        for (Exporter exporter : exporters.getEnabledExporters()) {
            if (exporter.config().enabled()) {
                String type = exporter.config().type();
                int count = (Integer) usage.getOrDefault(type, 0);
                usage.put(type, count + 1);
            }
        }
        return usage;
    }

}
