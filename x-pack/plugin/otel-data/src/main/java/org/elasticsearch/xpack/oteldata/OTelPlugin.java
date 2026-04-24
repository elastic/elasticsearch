/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.oteldata.otlp.OTLPLogsRestAction;
import org.elasticsearch.xpack.oteldata.otlp.OTLPLogsTransportAction;
import org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsRestAction;
import org.elasticsearch.xpack.oteldata.otlp.OTLPMetricsTransportAction;
import org.elasticsearch.xpack.oteldata.otlp.OTLPTracesRestAction;
import org.elasticsearch.xpack.oteldata.otlp.OTLPTracesTransportAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class OTelPlugin extends Plugin implements ActionPlugin {

    // OTEL_DATA_REGISTRY_ENABLED controls enabling the index template registry.
    //
    // This setting will be ignored if the plugin is disabled.
    static final Setting<Boolean> OTEL_DATA_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.otel_data.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public enum HistogramMappingSettingValues {
        HISTOGRAM,
        EXPONENTIAL_HISTOGRAM
    };

    public static final Setting<HistogramMappingSettingValues> HISTOGRAM_FIELD_TYPE_SETTING = Setting.enumSetting(
        HistogramMappingSettingValues.class,
        "xpack.otel_data.histogram_field_type",
        HistogramMappingSettingValues.EXPONENTIAL_HISTOGRAM,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(OTelPlugin.class);

    private static final boolean OTLP_TRACES_ENABLED = new FeatureFlag("otlp_traces").isEnabled();
    private static final boolean OTLP_LOGS_ENABLED = new FeatureFlag("otlp_logs").isEnabled();
    private final SetOnce<OTelIndexTemplateRegistry> registry = new SetOnce<>();
    private final SetOnce<IndexingPressure> indexingPressure = new SetOnce<>();
    private final boolean enabled;
    private final long maxProtobufContentLengthBytes;

    public OTelPlugin(Settings settings) {
        this.enabled = XPackSettings.OTEL_DATA_ENABLED.get(settings);
        this.maxProtobufContentLengthBytes = HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.get(settings).getBytes();
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        assert indexingPressure.get() != null : "indexing pressure must be set";
        List<RestHandler> handlers = new ArrayList<>(3);
        handlers.add(new OTLPMetricsRestAction(indexingPressure.get(), maxProtobufContentLengthBytes));
        if (OTLP_TRACES_ENABLED) {
            handlers.add(new OTLPTracesRestAction(indexingPressure.get(), maxProtobufContentLengthBytes));
        }
        if (OTLP_LOGS_ENABLED) {
            handlers.add(new OTLPLogsRestAction(indexingPressure.get(), maxProtobufContentLengthBytes));
        }
        return handlers;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("OTel ingest plugin is {}", enabled ? "enabled" : "disabled");
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        indexingPressure.set(services.indexingPressure());
        registry.set(
            new OTelIndexTemplateRegistry(settings, clusterService, services.threadPool(), services.client(), services.xContentRegistry())
        );
        if (enabled) {
            OTelIndexTemplateRegistry registryInstance = registry.get();
            registryInstance.setEnabled(OTEL_DATA_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return List.of();
    }

    @Override
    public void close() {
        registry.get().close();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(OTEL_DATA_REGISTRY_ENABLED, HISTOGRAM_FIELD_TYPE_SETTING);
    }

    @Override
    public Collection<ActionHandler> getActions() {
        List<ActionHandler> handlers = new ArrayList<>(3);
        handlers.add(new ActionHandler(OTLPMetricsTransportAction.TYPE, OTLPMetricsTransportAction.class));
        if (OTLP_TRACES_ENABLED) {
            handlers.add(new ActionHandler(OTLPTracesTransportAction.TYPE, OTLPTracesTransportAction.class));
        }
        if (OTLP_LOGS_ENABLED) {
            handlers.add(new ActionHandler(OTLPLogsTransportAction.TYPE, OTLPLogsTransportAction.class));
        }
        return handlers;
    }
}
