/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.prometheus.rest.PrometheusInstantQueryRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusLabelValuesRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusLabelsRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusMetadataRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryRangeRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusRemoteWriteTransportAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusSeriesRestAction;
import org.elasticsearch.xpack.prometheus.rest.PrometheusStatusBuildInfoRestAction;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class PrometheusPlugin extends Plugin implements ActionPlugin {

    public static final FeatureFlag METRIC_EXEMPLARS_FEATURE_FLAG = new FeatureFlag("metric_exemplars");

    // Controls enabling the index template registry.
    // This setting will be ignored if the plugin is disabled.
    static final Setting<Boolean> PROMETHEUS_REGISTRY_ENABLED = Setting.boolSetting(
        "xpack.prometheus.registry.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The default and maximum timeout of PromQL queries run through the {@code query} and {@code query_range} endpoints, mirroring
     * Prometheus' {@code -query.timeout} flag. A per-request {@code timeout} parameter can only lower it. {@code -1} disables it.
     * {@code 0} is rejected because in Prometheus it makes every query time out immediately, the opposite of disabling it.
     */
    static final Setting<TimeValue> PROMETHEUS_QUERY_TIMEOUT = Setting.timeSetting(
        "xpack.prometheus.query.timeout",
        TimeValue.timeValueMinutes(2),
        value -> {
            if (value.duration() == 0) {
                throw new IllegalArgumentException(
                    "[xpack.prometheus.query.timeout] must be positive or -1 to disable the timeout, got [" + value + "]"
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final SetOnce<PrometheusIndexTemplateRegistry> indexTemplateRegistry = new SetOnce<>();
    private final SetOnce<IndexingPressure> indexingPressure = new SetOnce<>();
    private final SetOnce<Recycler<BytesRef>> recycler = new SetOnce<>();
    private final boolean enabled;
    private volatile TimeValue queryTimeout;
    private final long maxProtobufContentLengthBytes;

    public PrometheusPlugin(Settings settings) {
        this.enabled = XPackSettings.PROMETHEUS_ENABLED.get(settings);
        this.maxProtobufContentLengthBytes = HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.get(settings).getBytes();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        indexingPressure.set(services.indexingPressure());
        recycler.set(services.bigArrays().bytesRefRecycler());
        queryTimeout = PROMETHEUS_QUERY_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PROMETHEUS_QUERY_TIMEOUT, value -> queryTimeout = value);
        indexTemplateRegistry.set(
            new PrometheusIndexTemplateRegistry(
                settings,
                clusterService,
                services.threadPool(),
                services.client(),
                services.xContentRegistry(),
                services.featureService()
            )
        );
        if (enabled) {
            PrometheusIndexTemplateRegistry registryInstance = indexTemplateRegistry.get();
            registryInstance.setEnabled(PROMETHEUS_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return List.of();
    }

    @Override
    public void close() {
        if (indexTemplateRegistry.get() != null) {
            indexTemplateRegistry.get().close();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(PROMETHEUS_REGISTRY_ENABLED, PROMETHEUS_QUERY_TIMEOUT);
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (enabled) {
            assert indexingPressure.get() != null : "indexing pressure must be set if plugin is enabled";
            return List.of(
                new PrometheusRemoteWriteRestAction(indexingPressure.get(), maxProtobufContentLengthBytes, recycler.get()),
                new PrometheusSeriesRestAction(),
                new PrometheusQueryRangeRestAction(() -> queryTimeout),
                new PrometheusInstantQueryRestAction(() -> queryTimeout),
                new PrometheusLabelsRestAction(),
                new PrometheusLabelValuesRestAction(),
                new PrometheusMetadataRestAction(),
                new PrometheusStatusBuildInfoRestAction()
            );
        }
        return List.of();
    }

    @Override
    public Collection<ActionHandler> getActions() {
        if (enabled) {
            return List.of(new ActionHandler(PrometheusRemoteWriteTransportAction.TYPE, PrometheusRemoteWriteTransportAction.class));
        }
        return List.of();
    }
}
