/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import com.dynatrace.hash4j.hashing.HashStream32;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher32;
import com.dynatrace.hash4j.hashing.Hashing;
import com.google.protobuf.ByteString;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
import org.elasticsearch.xpack.oteldata.otlp.tsid.DataPointTsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.tsid.ResourceTsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.tsid.ScopeTsidFunnel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class DataPointGroupingContext {
    private static final Hasher32 HASHER_32 = Hashing.murmur3_32();

    private final BufferedByteStringAccessor byteStringAccessor;
    private final Map<HashValue128, ResourceGroup> resourceGroups = new HashMap<>();
    private final Set<String> ignoredDataPointMessages = new HashSet<>();

    private int totalDataPoints = 0;
    private int ignoredDataPoints = 0;

    public DataPointGroupingContext(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    public void groupDataPoints(ExportMetricsServiceRequest exportMetricsServiceRequest) {
        List<ResourceMetrics> resourceMetricsList = exportMetricsServiceRequest.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            ResourceGroup resourceGroup = getOrCreateResourceGroup(resourceMetrics);
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                ScopeGroup scopeGroup = resourceGroup.getOrCreateScope(scopeMetrics);
                List<Metric> metricsList = scopeMetrics.getMetricsList();
                for (int k = 0; k < metricsList.size(); k++) {
                    var metric = metricsList.get(k);
                    switch (metric.getDataCase()) {
                        case SUM:
                            scopeGroup.addDataPoints(metric, metric.getSum().getDataPointsList(), DataPoint.Number::new);
                            break;
                        case GAUGE:
                            scopeGroup.addDataPoints(metric, metric.getGauge().getDataPointsList(), DataPoint.Number::new);
                            break;
                        case EXPONENTIAL_HISTOGRAM:
                            scopeGroup.addDataPoints(
                                metric,
                                metric.getExponentialHistogram().getDataPointsList(),
                                DataPoint.ExponentialHistogram::new
                            );
                            break;
                        case HISTOGRAM:
                            scopeGroup.addDataPoints(metric, metric.getHistogram().getDataPointsList(), DataPoint.Histogram::new);
                            break;
                        case SUMMARY:
                            scopeGroup.addDataPoints(metric, metric.getSummary().getDataPointsList(), DataPoint.Summary::new);
                            break;
                        default:
                            ignoredDataPoints++;
                            ignoredDataPointMessages.add("unsupported metric type " + metric.getDataCase());
                            break;
                    }
                }
            }
        }
    }

    /**
     * Consumes all data point groups in the context, removing them from the context.
     *
     * @param consumer the consumer to process each {@link DataPointGroup}
     * @param <E>      the type of exception that can be thrown by the consumer
     * @throws E if the consumer throws an exception
     */
    public <E extends Exception> void consume(CheckedConsumer<DataPointGroup, E> consumer) throws E {
        for (Iterator<ResourceGroup> iterator = resourceGroups.values().iterator(); iterator.hasNext(); ) {
            ResourceGroup resourceGroup = iterator.next();
            // Remove the resource group from the map can help to significantly reduce GC overhead.
            // This avoids that the resource groups are promoted to survivor space when the context is kept alive for a while,
            // for example, when referenced in the bulk response listener.
            iterator.remove();
            resourceGroup.forEach(consumer);
        }
    }

    public int totalDataPoints() {
        return totalDataPoints;
    }

    public int getIgnoredDataPoints() {
        return ignoredDataPoints;
    }

    public String getIgnoredDataPointsMessage() {
        return ignoredDataPointMessages.isEmpty() ? "" : String.join("\n", ignoredDataPointMessages);
    }

    private ResourceGroup getOrCreateResourceGroup(ResourceMetrics resourceMetrics) {
        TsidBuilder resourceTsidBuilder = ResourceTsidFunnel.forResource(byteStringAccessor, resourceMetrics);
        HashValue128 resourceHash = resourceTsidBuilder.hash();
        ResourceGroup resourceGroup = resourceGroups.get(resourceHash);
        if (resourceGroup == null) {
            resourceGroup = new ResourceGroup(resourceMetrics.getResource(), resourceMetrics.getSchemaUrlBytes(), resourceTsidBuilder);
            resourceGroups.put(resourceHash, resourceGroup);
        }
        return resourceGroup;
    }

    class ResourceGroup {
        private final Resource resource;
        private final ByteString resourceSchemaUrl;
        private final TsidBuilder resourceTsidBuilder;
        private final Map<HashValue128, ScopeGroup> scopes;

        ResourceGroup(Resource resource, ByteString resourceSchemaUrl, TsidBuilder resourceTsidBuilder) {
            this.resource = resource;
            this.resourceSchemaUrl = resourceSchemaUrl;
            this.resourceTsidBuilder = resourceTsidBuilder;
            this.scopes = new HashMap<>();
        }

        public ScopeGroup getOrCreateScope(ScopeMetrics scopeMetrics) {
            TsidBuilder scopeTsidBuilder = ScopeTsidFunnel.forScope(byteStringAccessor, scopeMetrics);
            HashValue128 scopeHash = scopeTsidBuilder.hash();
            ScopeGroup scopeGroup = scopes.get(scopeHash);
            if (scopeGroup == null) {
                scopeGroup = new ScopeGroup(this, scopeMetrics.getScope(), scopeMetrics.getSchemaUrlBytes(), scopeTsidBuilder);
                scopes.put(scopeHash, scopeGroup);
            }
            return scopeGroup;
        }

        public <E extends Exception> void forEach(CheckedConsumer<DataPointGroup, E> consumer) throws E {
            for (ScopeGroup scopeGroup : scopes.values()) {
                scopeGroup.forEach(consumer);
            }
        }
    }

    class ScopeGroup {
        private final ResourceGroup resourceGroup;
        private final InstrumentationScope scope;
        private final ByteString scopeSchemaUrl;
        private final TsidBuilder scopeTsidBuilder;
        private final Map<TargetIndex, Map<HashValue128, DataPointGroup>> dataPointGroupsByIndex;

        ScopeGroup(ResourceGroup resourceGroup, InstrumentationScope scope, ByteString scopeSchemaUrl, TsidBuilder scopeTsidBuilder) {
            this.resourceGroup = resourceGroup;
            this.scope = scope;
            this.scopeSchemaUrl = scopeSchemaUrl;
            this.scopeTsidBuilder = scopeTsidBuilder;
            this.dataPointGroupsByIndex = new HashMap<>();
        }

        public <T> void addDataPoints(Metric metric, List<T> dataPoints, BiFunction<T, Metric, DataPoint> createDataPoint) {
            for (int i = 0; i < dataPoints.size(); i++) {
                T dataPoint = dataPoints.get(i);
                addDataPoint(createDataPoint.apply(dataPoint, metric));
            }
        }

        public void addDataPoint(DataPoint dataPoint) {
            totalDataPoints++;
            if (dataPoint.isValid(ignoredDataPointMessages) == false) {
                ignoredDataPoints++;
                return;
            }
            getOrCreateDataPointGroup(dataPoint).addDataPoint(dataPoint);
        }

        private DataPointGroup getOrCreateDataPointGroup(DataPoint dataPoint) {
            TsidBuilder dataPointGroupTsidBuilder = DataPointTsidFunnel.forDataPoint(byteStringAccessor, dataPoint);
            HashValue128 dataPointGroupHash = dataPointGroupTsidBuilder.hash();
            TargetIndex targetIndex = TargetIndex.route(
                "metrics",
                dataPoint.getAttributes(),
                scope.getName(),
                scope.getAttributesList(),
                resourceGroup.resource.getAttributesList()
            );
            Map<HashValue128, DataPointGroup> dataPointGroups = dataPointGroupsByIndex.computeIfAbsent(targetIndex, k -> new HashMap<>());
            DataPointGroup dataPointGroup = dataPointGroups.get(dataPointGroupHash);
            if (dataPointGroup == null) {
                dataPointGroup = new DataPointGroup(
                    resourceGroup.resource,
                    resourceGroup.resourceSchemaUrl,
                    resourceGroup.resourceTsidBuilder,
                    scope,
                    scopeSchemaUrl,
                    scopeTsidBuilder,
                    dataPointGroupTsidBuilder,
                    dataPoint.getAttributes(),
                    dataPoint.getUnit(),
                    new ArrayList<>(),
                    targetIndex
                );
                dataPointGroups.put(dataPointGroupHash, dataPointGroup);
            }
            return dataPointGroup;
        }

        public <E extends Exception> void forEach(CheckedConsumer<DataPointGroup, E> consumer) throws E {
            for (Map<HashValue128, DataPointGroup> dataPointGroups : dataPointGroupsByIndex.values()) {
                for (DataPointGroup dataPointGroup : dataPointGroups.values()) {
                    consumer.accept(dataPointGroup);
                }
            }
        }
    }

    public record DataPointGroup(
        Resource resource,
        ByteString resourceSchemaUrl,
        TsidBuilder resourceTsidBuilder,
        InstrumentationScope scope,
        ByteString scopeSchemaUrl,
        TsidBuilder scopeTsidBuilder,
        TsidBuilder dataPointGroupTsidBuilder,
        List<KeyValue> dataPointAttributes,
        String unit,
        List<DataPoint> dataPoints,
        TargetIndex targetIndex
    ) {

        public long getTimestampUnixNano() {
            return dataPoints.getFirst().getTimestampUnixNano();
        }

        public long getStartTimestampUnixNano() {
            return dataPoints.getFirst().getStartTimestampUnixNano();
        }

        public String getMetricNamesHash() {
            HashStream32 metricNamesHash = HASHER_32.hashStream();
            for (int i = 0; i < dataPoints.size(); i++) {
                metricNamesHash.putString(dataPoints.get(i).getMetricName());
            }
            return Integer.toHexString(metricNamesHash.getAsInt());
        }

        public void addDataPoint(DataPoint dataPoint) {
            dataPoints.add(dataPoint);
        }
    }
}
