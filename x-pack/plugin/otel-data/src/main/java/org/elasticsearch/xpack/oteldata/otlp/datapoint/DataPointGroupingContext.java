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

import com.google.protobuf.ByteString;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3.Hash128;
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

    private final BufferedByteStringAccessor byteStringAccessor;
    private final Map<Hash128, ResourceGroup> resourceGroups = new HashMap<>();
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
                            ignoredDataPoints += metric.getExponentialHistogram().getDataPointsCount();
                            ignoredDataPointMessages.add("Exponential histogram is not supported yet. Dropping " + metric.getName());
                            break;
                        case HISTOGRAM:
                            ignoredDataPoints += metric.getHistogram().getDataPointsCount();
                            ignoredDataPointMessages.add("Histogram is not supported yet. Dropping " + metric.getName());
                            break;
                        case SUMMARY:
                            ignoredDataPoints += metric.getSummary().getDataPointsList().size();
                            ignoredDataPointMessages.add("Summary is not supported yet. Dropping " + metric.getName());
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
        for (Iterator<ResourceGroup> iterator = resourceGroups.values().iterator(); iterator.hasNext();) {
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
        Hash128 resourceHash = resourceTsidBuilder.hash();
        ResourceGroup resourceGroup = resourceGroups.get(resourceHash);
        if (resourceGroup == null) {
            resourceGroup = new ResourceGroup(resourceMetrics.getResource(), resourceMetrics.getSchemaUrlBytes());
            resourceGroups.put(resourceHash, resourceGroup);
        }
        return resourceGroup;
    }

    class ResourceGroup {
        private final Resource resource;
        private final ByteString resourceSchemaUrl;
        private final Map<Hash128, ScopeGroup> scopes;

        ResourceGroup(Resource resource, ByteString resourceSchemaUrl) {
            this.resource = resource;
            this.resourceSchemaUrl = resourceSchemaUrl;
            this.scopes = new HashMap<>();
        }

        public ScopeGroup getOrCreateScope(ScopeMetrics scopeMetrics) {
            TsidBuilder scopeTsidBuilder = ScopeTsidFunnel.forScope(byteStringAccessor, scopeMetrics);
            Hash128 scopeHash = scopeTsidBuilder.hash();
            ScopeGroup scopeGroup = scopes.get(scopeHash);
            if (scopeGroup == null) {
                scopeGroup = new ScopeGroup(this, scopeMetrics.getScope(), scopeMetrics.getSchemaUrlBytes());
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
        // index -> timestamp -> dataPointGroupHash -> DataPointGroup
        private final Map<String, Map<Hash128, Map<Hash128, DataPointGroup>>> dataPointGroupsByIndexAndTimestamp;

        ScopeGroup(ResourceGroup resourceGroup, InstrumentationScope scope, ByteString scopeSchemaUrl) {
            this.resourceGroup = resourceGroup;
            this.scope = scope;
            this.scopeSchemaUrl = scopeSchemaUrl;
            this.dataPointGroupsByIndexAndTimestamp = new HashMap<>();
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
            Hash128 dataPointGroupHash = dataPointGroupTsidBuilder.hash();
            // in addition to the fields that go into the _tsid, we also need to group by timestamp and start timestamp
            Hash128 timestamp = new Hash128(dataPoint.getTimestampUnixNano(), dataPoint.getStartTimestampUnixNano());
            // TODO determine based on attributes and scope name
            String targetIndex = "metrics-generic.otel-default";
            var dataPointGroupsByTimestamp = dataPointGroupsByIndexAndTimestamp.computeIfAbsent(targetIndex, k -> new HashMap<>());
            var dataPointGroups = dataPointGroupsByTimestamp.computeIfAbsent(timestamp, k -> new HashMap<>());
            DataPointGroup dataPointGroup = dataPointGroups.get(dataPointGroupHash);
            if (dataPointGroup == null) {
                dataPointGroup = new DataPointGroup(
                    resourceGroup.resource,
                    resourceGroup.resourceSchemaUrl,
                    scope,
                    scopeSchemaUrl,
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
            for (var dataPointGroupsByTime : dataPointGroupsByIndexAndTimestamp.values()) {
                for (var dataPointGroups : dataPointGroupsByTime.values()) {
                    for (DataPointGroup dataPointGroup : dataPointGroups.values()) {
                        consumer.accept(dataPointGroup);
                    }
                }
            }
        }
    }

    public static final class DataPointGroup {
        private final Resource resource;
        private final ByteString resourceSchemaUrl;
        private final InstrumentationScope scope;
        private final ByteString scopeSchemaUrl;
        private final List<KeyValue> dataPointAttributes;
        private final String unit;
        private final List<DataPoint> dataPoints;
        private final String targetIndex;
        private String metricNamesHash;

        public DataPointGroup(
            Resource resource,
            ByteString resourceSchemaUrl,
            InstrumentationScope scope,
            ByteString scopeSchemaUrl,
            List<KeyValue> dataPointAttributes,
            String unit,
            List<DataPoint> dataPoints,
            String targetIndex
        ) {
            this.resource = resource;
            this.resourceSchemaUrl = resourceSchemaUrl;
            this.scope = scope;
            this.scopeSchemaUrl = scopeSchemaUrl;
            this.dataPointAttributes = dataPointAttributes;
            this.unit = unit;
            this.dataPoints = dataPoints;
            this.targetIndex = targetIndex;
        }

        public long getTimestampUnixNano() {
            return dataPoints.getFirst().getTimestampUnixNano();
        }

        public long getStartTimestampUnixNano() {
            return dataPoints.getFirst().getStartTimestampUnixNano();
        }

        public String getMetricNamesHash() {
            if (metricNamesHash == null) {
                BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
                for (int i = 0; i < dataPoints.size(); i++) {
                    hasher.addString(dataPoints.get(i).getMetricName());
                }
                metricNamesHash = Integer.toHexString(hasher.digestHash().hashCode());
            }
            return metricNamesHash;
        }

        public void addDataPoint(DataPoint dataPoint) {
            metricNamesHash = null; // reset the hash when adding a new data point
            dataPoints.add(dataPoint);
        }

        public Resource resource() {
            return resource;
        }

        public ByteString resourceSchemaUrl() {
            return resourceSchemaUrl;
        }

        public InstrumentationScope scope() {
            return scope;
        }

        public ByteString scopeSchemaUrl() {
            return scopeSchemaUrl;
        }

        public List<KeyValue> dataPointAttributes() {
            return dataPointAttributes;
        }

        public String unit() {
            return unit;
        }

        public List<DataPoint> dataPoints() {
            return dataPoints;
        }

        public String targetIndex() {
            return targetIndex;
        }
    }
}
