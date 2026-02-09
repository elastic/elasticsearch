/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.ExponentialHistogramConverter;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class constructs an Elasticsearch document representation of a metric data point group.
 * It also handles dynamic templates for metrics based on their attributes.
 */
public class MetricDocumentBuilder extends OTelDocumentBuilder {

    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
    private final MappingHints defaultMappingHints;
    private final ExponentialHistogramConverter.BucketBuffer scratch = new ExponentialHistogramConverter.BucketBuffer();

    public MetricDocumentBuilder(BufferedByteStringAccessor byteStringAccessor, MappingHints defaultMappingHints) {
        super(byteStringAccessor);
        this.defaultMappingHints = defaultMappingHints;
    }

    public BytesRef buildMetricDocument(
        XContentBuilder builder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) throws IOException {
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            builder.field("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }
        buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl(), builder);
        buildDataStream(builder, dataPointGroup.targetIndex());
        buildScope(builder, dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());
        buildAttributes(builder, dataPointGroup.dataPointAttributes(), 0);
        if (Strings.hasLength(dataPointGroup.unit())) {
            builder.field("unit", dataPointGroup.unit());
        }
        String metricNamesHash = dataPointGroup.getMetricNamesHash(hasher);
        builder.field("_metric_names_hash", metricNamesHash);

        long docCount = 0;
        builder.startObject("metrics");
        for (int i = 0, dataPointsSize = dataPoints.size(); i < dataPointsSize; i++) {
            DataPoint dataPoint = dataPoints.get(i);
            builder.field(dataPoint.getMetricName());
            MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
            dataPoint.buildMetricValue(mappingHints, builder, scratch);
            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                String metricFieldPath = "metrics." + dataPoint.getMetricName();
                dynamicTemplates.put(metricFieldPath, dynamicTemplate);
                if (dataPointGroup.unit() != null && dataPointGroup.unit().isEmpty() == false) {
                    // Store the unit of the metric in the dynamic template parameters
                    dynamicTemplateParams.put(metricFieldPath, Map.of("unit", dataPointGroup.unit()));
                }
            }
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }
        builder.endObject();
        if (docCount > 0) {
            builder.field("_doc_count", docCount);
        }
        builder.endObject();
        TsidBuilder tsidBuilder = dataPointGroup.tsidBuilder();
        tsidBuilder.addStringDimension("_metric_names_hash", metricNamesHash);
        return tsidBuilder.buildTsid();
    }

}
