/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Exemplar;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Keeps exemplar observations independently queryable while preserving the dimensions of their parent metric series.
 */
public class ExemplarDocumentBuilder extends OtelTsdbDocumentBuilder {

    public static final String METRIC_NAME_FIELD = "metric_name";
    public static final String VALUE_FIELD = "value";

    public ExemplarDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    /**
     * Builds one exemplar document.
     */
    public void buildExemplarDocument(
        XContentBuilder builder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        DataPoint dataPoint,
        Exemplar exemplar,
        TargetIndex targetIndex
    ) throws IOException {
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(exemplar.getTimeUnixNano()));
        buildDimensionFields(builder, dataPointGroup, targetIndex);
        builder.field(METRIC_NAME_FIELD, dataPoint.getMetricName());
        buildFilteredAttributes(builder, exemplar.getFilteredAttributesList());
        addHexFieldIfNotEmpty(builder, "trace_id", exemplar.getTraceId());
        addHexFieldIfNotEmpty(builder, "span_id", exemplar.getSpanId());

        builder.field(VALUE_FIELD);
        switch (exemplar.getValueCase()) {
            case AS_DOUBLE -> builder.value(exemplar.getAsDouble());
            case AS_INT -> builder.value(exemplar.getAsInt());
            case VALUE_NOT_SET -> throw new IllegalArgumentException("exemplar has no value");
        }
        builder.endObject();
    }

    private void buildFilteredAttributes(XContentBuilder builder, List<KeyValue> filteredAttributes) throws IOException {
        if (filteredAttributes.isEmpty()) {
            return;
        }
        builder.startObject("filtered_attributes");
        for (int i = 0; i < filteredAttributes.size(); i++) {
            KeyValue attribute = filteredAttributes.get(i);
            builder.field(attribute.getKey());
            buildAnyValue(builder, attribute.getValue());
        }
        builder.endObject();
    }
}
