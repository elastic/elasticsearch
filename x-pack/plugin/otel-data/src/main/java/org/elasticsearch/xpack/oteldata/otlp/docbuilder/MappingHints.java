/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import java.util.List;

/**
 * Represents mapping hints that can be used to influence how data is indexed in Elasticsearch.
 * These hints can be provided by users via data point attributes.
 *
 * @param aggregateMetricDouble Indicates that the metric should be mapped as an aggregate_metric_double.
 *                              This hint is available for histogram and exponential histogram metrics.
 * @param docCount              Indicates that the metric should be mapped with a _doc_count field.
 *                              This hint is available for all metric types.
 *                              When used for a histogram, exponential histogram, or summary metric,
 *                              the _doc_count field will be populated with the number of total counts.
 *                              It is not recommended to use this hint for multiple metrics that are grouped together
 *                              into the same document.
 *                              In these cases, the behavior is undefined but does not lead to data loss.
 */
public record MappingHints(boolean aggregateMetricDouble, boolean docCount) {
    public static final String MAPPING_HINTS = "elasticsearch.mapping.hints";

    private static final MappingHints EMPTY = new MappingHints(false, false);
    private static final String AGGREGATE_METRIC_DOUBLE = "aggregate_metric_double";
    private static final String DOC_COUNT = "_doc_count";

    public static MappingHints fromAttributes(List<KeyValue> attributes) {
        boolean aggregateMetricDouble = false;
        boolean docCount = false;
        for (int i = 0, attributesSize = attributes.size(); i < attributesSize; i++) {
            KeyValue attribute = attributes.get(i);
            if (attribute.getKey().equals(MAPPING_HINTS)) {
                if (attribute.getValue().hasArrayValue()) {
                    List<AnyValue> valuesList = attribute.getValue().getArrayValue().getValuesList();
                    for (int j = 0, valuesListSize = valuesList.size(); j < valuesListSize; j++) {
                        AnyValue hint = valuesList.get(j);
                        if (hint.hasStringValue()) {
                            String value = hint.getStringValue();
                            if (value.equals(AGGREGATE_METRIC_DOUBLE)) {
                                aggregateMetricDouble = true;
                            } else if (value.equals(DOC_COUNT)) {
                                docCount = true;
                            }
                        }
                    }
                }
                return new MappingHints(aggregateMetricDouble, docCount);
            }
        }
        return EMPTY;
    }
}
