/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer.metrics;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class NumberLeafMetricField extends LeafMetricField {
    private final FormattedDocValues formattedDocValues;

    public NumberLeafMetricField(MetricCollector[] metricCollectors, LeafFieldData fieldData) {
        super(metricCollectors);
        this.formattedDocValues = fieldData.getFormattedValues(DocValueFormat.RAW);
    }

    @Override
    public void collectMetric(int docID) throws IOException {
        if (formattedDocValues.advanceExact(docID)) {
            for (int i = 0; i < formattedDocValues.docValueCount(); i++) {
                Object obj = formattedDocValues.nextValue();
                if (obj instanceof Number == false) {
                    throw new IllegalArgumentException("Expected [Number], got [" + obj.getClass() + "]");
                }
                double value = ((Number) obj).doubleValue();
                for (MetricCollector metric : metricCollectors) {
                    metric.collect(value);
                }
            }
        }
    }

    @Override
    public void writeMetrics(int docID, BytesStreamOutput out) throws IOException {
        if (formattedDocValues.advanceExact(docID)) {
            out.writeVInt(formattedDocValues.docValueCount());
            for (int i = 0; i < formattedDocValues.docValueCount(); i++) {
                Object obj = formattedDocValues.nextValue();
                if (obj instanceof Number == false) {
                    throw new IllegalArgumentException("Expected [Number], got [" + obj.getClass() + "]");
                }
                out.writeDouble(((Number) obj).doubleValue());
            }
        } else {
            out.writeVInt(0);
        }
    }
}
