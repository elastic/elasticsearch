/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class AggregateMetricFieldSerializer extends AbstractRollupFieldProducer {
    private final List<AbstractRollupFieldProducer> producers;

    public AggregateMetricFieldSerializer(String name, List<AbstractRollupFieldProducer> producers) {
        super(name);
        this.producers = producers;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.startObject(name());
            for (AbstractRollupFieldProducer rollupFieldProducer : producers) {
                assert name().equals(rollupFieldProducer.name()) : "producer has a different name";

                if (rollupFieldProducer instanceof MetricFieldProducer metricFieldProducer) {
                    for (MetricFieldProducer.Metric metric : metricFieldProducer.metrics()) {
                        if (metric.get() != null) {
                            builder.field(metric.name(), metric.get());
                        }
                    }
                } else if (rollupFieldProducer instanceof LabelFieldProducer labelFieldProducer) {
                    for (LabelFieldProducer.Label label : labelFieldProducer.labels()) {
                        if (label.get() != null) {
                            builder.field(label.name(), label.get());
                        }
                    }
                }
            }
            builder.endObject();
        }
    }

    @Override
    public boolean isEmpty() {
        for (AbstractRollupFieldProducer p : producers) {
            if (p.isEmpty() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void collect(FormattedDocValues docValues, int docId) throws IOException {
        throw new UnsupportedOperationException("Operation is not supported");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("Operation is not supported");
    }
}
