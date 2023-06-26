/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;

public class AggregateMetricFieldSerializer implements DownsampleFieldSerializer {
    private final Collection<AbstractDownsampleFieldProducer> producers;
    private final String name;

    /**
     * @param name the name of the aggregate_metric_double field as it will be serialized
     *             in the downsampled index
     * @param producers a collection of {@link AbstractDownsampleFieldProducer} instances with the subfields
     *                  of the aggregate_metric_double field.
     */
    public AggregateMetricFieldSerializer(String name, Collection<AbstractDownsampleFieldProducer> producers) {
        this.name = name;
        this.producers = producers;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty()) {
            return;
        }

        builder.startObject(name);
        for (AbstractDownsampleFieldProducer rollupFieldProducer : producers) {
            assert name.equals(rollupFieldProducer.name()) : "producer has a different name";
            if (rollupFieldProducer.isEmpty() == false) {
                if (rollupFieldProducer instanceof MetricFieldProducer metricFieldProducer) {
                    for (MetricFieldProducer.Metric metric : metricFieldProducer.metrics()) {
                        if (metric.get() != null) {
                            builder.field(metric.name(), metric.get());
                        }
                    }
                } else if (rollupFieldProducer instanceof LabelFieldProducer labelFieldProducer) {
                    LabelFieldProducer.Label label = labelFieldProducer.label();
                    if (label.get() != null) {
                        builder.field(label.name(), label.get());
                    }
                }
            }
        }
        builder.endObject();
    }

    private boolean isEmpty() {
        for (AbstractDownsampleFieldProducer p : producers) {
            if (p.isEmpty() == false) {
                return false;
            }
        }
        return true;
    }
}
