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

final class AggregateMetricFieldSerializer implements DownsampleFieldSerializer {
    private final Collection<AbstractDownsampleFieldProducer> producers;
    private final String name;

    /**
     * @param name the name of the aggregate_metric_double field as it will be serialized
     *             in the downsampled index
     * @param producers a collection of {@link AbstractDownsampleFieldProducer} instances with the subfields
     *                  of the aggregate_metric_double field.
     */
    AggregateMetricFieldSerializer(String name, Collection<AbstractDownsampleFieldProducer> producers) {
        this.name = name;
        this.producers = producers;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty()) {
            return;
        }

        builder.startObject(name);
        for (AbstractDownsampleFieldProducer fieldProducer : producers) {
            assert name.equals(fieldProducer.name()) : "producer has a different name";
            if (fieldProducer.isEmpty() == false) {
                if (fieldProducer instanceof MetricFieldProducer metricFieldProducer) {
                    if (metricFieldProducer instanceof MetricFieldProducer.GaugeMetricFieldProducer gaugeProducer) {
                        builder.field("max", gaugeProducer.max);
                        builder.field("min", gaugeProducer.min);
                        builder.field("sum", gaugeProducer.sum.value());
                        builder.field("value_count", gaugeProducer.count);
                    } else if (metricFieldProducer instanceof MetricFieldProducer.CounterMetricFieldProducer counterProducer) {
                        builder.field("last_value", counterProducer.lastValue);
                    } else if (metricFieldProducer instanceof MetricFieldProducer.AggregatedGaugeMetricFieldProducer producer) {
                        switch (producer.metric) {
                            case max -> builder.field("max", producer.max);
                            case min -> builder.field("min", producer.min);
                            case sum -> builder.field("sum", producer.sum.value());
                            case value_count -> builder.field("value_count", producer.count);
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                } else if (fieldProducer instanceof LabelFieldProducer labelFieldProducer) {
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
