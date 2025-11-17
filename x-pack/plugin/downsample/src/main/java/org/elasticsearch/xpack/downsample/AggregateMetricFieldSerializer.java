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

/**
 * This serialiser can be used to convert a list of producers to an aggregate metric double field. The producer should produce an
 * aggregate metric double or a sub metric of one, any other producers will trigger an error.
 */
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
            if (fieldProducer.isEmpty()) {
                continue;
            }
            switch (fieldProducer) {
                case MetricFieldProducer.AggregateGaugeMetricFieldProducer producer -> {
                    builder.field("max", producer.max);
                    builder.field("min", producer.min);
                    builder.field("sum", producer.sum.value());
                    builder.field("value_count", producer.count);
                }
                case MetricFieldProducer.AggregateSubMetricFieldProducer producer -> {
                    switch (producer.metric) {
                        case max -> builder.field("max", producer.max);
                        case min -> builder.field("min", producer.min);
                        case sum -> builder.field("sum", producer.sum.value());
                        case value_count -> builder.field("value_count", producer.count);
                    }
                }
                case LastValueFieldProducer.AggregateSubMetricFieldProducer lastValueFieldProducer -> {
                    Object lastValue = lastValueFieldProducer.lastValue();
                    if (lastValue != null) {
                        builder.field(lastValueFieldProducer.subMetric(), lastValue);
                    }
                }
                default -> throw new IllegalStateException(
                    "Unexpected field producer class: " + fieldProducer.getClass().getSimpleName() + " for " + name + " field"
                );
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
