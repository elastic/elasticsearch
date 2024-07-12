/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.lucene.util.Counter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

import java.io.Closeable;
import java.util.function.Supplier;

public interface MetricsRegistry extends Closeable {

    /**
     * Creates the counter.
     * @param name name of the counter.
     * @param description any description about the metric.
     * @param unit unit of the metric.
     * @return counter.
     */
    Counter createCounter(String name, String description, String unit);

    /**
     * Creates the upDown counter.
     * @param name name of the upDown counter.
     * @param description any description about the metric.
     * @param unit unit of the metric.
     * @return counter.
     */
    Counter createUpDownCounter(String name, String description, String unit);

    /**
     * Creates the histogram type of Metric. Implementation framework will take care
     * of the bucketing strategy.
     *
     * @param name        name of the histogram.
     * @param description any description about the metric.
     * @param unit        unit of the metric.
     * @return histogram.
     */
    Histogram createHistogram(String name, String description, String unit);

    /**
     * Creates the Observable Gauge type of Metric. Where the value provider will be called at a certain frequency
     * to capture the value.
     *
     * @param name          name of the observable gauge.
     * @param description   any description about the metric.
     * @param unit          unit of the metric.
     * @param valueProvider value provider.
     * @param tags          attributes/dimensions of the metric.
     * @return closeable to dispose/close the Gauge metric.
     */
    Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags);

    /**
     * Creates the Observable Gauge type of Metric. Where the value provider will be called at a certain frequency
     * to capture the value.
     *
     * @param name        name of the observable gauge.
     * @param description any description about the metric.
     * @param unit        unit of the metric.
     * @param value       value provider.
     * @return closeable to dispose/close the Gauge metric.
     */
    Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value);

}

