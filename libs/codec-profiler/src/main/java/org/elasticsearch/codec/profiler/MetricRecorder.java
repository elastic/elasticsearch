/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

/**
 * The codec-facing interface of the profiler. It hands out the {@link Counter} and {@link Timer} handles of a reader.
 * A codec asks for each handle once, when it builds the component that records. On the read path the codec calls only
 * {@link Counter#add}, {@link Timer#start} and {@link Timer#stop}, so no lookup happens there. Asking for a handle
 * registers the metric, so the metric is present in snapshots before anything is recorded.
 */
public interface MetricRecorder {

    /**
     * The counter for {@code metric}. All readers of one collector that ask for the same metric get the same counter.
     *
     * @throws IllegalStateException when another definition of the identity is registered, or the collector is full
     */
    Counter counter(CounterMetric metric);

    /**
     * The timer for {@code metric}, or {@link Timer#INERT} when the collector does not measure time. An inert timer
     * registers nothing and reads no clock. All readers of one collector that ask for the same metric get the same
     * timer, so the rules of {@link Timer} on overlapping calls apply to all of them.
     *
     * @throws IllegalStateException when another definition of the identity is registered, when the collector measures
     *                               CPU time and another definition holds the identity of the CPU time companion, or
     *                               when the collector is full
     */
    Timer timer(TimerMetric metric);
}
