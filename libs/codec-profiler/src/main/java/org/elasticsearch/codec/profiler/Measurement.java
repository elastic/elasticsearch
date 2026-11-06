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
 * An immutable result of one metric: the definition it was recorded under and what was recorded. It refers to no live
 * collector, so it can be kept, copied to another thread, or sent to another node. The definition travels with the
 * values, so two results of one identity can be merged only when they mean the same thing.
 */
public sealed interface Measurement permits CounterMeasurement, TimerMeasurement {

    /** The definition the values were recorded under. */
    Metric definition();

    /** The identity of the definition. */
    default Metric.Id id() {
        return definition().id();
    }
}
