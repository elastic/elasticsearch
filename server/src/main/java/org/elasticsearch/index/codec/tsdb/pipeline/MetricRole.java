/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Codec-local mirror of {@code org.elasticsearch.index.mapper.TimeSeriesParams.MetricType}.
 * The pipeline package needs to make routing decisions based on a field's TSDB metric
 * role, but the codec layer does not import mapper classes directly. {@code
 * PerFieldFormatSupplier} translates from the mapper enum to this one when building a
 * {@link FieldContext}.
 */
public enum MetricRole {
    GAUGE,
    COUNTER,
    HISTOGRAM,
    POSITION
}
