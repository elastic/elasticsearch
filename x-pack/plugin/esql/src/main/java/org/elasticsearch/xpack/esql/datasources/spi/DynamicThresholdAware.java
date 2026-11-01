/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Capability marker for {@link FormatReader} implementations that can use a live TopN threshold.
 * <p>
 * Presence of this interface is the capability signal: callers use {@code instanceof
 * DynamicThresholdAware} rather than a separate flag. Readers that cannot skip data by min/max
 * statistics simply omit it.
 */
public interface DynamicThresholdAware {
    /**
     * Returns a reader copy that consults {@code threshold} at natural skip boundaries.
     */
    FormatReader withDynamicThreshold(DynamicThreshold threshold);
}
