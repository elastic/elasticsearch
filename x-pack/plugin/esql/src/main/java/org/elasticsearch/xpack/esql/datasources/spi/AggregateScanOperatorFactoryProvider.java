/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.operator.SourceOperator;

/**
 * Functional interface for creating aggregate-scan source operator factories. Returned by
 * {@link ExternalSourceFactory#aggregateScanOperatorFactory()} for formats whose readers
 * implement {@link AggregateScanReader}.
 * <p>
 * The factory wires the format-specific {@link StorageProvider} and {@link FormatReader}
 * for a given context and returns a {@link SourceOperator.SourceOperatorFactory} that
 * iterates {@link AggregateScanReader#scanForAggregates} per split.
 */
@FunctionalInterface
public interface AggregateScanOperatorFactoryProvider {

    SourceOperator.SourceOperatorFactory create(AggregateScanOperatorContext context);
}
