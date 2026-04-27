/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.operator.SourceOperator;

/**
 * Functional interface for creating metadata-aggregate source operator factories.
 * Returned by {@link ExternalSourceFactory#metadataAggregateOperatorFactory()} for formats
 * whose readers implement {@link MetadataAggregateReader}.
 * <p>
 * The factory wires the format-specific {@link StorageProvider} and {@link FormatReader}
 * for a given context and returns a {@link SourceOperator.SourceOperatorFactory} that
 * emits per-split intermediate-shape aggregate pages.
 */
@FunctionalInterface
public interface MetadataAggregateOperatorFactoryProvider {

    SourceOperator.SourceOperatorFactory create(MetadataAggregateOperatorContext context);
}
