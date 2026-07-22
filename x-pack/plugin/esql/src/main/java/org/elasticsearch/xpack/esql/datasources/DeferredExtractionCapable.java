/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.operator.DriverContext;

/**
 * Capability marker for a {@link org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory
 * SourceOperatorFactory} that participates in deferred (post-TopN) field extraction. Implementing
 * factories register a per-file {@link org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor
 * ColumnExtractor} and emit a synthetic {@code _rowPosition} column whose values pack the
 * extractor id with a file-local row position (see {@link SourceExtractors#encode(int, long)}).
 * <p>
 * The local execution planner uses this interface to reach the per-driver {@link SourceExtractors}
 * registry of an upstream source factory when planning a paired
 * {@link org.elasticsearch.xpack.esql.plan.physical.ExternalFieldExtractExec ExternalFieldExtractExec}
 * — see {@code LocalExecutionPlanner#planExternalFieldExtract}.
 */
public interface DeferredExtractionCapable {

    /**
     * Returns the per-driver {@link SourceExtractors} registry, lazily creating it if it does not
     * yet exist. The registry is shared between the source operator (which populates it as files
     * open) and the field-extract operator (which reads from it post-TopN). Both must reach the
     * same instance via the same {@link DriverContext}.
     */
    SourceExtractors sourceExtractorsFor(DriverContext driverContext);
}
