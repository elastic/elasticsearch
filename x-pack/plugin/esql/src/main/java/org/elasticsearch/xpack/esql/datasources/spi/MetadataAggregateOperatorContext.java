/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.ExternalSliceQueue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Context for creating a metadata-aggregate operator factory. Mirrors the role of
 * {@link SourceOperatorContext} but for the runtime metadata-aggregate path used by
 * {@code ExternalMetadataAggregateExec}: each split is dispatched to a driver, the operator
 * asks the format reader for per-file aggregate metadata (row count / null-count / min /
 * max) without scanning row data, and emits one intermediate-shape page per split.
 * <p>
 * This context intentionally omits scan-only fields (batch size, push filter, error policy,
 * row limit, parsing parallelism) since metadata-only reads do not need them.
 *
 * @param sourceType             the format identifier (e.g. "parquet", "parquet-rs")
 * @param path                   the original (possibly globbed) source path; per-file paths
 *                               come from the splits in {@code sliceQueue}
 * @param config                 reader/storage configuration
 * @param sliceQueue             the queue of splits to process; never {@code null}
 * @param aggregates             the list of {@code Alias(Count|Min|Max(...))} expressions to
 *                               compute, mirroring the parent {@code AggregateExec.aggregates()}
 * @param intermediateAttributes output shape — must match the parent
 *                               {@code AggregateExec.intermediateAttributes()}
 * @param columnsToProbe         column names whose null-count/min/max are needed; readers may
 *                               return stats for additional columns but at minimum cover these
 * @param executor               compute executor (typically {@code esql_worker})
 * @param fileReadExecutor       executor for blocking metadata reads (typically {@code generic})
 *                               so they don't starve compute drivers
 */
public record MetadataAggregateOperatorContext(
    String sourceType,
    StoragePath path,
    Map<String, Object> config,
    ExternalSliceQueue sliceQueue,
    List<NamedExpression> aggregates,
    List<Attribute> intermediateAttributes,
    List<String> columnsToProbe,
    Executor executor,
    Executor fileReadExecutor
) {
    public MetadataAggregateOperatorContext {
        Check.notNull(sourceType, "sourceType cannot be null");
        Check.notNull(path, "path cannot be null");
        Check.notNull(sliceQueue, "sliceQueue cannot be null");
        Check.notNull(executor, "executor cannot be null");
        config = config != null ? Map.copyOf(config) : Map.of();
        aggregates = List.copyOf(aggregates);
        intermediateAttributes = List.copyOf(intermediateAttributes);
        columnsToProbe = columnsToProbe != null ? List.copyOf(columnsToProbe) : List.of();
    }
}
