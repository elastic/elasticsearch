/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.Map;

/**
 * Describes a query to execute against a connector.
 * Immutable; use {@link #withBlockFactory} to create a copy bound to a specific driver context.
 * <p>
 * {@code pushedFilter} is an opaque, connector-specific filter object built during local physical optimization and
 * consumed by the connector's operator in the SAME JVM (external sources execute on the coordinator only), so it is
 * <b>never serialized</b>. It may therefore hold a rich JVM object (e.g. the JDBC connector's {@code JdbcPushedQuery}
 * predicate tree). {@code null} means "no WHERE pushdown" — the connector emits its unfiltered scan.
 */
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    int rowLimit,
    Object pushedFilter,
    BlockFactory blockFactory
) {

    public QueryRequest(
        String target,
        List<String> projectedColumns,
        List<Attribute> attributes,
        Map<String, Object> config,
        int batchSize,
        BlockFactory blockFactory
    ) {
        this(target, projectedColumns, attributes, config, batchSize, FormatReader.NO_LIMIT, null, blockFactory);
    }

    /**
     * Convenience constructor with an explicit {@code rowLimit} but no pushed filter. Retained so callers (chiefly
     * tests and connectors that push only projection + LIMIT) need not pass a {@code null} pushed-filter slot.
     */
    public QueryRequest(
        String target,
        List<String> projectedColumns,
        List<Attribute> attributes,
        Map<String, Object> config,
        int batchSize,
        int rowLimit,
        BlockFactory blockFactory
    ) {
        this(target, projectedColumns, attributes, config, batchSize, rowLimit, null, blockFactory);
    }

    public QueryRequest withBlockFactory(BlockFactory blockFactory) {
        return new QueryRequest(target, projectedColumns, attributes, config, batchSize, rowLimit, pushedFilter, blockFactory);
    }
}
