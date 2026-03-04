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
 */
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    BlockFactory blockFactory
) {

    public QueryRequest withBlockFactory(BlockFactory blockFactory) {
        return new QueryRequest(target, projectedColumns, attributes, config, batchSize, blockFactory);
    }
}
