/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

/**
 * Context for a single {@link RangeAwareFormatReader#readRange} call. Bundles the per-split
 * parameters and carries an opaque file-level context for cross-split state (e.g. cached
 * parsed footer metadata).
 */
public final class RangeReadContext {
    private final List<String> projectedColumns;
    private final int batchSize;
    private final long rangeStart;
    private final long rangeEnd;
    private final List<Attribute> resolvedAttributes;
    private final ErrorPolicy errorPolicy;
    /**
     * Opaque file-level context, single-writer/single-reader, carried by the owning producer across successive readRange calls.
     */
    @Nullable
    private Object fileContext;

    public RangeReadContext(
        List<String> projectedColumns,
        int batchSize,
        long rangeStart,
        long rangeEnd,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) {
        this.projectedColumns = projectedColumns;
        this.batchSize = batchSize;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.resolvedAttributes = resolvedAttributes;
        this.errorPolicy = errorPolicy;
    }

    public List<String> projectedColumns() {
        return projectedColumns;
    }

    public int batchSize() {
        return batchSize;
    }

    public long rangeStart() {
        return rangeStart;
    }

    public long rangeEnd() {
        return rangeEnd;
    }

    public List<Attribute> resolvedAttributes() {
        return resolvedAttributes;
    }

    public ErrorPolicy errorPolicy() {
        return errorPolicy;
    }

    @Nullable
    public Object fileContext() {
        return fileContext;
    }

    public void setFileContext(@Nullable Object fileContext) {
        this.fileContext = fileContext;
    }
}
