/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

/**
 * Opaque wrapper around a native DataFusion {@code Expr} handle representing a pushed filter.
 * <p>
 * The handle is a pointer to a heap-allocated {@code Box<Expr>} on the Rust side.
 * Ownership is transferred to {@link DataFusionBridge#openReader} when the reader opens.
 */
record DataFusionPushedFilter(long exprHandle) {}
