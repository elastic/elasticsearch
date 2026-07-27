/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.sourcebatch.SourceBatch;

import java.util.List;

/**
 * A batch of index operations ready for engine-level processing, produced by the bulk batch
 * indexing path and consumed by {@link Engine#indexBatch}.
 *
 * @param operations  per-document {@link Engine.Index} operations for this batch
 * @param sourceBatch the raw encoded source data backing the operations
 */
public record EngineBatch(List<Engine.Index> operations, SourceBatch sourceBatch) {}
