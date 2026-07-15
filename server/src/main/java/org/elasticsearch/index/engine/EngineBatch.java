/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;

import java.util.List;

/**
 * A chunk of the bulk batch-indexing fast path, as handed from {@code ShardBatchIndexer}/
 * {@code ShardBatchMapper} to the engine. Bundles what used to be threaded through the engine as
 * two separate parameters.
 */
public record EngineBatch(List<Engine.Index> operations, SourceBatch sourceBatch, MappedColumns columns) {}
