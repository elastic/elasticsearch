/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.sourcebatch.SliceableColumns;
import org.elasticsearch.sourcebatch.SourceBatch;

import java.util.List;

/**
 * A chunk of the bulk batch-indexing fast path, as handed from {@code ShardBatchIndexer}/
 * {@code ShardBatchMapper} to the engine. Bundles what used to be threaded through the engine as
 * two separate parameters:
 * <ul>
 *     <li>{@link #operations()} — one {@link Engine.Index} per document, in batch order. Always
 *     present, whether or not a real columnar mapping was produced (e.g. the replica path builds
 *     these via the traditional per-document parser and still batches them for one translog write).</li>
 *     <li>{@link #sourceBatch()} — the underlying {@link SourceBatch} slice, used for its raw
 *     {@code data()} bytes when writing the single {@code Translog.IndexBatch} record.</li>
 *     <li>{@link #columns()} — the {@link SliceableColumns} that assembles the batch-mapped columns
 *     into a real Lucene {@code ColumnBatch} for {@code IndexWriter#addBatch}. The engine slices
 *     this per sub-batch (determined by version-lock acquisition) before filling engine-assigned
 *     values and calling {@code IndexWriter#addBatch}. {@code null} unless
 *     {@code ShardBatchMapper.mapColumnBatch} actually engaged the columnar mapping path for this
 *     chunk; the engine falls back to per-operation Lucene indexing whenever this is {@code null}.</li>
 * </ul>
 */
public record EngineBatch(List<Engine.Index> operations, SourceBatch sourceBatch, @Nullable SliceableColumns columns) {}
