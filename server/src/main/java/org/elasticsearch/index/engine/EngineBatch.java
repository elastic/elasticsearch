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

/**
 * A mapped, engine-ready batch produced by {@link org.elasticsearch.index.mapper.ShardBatchMapper}.
 * Contains the flattened operation record ({@link IndexOperationBatch}) and the assembled Lucene
 * column data ({@link MappedColumns}) for the columnar write path.
 *
 * @param batch   the flattened per-document operation data (uids, sources, seq_no byte arrays, etc.)
 * @param columns the assembled {@link MappedColumns} whose seqNo/primaryTerm/version byte arrays are
 *                aliased by reference from {@code batch}, so engine writes via
 *                {@link MappedColumns#setSeqNo}/{@link MappedColumns#fillPrimaryTerm}/
 *                {@link MappedColumns#setVersion} are immediately visible to the Lucene columns.
 */
public record EngineBatch(IndexOperationBatch batch, MappedColumns columns) {}
