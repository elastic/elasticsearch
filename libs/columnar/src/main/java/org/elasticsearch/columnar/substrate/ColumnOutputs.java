/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexOutput;

/**
 * Where a column is written, one file per concern and each shared by every field of the segment.
 *
 * <ul>
 *   <li>{@code data} holds the values.</li>
 *   <li>{@code addressing} holds what is kept per document: which documents have a value, how many slots
 *       each holds, where a document's values begin.</li>
 *   <li>{@code navigation} holds what is kept per block or per chunk and locates everything else; small, and
 *       loaded up front.</li>
 * </ul>
 */
public record ColumnOutputs(IndexOutput data, IndexOutput addressing, IndexOutput navigation) {}
