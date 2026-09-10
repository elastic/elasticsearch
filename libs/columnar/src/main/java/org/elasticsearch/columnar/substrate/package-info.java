/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * The ColumNAR binary substrate: type-agnostic on-disk framing that addresses documents and frames
 * bytes, knowing nothing about numerics, keywords, or ordinals.
 *
 * <p>The column-iterator layer records which documents hold a value and maps a document to its 0-based
 * rank; a dense field stores no per-document data, a sparse field is addressed compactly,
 * and it supplies the {@link DocIdSetIterator#intoBitSet} fast path. {@link BlockBytesCodec} is the
 * terminal byte-stream stage applied to a column's encoded blocks.
 */
package org.elasticsearch.columnar.substrate;

import org.apache.lucene.search.DocIdSetIterator;
