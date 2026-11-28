/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.compute.data.Block;

import java.io.IOException;

/** Loads columns for the docs an {@link ExpressionDocFilter} still has in play. */
public interface DocColumns {
    /** {@code field} for each of {@code docs}, which are ascending doc ids of {@code leaf}, one block position per doc. The caller closes the block. */
    Block load(String field, LeafReaderContext leaf, int[] docs) throws IOException;
}
