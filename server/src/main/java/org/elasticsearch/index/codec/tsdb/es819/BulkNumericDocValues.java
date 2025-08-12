/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * An es819 doc values specialization that allows bulk loading of values that is optimized in the context of compute engine.
 */
public abstract class BulkNumericDocValues extends NumericDocValues {

    /**
     * Reads the values of all documents in {@code docs}.
     */
    public abstract BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset) throws IOException;

}
