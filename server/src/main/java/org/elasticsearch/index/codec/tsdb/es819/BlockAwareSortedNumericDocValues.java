/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.mapper.BlockDocValuesReader.ToDouble;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

public abstract class BlockAwareSortedNumericDocValues extends SortedNumericDocValues {

    public abstract void loadBlock(BlockLoader.LongBuilder builder, BlockLoader.Docs docs, int offset) throws IOException;

    public abstract void loadBlock(BlockLoader.IntBuilder builder, BlockLoader.Docs docs, int offset) throws IOException;

    public abstract void loadBlock(BlockLoader.DoubleBuilder builder, BlockLoader.Docs docs, int offset, ToDouble toDouble)
        throws IOException;

}
