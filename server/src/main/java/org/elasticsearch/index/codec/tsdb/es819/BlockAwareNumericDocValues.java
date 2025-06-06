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
import org.elasticsearch.index.mapper.BlockDocValuesReader;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

public abstract class BlockAwareNumericDocValues extends NumericDocValues {

    public abstract void loadBlock(BlockLoader.LongBuilder builder, BlockLoader.Docs docs) throws IOException;

    public abstract void loadDoc(BlockLoader.LongBuilder builder, int docId) throws IOException;

    public abstract void loadBlock(BlockLoader.IntBuilder builder, BlockLoader.Docs docs) throws IOException;

    public abstract void loadDoc(BlockLoader.IntBuilder builder, int docId) throws IOException;

    public abstract void loadBlock(BlockLoader.DoubleBuilder builder, BlockLoader.Docs docs, BlockDocValuesReader.ToDouble toDouble)
        throws IOException;

    public abstract void loadDoc(BlockLoader.DoubleBuilder builder, int docId, BlockDocValuesReader.ToDouble toDouble) throws IOException;
}
