/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

/**
 * Low level abstraction for es819 doc values to allow for bulk reading doc values into compute engine's block builders.
 */
public interface BulkReader {

    /**
     * Appends values into the provided builder for the specified docs from the specified offset.
     */
    void bulkRead(BlockLoader.SingletonBulkLongBuilder builder, BlockLoader.Docs docs, int offset) throws IOException;

    /**
     * @return the current docid of this bulk reader.
     */
    int docID();

}
