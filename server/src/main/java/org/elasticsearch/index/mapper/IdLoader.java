/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;

import java.io.IOException;

/**
 * Loads source {@code _id} during a GET or {@code _search}.
 */
public interface IdLoader {
    /**
     * Build the loader for some segment.
     */
    Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException;

    /**
     * Loads {@code _source} from some segment.
     */
    interface Leaf {
        /**
         * Load the {@code _id} for a document.
         * @param storedFields stored fields loader populated with {@code _id} if it
         *                      has been stored
         * @param docId the doc to load
         */
        String id(LeafStoredFieldLoader storedFields, int docId);
    }
}
