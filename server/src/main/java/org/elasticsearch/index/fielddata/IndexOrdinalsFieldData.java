/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;

/**
 * Specialization of {@link IndexFieldData} for data that is indexed with ordinals.
 */
public interface IndexOrdinalsFieldData extends IndexFieldData.Global<LeafOrdinalsFieldData> {

    /**
     * Load a global view of the ordinals for the given {@link IndexReader},
     * potentially from a cache.
     */
    @Override
    IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader);

    /**
     * Load a global view of the ordinals for the given {@link IndexReader}.
     */
    @Override
    IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception;

    /**
     * Returns the underlying {@link OrdinalMap} for this fielddata
     * or null if global ordinals are not needed (constant value or single segment).
     */
    OrdinalMap getOrdinalMap();

    /**
     * Whether this field data is able to provide a mapping between global and segment ordinals,
     * by returning the underlying {@link OrdinalMap}. If this method returns false, then calling
     * {@link #getOrdinalMap} will result in an {@link UnsupportedOperationException}.
     */
    boolean supportsGlobalOrdinalsMapping();
}
