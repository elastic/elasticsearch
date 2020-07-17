/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception;

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
