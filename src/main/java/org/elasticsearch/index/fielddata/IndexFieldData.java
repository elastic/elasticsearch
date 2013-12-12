/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public interface IndexFieldData<FD extends AtomicFieldData> extends IndexComponent {

    public static class CommonSettings {

        /**
         * Should single value cross documents case be optimized to remove ords. Note, this optimization
         * might not be supported by all Field Data implementations, but the ones that do, should consult
         * this method to check if it should be done or not.
         */
        public static boolean removeOrdsOnSingleValue(FieldDataType fieldDataType) {
            return !"always".equals(fieldDataType.getSettings().get("ordinals"));
        }
    }

    /**
     * The field name.
     */
    FieldMapper.Names getFieldNames();

    /**
     * Are the values ordered? (in ascending manner).
     */
    boolean valuesOrdered();

    /**
     * Loads the atomic field data for the reader, possibly cached.
     */
    FD load(AtomicReaderContext context);

    /**
     * Loads directly the atomic field data for the reader, ignoring any caching involved.
     */
    FD loadDirect(AtomicReaderContext context) throws Exception;

    /**
     * Comparator used for sorting.
     */
    XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode);

    /**
     * Clears any resources associated with this field data.
     */
    void clear();

    void clear(IndexReader reader);

    /**
     * Returns the highest ever seen unique values in an atomic reader.
     * @deprecated this method will be removed in version 1.0
     */
    @Deprecated
    long getHighestNumberOfSeenUniqueValues();

    // we need this extended source we we have custom comparators to reuse our field data
    // in this case, we need to reduce type that will be used when search results are reduced
    // on another node (we don't have the custom source them...)
    public abstract class XFieldComparatorSource extends FieldComparatorSource {

        public abstract SortField.Type reducedType();
    }

    interface Builder {

        IndexFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache);
    }

    public interface WithOrdinals<FD extends AtomicFieldData.WithOrdinals> extends IndexFieldData<FD> {

        /**
         * Loads the atomic field data for the reader, possibly cached.
         */
        FD load(AtomicReaderContext context);

        /**
         * Loads directly the atomic field data for the reader, ignoring any caching involved.
         */
        FD loadDirect(AtomicReaderContext context) throws Exception;
    }

}
