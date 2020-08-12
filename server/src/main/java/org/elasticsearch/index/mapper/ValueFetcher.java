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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads values for documents in a leaf. 
 */
@FunctionalInterface
public interface ValueFetcher {
    /**
     * Prepare to fetch values for a {@linkplain LeafReaderContext}.
     */
    LeafValueFetcher leaf(LeafReaderContext context) throws IOException;

    /**
     * Fetch values for a particular document.
     */
    @FunctionalInterface
    interface LeafValueFetcher {
        /**
         * Fetch values for a particular document.
         */
        List<?> fetch(int docId) throws IOException;
    }

    /**
     * Build a {@linkplain ValueFetcher} that reads values from the source
     * then calls {@code convertSourceValues} to parse, normalize, and format each value.
     * <p>
     * If the source doesn't contain any values then this will return an
     * empty list and won't call {@code convertSourceValues} at all.
     */
    static ValueFetcher fromSource(
        FieldMapper fieldMapper,
        SearchLookup lookup,
        CheckedFunction<Object, Object, IOException> convertSourceValue
    ) {
        assert fieldMapper.parsesArrayValue() == false;
        return uncheckedFromSource(fieldMapper, lookup, sourceValue -> {
            if (sourceValue instanceof List) {
                List<?> sourceValues = (List<?>) sourceValue;
                List<Object> values = new ArrayList<>(sourceValues.size());
                for (Object value : sourceValues) {
                    Object converted = convertSourceValue.apply(value);
                    if (converted != null) {
                        values.add(converted);
                    }
                }
                return values;
            }
            Object converted = convertSourceValue.apply(sourceValue);
            if (converted != null) {
                return List.of(converted);
            }
            return List.of();
        });
    }

    /**
     * Build a {@linkplain ValueFetcher} that reads values from the source
     * then calls {@code convertSourceValues} to parse, normalize, and format the value.
     * <p>
     * If the source doesn't contain the value then this will return an
     * empty list and won't call {@code convertSourceValues} at all.
     */
    static ValueFetcher fromSourceManualyHandlingLists(
        FieldMapper fieldMapper,
        SearchLookup lookup,
        CheckedFunction<Object, List<?>, IOException> convertSourceValues
    ) {
        assert fieldMapper.parsesArrayValue();
        return uncheckedFromSource(fieldMapper, lookup, convertSourceValues);
    }

    private static ValueFetcher uncheckedFromSource(
        FieldMapper fieldMapper,
        SearchLookup lookup,
        CheckedFunction<Object, List<?>, IOException> convertSourceValues
    ) {
        return ctx -> docId -> {
            /*
             * setSegmentAndDocument out of pure paranoia. The fetch phase should already
             * have positioned us at the right document. So we assert that too.
             */
            boolean alreadyPositioned = lookup.source().setSegmentAndDocument(ctx, docId);
            assert alreadyPositioned;
            Object sourceValue = lookup.source().extractValue(fieldMapper.name(), fieldMapper.nullValue());
            if (sourceValue == null) {
                return List.of();
            }
            return convertSourceValues.apply(sourceValue);
        };
    }

    /**
     * Build a {@linkplain ValueFetcher} that reads values from doc values using the
     * same logic as the doc values fetch phase.
     */
    static ValueFetcher fromDocValues(MappedFieldType ft, SearchLookup lookup, String format) {
        DocValueFormat dvFormat = ft.docValueFormat(format, null);
        IndexFieldData<?> fd = lookup.doc().getForField(ft);
        return ctx -> fd.load(ctx).buildFetcher(dvFormat);
    }
}
