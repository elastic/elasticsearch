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

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class SearchLookup {

    private final SourceLookup sourceLookup;
    private final TrackingMappedFieldsLookup fieldTypeLookup;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup;

    /**
     * Create the top level field lookup for a search request.
     *
     * Provides a way to look up fields from doc_values, stored fields, or _source.
     */
    public SearchLookup(Function<String, MappedFieldType> fieldTypeLookup,
                        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup) {
        this.fieldTypeLookup = new TrackingMappedFieldsLookup(fieldTypeLookup);
        this.sourceLookup = new SourceLookup();
        this.fieldDataLookup = fieldDataLookup;
    }

    /**
     * Create a new {@link SearchLookup} that looks fields up the same as the one provided as argument,
     * while also tracking field references starting from the provided field name. It detects cycles
     * and prevents resolving fields that depend on more than
     * {@link TrackingMappedFieldsLookup#MAX_FIELD_CHAIN_DEPTH} fields.
     * @param searchLookup the existing lookup to create a new one from
     * @param field        the field to exclude from further field lookups
     */
    public SearchLookup(SearchLookup searchLookup, String field) {
        this.sourceLookup = searchLookup.sourceLookup;
        this.fieldTypeLookup = searchLookup.fieldTypeLookup.excludingField(field);
        this.fieldDataLookup = searchLookup.fieldDataLookup;
    }

    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(context,
                new LeafDocLookup(fieldTypeLookup::get, this::getForField, context),
                sourceLookup,
                new LeafStoredFieldsLookup(fieldTypeLookup::get, (doc, visitor) -> context.reader().document(doc, visitor)));
    }

    public MappedFieldType fieldType(String fieldName) {
        return fieldTypeLookup.get(fieldName);
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return fieldDataLookup.apply(fieldType, () -> new SearchLookup(this, fieldType.name()));
    }

    public SourceLookup source() {
        return sourceLookup;
    }

}
