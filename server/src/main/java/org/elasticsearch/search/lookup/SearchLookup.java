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
import org.elasticsearch.index.mapper.MapperService;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class SearchLookup {

    private static final int MAX_FIELD_CHAIN = 5;

    private final DocLookup docMap;
    private final SourceLookup sourceLookup;
    private final FieldsLookup fieldsLookup;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup;

    /**
     * Create the top level field lookup for a search request. Provides a way to look up fields from  doc_values,
     * stored fields, or _source
     */
    public SearchLookup(MapperService mapperService,
                        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup) {
        docMap = new DocLookup(mapperService,
            fieldType -> fieldDataLookup.apply(fieldType, () -> forkAndTrackFieldReferences(fieldType.name())));
        sourceLookup = new SourceLookup();
        fieldsLookup = new FieldsLookup(mapperService);
        this.fieldDataLookup = fieldDataLookup;
    }

    /**
     * Create a new {@link SearchLookup} that looks fields up the same as the one provided as argument,
     * while also tracking field references starting from the provided field name. It detects cycles
     * and prevents resolving fields that depend on more than {@link #MAX_FIELD_CHAIN} fields.
     * @param searchLookup the existing lookup to create a new one from
     * @param field the initial field being referred to
     */
    private SearchLookup(SearchLookup searchLookup, String field) {
        assert searchLookup.fieldDataLookup != null : "SearchLookup can only be forked once!";
        this.docMap = new DocLookup(searchLookup.docMap.mapperService(),
            wrapAndTrackFieldReferences(field, fieldType -> searchLookup.fieldDataLookup.apply(fieldType, () -> this)));
        this.sourceLookup = searchLookup.sourceLookup;
        this.fieldsLookup = searchLookup.fieldsLookup;
        this.fieldDataLookup = null;
    }

    /**
     * Creates a copy of the current {@link SearchLookup} that looks fields up in the same way, but also tracks field
     * references in order to detect cycles and prevent resolving fields that depend on more than {@link #MAX_FIELD_CHAIN} other fields.
     * @param field the initial field being referred to
     * @return the new lookup
     * @throws IllegalArgumentException if a cycle is detected in the fields required to build doc values, or if the field
     * being resolved depends on more than {@link #MAX_FIELD_CHAIN}
     */
    public SearchLookup forkAndTrackFieldReferences(String field) {
        return new SearchLookup(this, field);
    }

    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(context,
                docMap.getLeafDocLookup(context),
                sourceLookup,
                fieldsLookup.getLeafFieldsLookup(context));
    }

    public DocLookup doc() {
        return docMap;
    }

    public SourceLookup source() {
        return sourceLookup;
    }

    private static Function<MappedFieldType, IndexFieldData<?>> wrapAndTrackFieldReferences(String field,
            Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        final Set<String> fields = new LinkedHashSet<>();
        fields.add(field);
        return fieldType -> {
            if (fields.add(fieldType.name()) == false) {
                String message = String.join(" -> ", fields) + " -> " + fieldType.name();
                throw new IllegalArgumentException("Cyclic dependency detected while resolving runtime fields: " + message);
            }
            if (fields.size() > MAX_FIELD_CHAIN) {
                throw new IllegalArgumentException("Field requires resolving too many dependent fields: " + String.join(" -> ", fields));
            }
            return fieldDataLookup.apply(fieldType);
        };
    }
}
