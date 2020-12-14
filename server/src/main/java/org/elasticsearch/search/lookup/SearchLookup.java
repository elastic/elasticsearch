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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class SearchLookup {
    /**
     * The maximum depth of field dependencies.
     * When a runtime field's doc values depends on another runtime field's doc values,
     * which depends on another runtime field's doc values and so on, it can
     * make a very deep stack, which we want to limit.
     */
    private static final int MAX_FIELD_CHAIN_DEPTH = 5;

    /**
     * The chain of fields for which this lookup was created, used for detecting
     * loops caused by runtime fields referring to other runtime fields. The chain is empty
     * for the "top level" lookup created for the entire search. When a lookup is used to load
     * fielddata for a field, we fork it and make sure the field name name isn't in the chain,
     * then add it to the end. So the lookup for the a field named {@code a} will be {@code ["a"]}. If
     * that field looks up the values of a field named {@code b} then
     * {@code b}'s chain will contain {@code ["a", "b"]}.
     */
    private final Set<String> fieldChain;
    private final DocLookup docMap;
    private final SourceLookup sourceLookup;
    private final StoredFieldsLookup storedFieldsLookup;
    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup;

    /**
     * Create the top level field lookup for a search request. Provides a way to look up fields from  doc_values,
     * stored fields, or _source.
     */
    public SearchLookup(Function<String, MappedFieldType> fieldTypeLookup,
                        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.fieldChain = Collections.emptySet();
        docMap = new DocLookup(fieldTypeLookup,
            fieldType -> fieldDataLookup.apply(fieldType, () -> forkAndTrackFieldReferences(fieldType.name())));
        sourceLookup = new SourceLookup();
        storedFieldsLookup = new StoredFieldsLookup(fieldTypeLookup);
        this.fieldDataLookup = fieldDataLookup;
    }

    /**
     * Create a new {@link SearchLookup} that looks fields up the same as the one provided as argument,
     * while also tracking field references starting from the provided field name. It detects cycles
     * and prevents resolving fields that depend on more than {@link #MAX_FIELD_CHAIN_DEPTH} fields.
     * @param searchLookup the existing lookup to create a new one from
     * @param fieldChain the chain of fields that required the field currently being loaded
     */
    private SearchLookup(SearchLookup searchLookup, Set<String> fieldChain) {
        this.fieldChain = Collections.unmodifiableSet(fieldChain);
        this.docMap = new DocLookup(searchLookup.fieldTypeLookup,
            fieldType -> searchLookup.fieldDataLookup.apply(fieldType, () -> forkAndTrackFieldReferences(fieldType.name())));
        this.sourceLookup = searchLookup.sourceLookup;
        this.storedFieldsLookup = searchLookup.storedFieldsLookup;
        this.fieldTypeLookup = searchLookup.fieldTypeLookup;
        this.fieldDataLookup = searchLookup.fieldDataLookup;
    }

    /**
     * Creates a copy of the current {@link SearchLookup} that looks fields up in the same way, but also tracks field references
     * in order to detect cycles and prevent resolving fields that depend on more than {@link #MAX_FIELD_CHAIN_DEPTH} other fields.
     * @param field the field being referred to, for which fielddata needs to be loaded
     * @return the new lookup
     * @throws IllegalArgumentException if a cycle is detected in the fields required to build doc values, or if the field
     * being resolved depends on more than {@link #MAX_FIELD_CHAIN_DEPTH}
     */
    public final SearchLookup forkAndTrackFieldReferences(String field) {
        Objects.requireNonNull(field, "field cannot be null");
        Set<String> newFieldChain = new LinkedHashSet<>(fieldChain);
        if (newFieldChain.add(field) == false) {
            String message = String.join(" -> ", newFieldChain) + " -> " + field;
            throw new IllegalArgumentException("Cyclic dependency detected while resolving runtime fields: " + message);
        }
        if (newFieldChain.size() > MAX_FIELD_CHAIN_DEPTH) {
            throw new IllegalArgumentException("Field requires resolving too many dependent fields: " + String.join(" -> ", newFieldChain));
        }
        return new SearchLookup(this, newFieldChain);
    }

    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(context,
                docMap.getLeafDocLookup(context),
                sourceLookup,
                storedFieldsLookup.getLeafFieldsLookup(context));
    }

    public DocLookup doc() {
        return docMap;
    }

    public SourceLookup source() {
        return sourceLookup;
    }
}
