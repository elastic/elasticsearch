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

    private final SourceLookup sourceLookup;
    private final MappedFields fieldTypeLookup;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup;

    /**
     * Create the top level field lookup for a search request. Provides a way to look up fields from  doc_values,
     * stored fields, or _source.
     */
    public SearchLookup(Function<String, MappedFieldType> fieldTypeLookup,
                        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup) {
        this.fieldTypeLookup = new MappedFields(fieldTypeLookup);
        this.sourceLookup = new SourceLookup();
        this.fieldDataLookup = fieldDataLookup;
    }

    /**
     * Create a new {@link SearchLookup} that looks fields up the same as the one provided as argument,
     * while also tracking field references starting from the provided field name. It detects cycles
     * and prevents resolving fields that depend on more than {@link #MAX_FIELD_CHAIN_DEPTH} fields.
     * @param searchLookup the existing lookup to create a new one from
     * @param field        the field to exclude from further field lookups
     */
    private SearchLookup(SearchLookup searchLookup, String field) {
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

    /**
     * The chain of fields for which this lookup was created, used for detecting
     * loops caused by runtime fields referring to other runtime fields. The chain is empty
     * for the "top level" lookup created for the entire search. When a lookup is used to load
     * fielddata for a field, we fork it and make sure the field name name isn't in the chain,
     * then add it to the end. So the lookup for the a field named {@code a} will be {@code ["a"]}. If
     * that field looks up the values of a field named {@code b} then
     * {@code b}'s chain will contain {@code ["a", "b"]}.
     */
    private static final class MappedFields {

        final Function<String, MappedFieldType> lookup;
        final Set<String> references;

        MappedFields(Function<String, MappedFieldType> lookup) {
            this.lookup = lookup;
            this.references = Collections.emptySet();
        }

        MappedFields(Function<String, MappedFieldType> lookup, Set<String> references, String field) {
            this.lookup = lookup;
            this.references = new LinkedHashSet<>(references);
            if (this.references.add(field) == false) {
                String message = String.join(" -> ", this.references) + " -> " + field;
                throw new IllegalArgumentException("Cyclic dependency detected while resolving fields: " + message);
            }
            if (this.references.size() > MAX_FIELD_CHAIN_DEPTH) {
                throw new IllegalArgumentException("Field requires resolving too many dependent fields: "
                    + String.join(" -> ", this.references));
            }
        }

        MappedFields excludingField(String field) {
            return new MappedFields(lookup, this.references, field);
        }

        MappedFieldType get(String field) {
            return lookup.apply(field);
        }

    }
}
