/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides a way to look up per-document values from docvalues, stored fields or _source
 */
public class SearchLookup implements SourceProvider {
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
     * fielddata for a field, we fork it and make sure the field name isn't in the chain,
     * then add it to the end. So the lookup for a field named {@code a} will be {@code ["a"]}. If
     * that field looks up the values of a field named {@code b} then
     * {@code b}'s chain will contain {@code ["a", "b"]}.
     */
    private final Set<String> fieldChain;
    private final SourceProvider sourceProvider;
    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final TriFunction<
        MappedFieldType,
        Supplier<SearchLookup>,
        MappedFieldType.FielddataOperation,
        IndexFieldData<?>> fieldDataLookup;
    private final Function<LeafReaderContext, LeafFieldLookupProvider> fieldLookupProvider;

    /**
     * Create a new SearchLookup, using the default stored fields provider
     * @param fieldTypeLookup   defines how to look up field types
     * @param fieldDataLookup   defines how to look up field data
     * @param sourceProvider    defines how to look up the source
     */
    public SearchLookup(
        Function<String, MappedFieldType> fieldTypeLookup,
        TriFunction<MappedFieldType, Supplier<SearchLookup>, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup,
        SourceProvider sourceProvider
    ) {
        this(fieldTypeLookup, fieldDataLookup, sourceProvider, LeafFieldLookupProvider.fromStoredFields());
    }

    /**
     * Create a new SearchLookup, using the default stored fields provider
     * @param fieldTypeLookup       defines how to look up field types
     * @param fieldDataLookup       defines how to look up field data
     * @param sourceProvider        defines how to look up the source
     * @param fieldLookupProvider   defines how to look up stored fields
     */
    public SearchLookup(
        Function<String, MappedFieldType> fieldTypeLookup,
        TriFunction<MappedFieldType, Supplier<SearchLookup>, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataLookup,
        SourceProvider sourceProvider,
        Function<LeafReaderContext, LeafFieldLookupProvider> fieldLookupProvider
    ) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.fieldChain = Collections.emptySet();
        this.sourceProvider = sourceProvider;
        this.fieldDataLookup = fieldDataLookup;
        this.fieldLookupProvider = fieldLookupProvider;
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
        this.sourceProvider = searchLookup.sourceProvider;
        this.fieldTypeLookup = searchLookup.fieldTypeLookup;
        this.fieldDataLookup = searchLookup.fieldDataLookup;
        this.fieldLookupProvider = searchLookup.fieldLookupProvider;
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
        return new LeafSearchLookup(
            context,
            new LeafDocLookup(fieldTypeLookup, this::getForField, context),
            sourceProvider,
            new LeafStoredFieldsLookup(fieldTypeLookup, fieldLookupProvider.apply(context))
        );
    }

    public MappedFieldType fieldType(String fieldName) {
        return fieldTypeLookup.apply(fieldName);
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType, MappedFieldType.FielddataOperation options) {
        return fieldDataLookup.apply(fieldType, () -> forkAndTrackFieldReferences(fieldType.name()), options);
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        return sourceProvider.getSource(ctx, doc);
    }

}
