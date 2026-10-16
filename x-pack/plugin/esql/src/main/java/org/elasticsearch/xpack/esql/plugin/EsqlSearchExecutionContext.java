/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.mapper.flattened.UnmappedSinkFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;

import java.util.Map;

/**
 * An ESQL-specific subclass of {@link SearchExecutionContext} that carries the {@link QueryWarnings} bridge, decides whether the keys
 * of {@code flattened} fields are fields, and finds the unmapped fields an index keeps in its {@code _unmapped} field.
 */
public class EsqlSearchExecutionContext extends SearchExecutionContext {
    private final QueryWarnings queryWarnings;
    private final boolean flattenedKeys;
    private final Map<String, MappedFieldType> unmappedFields;

    public EsqlSearchExecutionContext(SearchExecutionContext base, QueryWarnings queryWarnings) {
        this(base, queryWarnings, false);
    }

    /**
     * @param flattenedKeys whether a name under a {@code flattened} field is a mapped field, the key of that name
     */
    public EsqlSearchExecutionContext(SearchExecutionContext base, QueryWarnings queryWarnings, boolean flattenedKeys) {
        this(base, queryWarnings, flattenedKeys, Map.of());
    }

    private EsqlSearchExecutionContext(
        SearchExecutionContext base,
        QueryWarnings queryWarnings,
        boolean flattenedKeys,
        Map<String, MappedFieldType> unmappedFields
    ) {
        super(base);
        this.queryWarnings = queryWarnings;
        this.flattenedKeys = flattenedKeys;
        this.unmappedFields = unmappedFields;
    }

    /**
     * The field type of the unmapped field {@code name} as a key of the {@code _unmapped} field, the searchable container of the
     * unmapped fields of an index, or {@code null} if the index has none or field-level security hides {@code name}: the container
     * holds every unmapped field, so reading it must not reveal the ones the user cannot see.
     */
    @Nullable
    public MappedFieldType unmappedFieldInContainer(String name) {
        return fieldVisibilityPredicate.test(name) ? UnmappedSinkFieldMapper.unmappedFieldType(getMappingLookup(), name) : null;
    }

    /**
     * This context, resolving each name of {@code unmappedFields} to its field type, so that a query over unmapped fields runs
     * on the container that holds them.
     */
    public EsqlSearchExecutionContext withUnmappedFields(Map<String, MappedFieldType> unmappedFields) {
        return new EsqlSearchExecutionContext(this, queryWarnings, flattenedKeys, Map.copyOf(unmappedFields));
    }

    @Override
    protected MappedFieldType fieldType(String name) {
        MappedFieldType unmapped = unmappedFields.get(name);
        return unmapped != null ? unmapped : super.fieldType(name);
    }

    @Override
    public boolean isMappedField(String name) {
        return super.isMappedField(name) || (flattenedKeys && fieldType(name) instanceof FlattenedFieldMapper.KeyedFlattenedFieldType);
    }

    @Override
    public SourceProvider createSourceProvider(SourceFilter sourceFilter) {
        return new ReinitializingSourceProvider(super::createSourceProvider);
    }

    /**
     * Return the {@link QueryWarnings} bridge for this context.
     */
    public QueryWarnings queryWarnings() {
        return queryWarnings;
    }
}
