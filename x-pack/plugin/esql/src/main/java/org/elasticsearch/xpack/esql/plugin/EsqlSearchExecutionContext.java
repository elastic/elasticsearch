/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.KeywordScriptFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.UnmappedSinkFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An ESQL-specific subclass of {@link SearchExecutionContext} that carries the {@link QueryWarnings} bridge and resolves the fields
 * the plan reads that some shards may not map.
 * <p>
 * The plan only names such a field. This shard resolves the name to what it holds: the field it maps, the key of a flattened
 * field, the key of its {@code _unmapped} field, or the value in {@code _source}. Loading, pushed queries and the statistics of the
 * planner all see that resolution.
 */
public class EsqlSearchExecutionContext extends SearchExecutionContext {
    private final QueryWarnings queryWarnings;
    private final Set<String> unmappedFieldNames;
    private final Map<String, Optional<MappedFieldType>> unmappedFieldTypes = new ConcurrentHashMap<>();

    public EsqlSearchExecutionContext(SearchExecutionContext base, QueryWarnings queryWarnings) {
        this(base, queryWarnings, Set.of());
    }

    /**
     * @param unmappedFieldNames the fields the plan reads that some shards may not map
     */
    public EsqlSearchExecutionContext(SearchExecutionContext base, QueryWarnings queryWarnings, Set<String> unmappedFieldNames) {
        super(base);
        this.queryWarnings = queryWarnings;
        this.unmappedFieldNames = Set.copyOf(unmappedFieldNames);
    }

    @Override
    protected MappedFieldType fieldType(String name) {
        MappedFieldType mapped = super.fieldType(name);
        if (mapped != null || unmappedFieldNames.contains(name) == false) {
            return mapped;
        }
        return unmappedFieldTypes.computeIfAbsent(name, n -> Optional.ofNullable(unmappedFieldType(n))).orElse(null);
    }

    @Override
    public boolean isMappedField(String name) {
        return super.isMappedField(name) || (unmappedFieldNames.contains(name) && fieldType(name) != null);
    }

    @Override
    public Set<String> getMatchingFieldNames(String pattern) {
        Set<String> matches = super.getMatchingFieldNames(pattern);
        Set<String> resolved = null;
        for (String name : unmappedFieldNames) {
            if (matches.contains(name) == false && Regex.simpleMatch(pattern, name) && fieldType(name) != null) {
                if (resolved == null) {
                    resolved = new HashSet<>(matches);
                }
                resolved.add(name);
            }
        }
        return resolved == null ? matches : resolved;
    }

    /**
     * Whether this shard reads the field {@code name} the plan reads from {@code _source}, as it maps no field and keeps no key of
     * that name.
     */
    public boolean readsFromSource(String name) {
        return unmappedFieldNames.contains(name)
            && super.fieldType(name) == null
            && fieldType(name) != null
            && UnmappedSinkFieldMapper.unmappedFieldType(getMappingLookup(), name) == null;
    }

    /**
     * The field type of an unmapped field: the key of its name in the {@code _unmapped} field, a {@code keyword} read from
     * {@code _source}, on which the shard runs no query, or {@code null} if no document of this shard may hold it or field-level
     * security hides it.
     */
    @Nullable
    private MappedFieldType unmappedFieldType(String name) {
        if (isFieldVisible(name) == false) {
            return null;
        }
        MappedFieldType key = UnmappedSinkFieldMapper.unmappedFieldType(getMappingLookup(), name);
        if (key != null) {
            return key;
        }
        if (unmappedFields().mayExist(name) == false) {
            return null;
        }
        return KeywordScriptFieldType.sourceOnly(name).asMappedFieldTypes().findFirst().orElseThrow();
    }

    /**
     * Whether field-level security lets the query read {@code name}, as it does for a mapped field.
     */
    private boolean isFieldVisible(String name) {
        return (allowedFields == null || allowedFields.test(name)) && fieldVisibilityPredicate.test(name);
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
