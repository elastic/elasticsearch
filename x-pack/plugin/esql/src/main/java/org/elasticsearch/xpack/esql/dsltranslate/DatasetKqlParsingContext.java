/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.kql.parser.KqlParsingContext;

import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link KqlParsingContext} answered from a dataset schema (name &rarr; {@link DataType}) instead of an index
 * mapping. Every {@code QueryRewriteContext}-backed method of the base is overridden; {@link #fieldType} throws
 * to make any future base-class regression loud rather than a {@link NullPointerException} on the null rewrite context.
 *
 * <p>{@code isSearchableField} deliberately returns true for every column the schema has, including {@code text}:
 * excluding it would make the parser silently drop the field from a disjunction (an under-match), whereas including it
 * routes the leaf to {@code match}, which {@link QueryDslTranslator} degrades loudly rather than mis-matching.
 */
final class DatasetKqlParsingContext extends KqlParsingContext {

    private final Map<String, DataType> schema;

    DatasetKqlParsingContext(Map<String, DataType> schema, boolean caseInsensitive, ZoneId timeZone, String defaultField) {
        super(null, caseInsensitive, timeZone, defaultField);
        this.schema = schema;
    }

    @Override
    public Set<String> resolveFieldNames(String fieldNamePattern) {
        // The index impl delegates to QueryRewriteContext.getMatchingFieldNames — same simpleMatch semantics.
        return schema.keySet().stream().filter(name -> Regex.simpleMatch(fieldNamePattern, name)).collect(Collectors.toSet());
    }

    @Override
    public Set<String> resolveDefaultFieldNames() {
        // Honest analog of the index impl: an explicit default_field wins; datasets have no index.query.default_field
        // setting, so the fallback is the index's own fallback — everything.
        return defaultField() != null ? resolveFieldNames(defaultField()) : Set.copyOf(schema.keySet());
    }

    @Override
    public MappedFieldType fieldType(String fieldName) {
        throw new UnsupportedOperationException("dataset schemas have no MappedFieldType");
    }

    @Override
    public boolean isNestedField(String fieldName) {
        return false;
    }

    @Override
    public String nestedPath(String fieldName) {
        return null;
    }

    @Override
    public boolean isKeywordField(String fieldName) {
        return schema.get(fieldName) == DataType.KEYWORD;
    }

    @Override
    public boolean isDateField(String fieldName) {
        DataType type = schema.get(fieldName);
        return type == DataType.DATETIME || type == DataType.DATE_NANOS;
    }

    @Override
    public boolean isRangeField(String fieldName) {
        return false; // no range types in dataset schemas
    }

    @Override
    public boolean isRuntimeField(String fieldName) {
        return false; // no runtime fields either
    }

    @Override
    public boolean isSearchableField(String fieldName) {
        return schema.containsKey(fieldName);
    }
}
