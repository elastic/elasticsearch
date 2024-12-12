/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.support.NestedScope;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;

public class KqlParsingContext {

    private static final List<String> IGNORED_METADATA_FIELDS = List.of(
        "_seq_no",
        "_index_mode",
        "_routing",
        "_ignored",
        "_nested_path",
        "_field_names"
    );

    public static Builder builder(QueryRewriteContext queryRewriteContext) {
        return new Builder(queryRewriteContext);
    }

    private final QueryRewriteContext queryRewriteContext;
    private final boolean caseInsensitive;
    private final ZoneId timeZone;
    private final String defaultField;
    private final NestedScope nestedScope = new NestedScope();

    public KqlParsingContext(QueryRewriteContext queryRewriteContext, boolean caseInsensitive, ZoneId timeZone, String defaultField) {
        this.queryRewriteContext = queryRewriteContext;
        this.caseInsensitive = caseInsensitive;
        this.timeZone = timeZone;
        this.defaultField = defaultField;
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public ZoneId timeZone() {
        return timeZone;
    }

    public String defaultField() {
        return defaultField;
    }

    public String nestedPath(String fieldName) {
        return nestedLookup().getNestedParent(fieldName);
    }

    public boolean isNestedField(String fieldName) {
        return nestedMappers().containsKey(fullFieldName(fieldName));
    }

    public Set<String> resolveFieldNames(String fieldNamePattern) {
        assert fieldNamePattern != null && fieldNamePattern.isEmpty() == false : "fieldNamePattern cannot be null or empty";
        return queryRewriteContext.getMatchingFieldNames(fullFieldName(fieldNamePattern));
    }

    public Set<String> resolveDefaultFieldNames() {
        return resolveFieldNames(defaultField);
    }

    public MappedFieldType fieldType(String fieldName) {
        return queryRewriteContext.getFieldType(fieldName);
    }

    public static boolean isRuntimeField(MappedFieldType fieldType) {
        return fieldType instanceof AbstractScriptFieldType<?>;
    }

    public static boolean isDateField(MappedFieldType fieldType) {
        return fieldType.typeName().equals(DateFieldMapper.CONTENT_TYPE);
    }

    public static boolean isKeywordField(MappedFieldType fieldType) {
        return fieldType.typeName().equals(KeywordFieldMapper.CONTENT_TYPE);
    }

    public static boolean isSearchableField(String fieldName, MappedFieldType fieldType) {
        return IGNORED_METADATA_FIELDS.contains(fieldName) == false && fieldType.isSearchable();
    }

    public boolean isSearchableField(String fieldName) {
        return isSearchableField(fieldName, fieldType(fieldName));
    }

    public NestedScope nestedScope() {
        return nestedScope;
    }

    public <T> T withNestedPath(String nestedFieldName, Supplier<T> supplier) {
        assert isNestedField(nestedFieldName);
        nestedScope.nextLevel(nestedMappers().get(fullFieldName(nestedFieldName)));
        T result = supplier.get();
        nestedScope.previousLevel();
        return result;
    }

    public String currentNestedPath() {
        return nestedScope().getObjectMapper() != null ? nestedScope().getObjectMapper().fullPath() : null;
    }

    public String fullFieldName(String fieldName) {
        if (nestedScope.getObjectMapper() == null) {
            return fieldName;
        }

        return format("%s.%s", nestedScope.getObjectMapper().fullPath(), fieldName);
    }

    private NestedLookup nestedLookup() {
        return queryRewriteContext.getMappingLookup().nestedLookup();
    }

    private Map<String, NestedObjectMapper> nestedMappers() {
        return nestedLookup().getNestedMappers();
    }

    public static class Builder {
        private final QueryRewriteContext queryRewriteContext;
        private boolean caseInsensitive = true;
        private ZoneId timeZone = null;
        private String defaultField = null;

        private Builder(QueryRewriteContext queryRewriteContext) {
            this.queryRewriteContext = queryRewriteContext;
        }

        public KqlParsingContext build() {
            return new KqlParsingContext(queryRewriteContext, caseInsensitive, timeZone, defaultField);
        }

        public Builder caseInsensitive(boolean caseInsensitive) {
            this.caseInsensitive = caseInsensitive;
            return this;
        }

        public Builder timeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder defaultField(String defaultField) {
            this.defaultField = defaultField;
            return this;
        }
    }
}
