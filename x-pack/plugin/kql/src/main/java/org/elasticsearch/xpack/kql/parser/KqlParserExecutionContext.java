/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.time.ZoneId;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Tuple.tuple;

class KqlParserExecutionContext extends SearchExecutionContext {

    private static final List<String> IGNORED_METADATA_FIELDS = List.of(
        "_seq_no",
        "_index_mode",
        "_routing",
        "_ignored",
        "_nested_path",
        "_field_names"
    );

    private static Predicate<Tuple<String, MappedFieldType>> searchableFieldFilter = (fieldDef) -> fieldDef.v2().isSearchable();

    private static Predicate<Tuple<String, MappedFieldType>> ignoredFieldFilter = (fieldDef) -> IGNORED_METADATA_FIELDS.contains(
        fieldDef.v1()
    );

    KqlParserExecutionContext(SearchExecutionContext source) {
        super(source);
    }

    public Iterable<Tuple<String, MappedFieldType>> resolveFields(KqlBaseParser.FieldNameContext fieldNameContext) {
        // TODO: use index settings default field.
        String fieldNamePattern = fieldNameContext != null ? ParserUtils.extractText(fieldNameContext) : "*";

        if (fieldNameContext != null && fieldNameContext.value != null && fieldNameContext.value.getType() == KqlBaseParser.QUOTED_STRING) {
            return isFieldMapped(fieldNamePattern) ? List.of(tuple(fieldNamePattern, getFieldType(fieldNamePattern))) : List.of();
        }

        return getMatchingFieldNames(fieldNamePattern).stream()
            .map(fieldName -> tuple(fieldName, getFieldType(fieldName)))
            .filter(searchableFieldFilter.and(Predicate.not(ignoredFieldFilter)))
            .collect(Collectors.toList());
    }

    public boolean isCaseSensitive() {
        // TODO: implementation
        return false;
    }

    public ZoneId timeZone() {
        return null;
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
}
