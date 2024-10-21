/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Tuple.tuple;

class KqlParserExecutionContext extends SearchExecutionContext {
    KqlParserExecutionContext(SearchExecutionContext source) {
        super(source);
    }

    public Iterable<Tuple<String, MappedFieldType>> resolveFields(KqlBaseParser.FieldNameContext fieldNameContext) {
        // TODO: handle the case where fieldName context is null;
        Iterable<Tuple<String, MappedFieldType>> fields = List.of();
        String fieldNamePattern = ParserUtils.extractText(fieldNameContext);

        if (fieldNameContext.value != null && fieldNameContext.value.getType() == KqlBaseParser.QUOTED_STRING) {
            if (isFieldMapped(fieldNamePattern)) {
                fields = List.of(tuple(fieldNamePattern, getFieldType(fieldNamePattern)));
            }
        } else {
            fields = getMatchingFieldNames(fieldNamePattern).stream()
                .map(fieldName -> tuple(fieldName, getFieldType(fieldName)))
                .collect(Collectors.toList());
        }

        return fields;
    }

    public boolean isRuntimeField(MappedFieldType fieldType) {
        return fieldType instanceof AbstractScriptFieldType<?>;
    }
}
