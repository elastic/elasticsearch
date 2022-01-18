/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A runtime field that retrieves fields from related indices.
 * {
 *    "type": "lookup",
 *    "lookup_index": "an_external_index",
 *    "id_field": "field_whose_values_are_ids_of_lookup_index",
 *    "lookup_fields: ["field-1", "field-2"]
 *  }
 */
public final class LookupRuntimeFieldType extends MappedFieldType {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);
    public static final String CONTENT_TYPE = "lookup";

    private static class Builder extends RuntimeField.Builder {
        private final FieldMapper.Parameter<String> lookupIndex = FieldMapper.Parameter.stringParam(
            "lookup_index",
            false,
            RuntimeField.initializerNotSupported(),
            null
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException("[lookup_index] parameter must be specified");
            }
        });

        private final FieldMapper.Parameter<String> idField = FieldMapper.Parameter.stringParam(
            "id_field",
            false,
            RuntimeField.initializerNotSupported(),
            null
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException("[id_field] parameter must be specified");
            }
        });

        private final FieldMapper.Parameter<List<String>> lookupFields = FieldMapper.Parameter.stringArrayParam(
            "lookup_fields",
            false,
            RuntimeField.initializerNotSupported(),
            List.of()
        );

        Builder(String name) {
            super(name);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            final List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(lookupIndex);
            parameters.add(idField);
            parameters.add(lookupFields);
            return parameters;
        }

        @Override
        protected RuntimeField createRuntimeField(MappingParserContext parserContext) {
            final LookupRuntimeFieldType ft = new LookupRuntimeFieldType(
                name,
                meta(),
                lookupIndex.get(),
                idField.get(),
                lookupFields.get()
            );
            return new LeafRuntimeField(name, ft, getParameters());
        }

        @Override
        protected RuntimeField createChildRuntimeField(
            MappingParserContext parserContext,
            String parentName,
            Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory
        ) {
            return createRuntimeField(parserContext);
        }
    }

    private static final ValueFetcher EMPTY_VALUE_FETCHER = (lookup, ignoredValues) -> List.of();
    private final String lookupIndex;
    private final String idField;
    private final List<String> lookupFields;

    LookupRuntimeFieldType(String name, Map<String, String> meta, String lookupIndex, String idField, List<String> lookupFields) {
        super(name, false, false, false, TextSearchInfo.NONE, meta);
        this.lookupIndex = lookupIndex;
        this.idField = idField;
        this.lookupFields = lookupFields;
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        return EMPTY_VALUE_FETCHER;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is a lookup field.");
    }

    public String getLookupIndex() {
        return lookupIndex;
    }

    public String getIdField() {
        return idField;
    }

    public List<String> getLookupFields() {
        return lookupFields;
    }
}
