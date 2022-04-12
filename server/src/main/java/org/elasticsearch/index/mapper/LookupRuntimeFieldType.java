/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A runtime field that retrieves fields from related indices.
 * <pre>
 * {
 *     "type": "lookup",
 *     "target_index": "an_external_index",
 *     "input_field": "ip_address",
 *     "target_field": "host_ip",
 *     "fetch_fields": [
 *         "field-1",
 *         "field-2"
 *     ]
 * }
 * </pre>
 */
public final class LookupRuntimeFieldType extends MappedFieldType {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(Builder::new);
    public static final String CONTENT_TYPE = "lookup";

    private static class Builder extends RuntimeField.Builder {
        private final FieldMapper.Parameter<String> targetIndex = FieldMapper.Parameter.stringParam(
            "target_index",
            false,
            RuntimeField.initializerNotSupported(),
            null
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException("[target_index] parameter must be specified");
            }
        });

        private final FieldMapper.Parameter<String> inputField = FieldMapper.Parameter.stringParam(
            "input_field",
            false,
            RuntimeField.initializerNotSupported(),
            null
        ).addValidator(inputField -> {
            if (Strings.isEmpty(inputField)) {
                throw new IllegalArgumentException("[input_field] parameter must be specified");
            }
            if (inputField.equals(name)) {
                throw new IllegalArgumentException("lookup field [" + name + "] can't use input from itself");
            }
        });

        private final FieldMapper.Parameter<String> targetField = FieldMapper.Parameter.stringParam(
            "target_field",
            false,
            RuntimeField.initializerNotSupported(),
            null
        ).addValidator(targetField -> {
            if (Strings.isEmpty(targetField)) {
                throw new IllegalArgumentException("[target_field] parameter must be specified");
            }
        });

        private static FieldMapper.Parameter<List<FieldAndFormat>> newFetchFields() {
            final FieldMapper.Parameter<List<FieldAndFormat>> fetchFields = new FieldMapper.Parameter<>(
                "fetch_fields",
                false,
                List::of,
                (s, ctx, o) -> parseFetchFields(o),
                RuntimeField.initializerNotSupported(),
                XContentBuilder::field,
                Object::toString
            );
            fetchFields.addValidator(fields -> {
                if (fields.isEmpty()) {
                    throw new MapperParsingException("[fetch_fields] parameter must not be empty");
                }
            });
            return fetchFields;
        }

        @SuppressWarnings("rawtypes")
        private static List<FieldAndFormat> parseFetchFields(Object o) {
            final List<?> values = (List<?>) o;
            return values.stream().map(v -> {
                if (v instanceof Map m) {
                    final String field = (String) m.get(FieldAndFormat.FIELD_FIELD.getPreferredName());
                    final String format = (String) m.get(FieldAndFormat.FORMAT_FIELD.getPreferredName());
                    if (field == null) {
                        throw new MapperParsingException("[field] parameter of [fetch_fields] must be provided");
                    }
                    return new FieldAndFormat(field, format);
                } else if (v instanceof String s) {
                    return new FieldAndFormat(s, null);
                } else {
                    throw new MapperParsingException("unexpected value [" + v + "] for [fetch_fields] parameter");
                }
            }).toList();
        }

        private final FieldMapper.Parameter<List<FieldAndFormat>> fetchFields = newFetchFields();

        Builder(String name) {
            super(name);

        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            final List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(targetIndex);
            parameters.add(inputField);
            parameters.add(targetField);
            parameters.add(fetchFields);
            return parameters;
        }

        @Override
        protected RuntimeField createRuntimeField(MappingParserContext parserContext) {
            final LookupRuntimeFieldType ft = new LookupRuntimeFieldType(
                name,
                meta(),
                targetIndex.get(),
                inputField.get(),
                targetField.get(),
                fetchFields.get()
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

    private final String lookupIndex;
    private final String inputField;
    private final String targetField;
    private final List<FieldAndFormat> fetchFields;

    private LookupRuntimeFieldType(
        String name,
        Map<String, String> meta,
        String lookupIndex,
        String inputField,
        String targetField,
        List<FieldAndFormat> fetchFields
    ) {
        super(name, false, false, false, TextSearchInfo.NONE, meta);
        this.lookupIndex = lookupIndex;
        this.inputField = inputField;
        this.targetField = targetField;
        this.fetchFields = fetchFields;
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "cannot be executed against lookup field ["
                    + name()
                    + "] while ["
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "] is set to [false]."
            );
        }
        return new LookupFieldValueFetcher(context);
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is a lookup field.");
    }

    private class LookupFieldValueFetcher implements ValueFetcher {
        private final ValueFetcher inputFieldValueFetcher;

        LookupFieldValueFetcher(SearchExecutionContext context) {
            final MappedFieldType inputFieldType = context.getFieldType(inputField);
            // do not allow unmapped field
            if (inputFieldType == null) {
                throw new QueryShardException(context, "No field mapping can be found for the field with name [{}]", inputField);
            }
            this.inputFieldValueFetcher = inputFieldType.valueFetcher(context, null);
        }

        @Override
        public List<Object> fetchValues(SourceLookup lookup, List<Object> ignoredValues) throws IOException {
            assert false : "call #fetchDocumentField() instead";
            throw new UnsupportedOperationException("call #fetchDocumentField() instead");
        }

        @Override
        public DocumentField fetchDocumentField(String docName, SourceLookup lookup) throws IOException {
            final DocumentField inputDoc = inputFieldValueFetcher.fetchDocumentField(inputField, lookup);
            if (inputDoc == null || inputDoc.getValues().isEmpty()) {
                return null;
            }
            final List<LookupField> lookupFields = inputDoc.getValues().stream().map(input -> {
                final TermQueryBuilder query = new TermQueryBuilder(targetField, input.toString());
                return new LookupField(lookupIndex, query, fetchFields, 1);
            }).toList();
            return new DocumentField(docName, List.of(), List.of(), lookupFields);
        }

        @Override
        public void setNextReader(LeafReaderContext context) {
            inputFieldValueFetcher.setNextReader(context);
        }
    }
}
