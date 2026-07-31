/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.index.mapper.InferenceEmbeddingsMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SemanticInferenceEmbeddingsMetadataFieldsMapper extends InferenceEmbeddingsMetadataFieldsMapper {
    private static final SemanticInferenceEmbeddingsMetadataFieldsMapper INSTANCE = new SemanticInferenceEmbeddingsMetadataFieldsMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    static class FieldType extends InferenceEmbeddingsMetadataFieldType {
        private static final FieldType INSTANCE = new FieldType();

        FieldType() {
            super();
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            MappingLookup mappingLookup = context.getMappingLookup();
            Function<Query, BitSetProducer> bitSetCache = context::bitsetFilter;
            IndexSearcher searcher = context.searcher();
            Map<String, ValueFetcher> fieldFetchers = new HashMap<>();
            for (var inferenceField : mappingLookup.inferenceFields().keySet()) {
                MappedFieldType ft = mappingLookup.getFieldType(inferenceField);
                if (ft instanceof SemanticFieldMapper.SemanticFieldType semanticFieldType) {
                    fieldFetchers.put(inferenceField, new EmbeddingsSemanticFieldValueFetcher(semanticFieldType, bitSetCache, searcher));
                } else {
                    throw new IllegalArgumentException("Field [" + ft.name() + "] is not an inference field");
                }
            }
            if (fieldFetchers.isEmpty()) {
                return ValueFetcher.EMPTY;
            }
            return new ValueFetcher() {
                @Override
                public void setNextReader(LeafReaderContext context) {
                    fieldFetchers.values().forEach(f -> f.setNextReader(context));
                }

                @Override
                public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
                    Map<String, Object> result = new HashMap<>();
                    for (var entry : fieldFetchers.entrySet()) {
                        var values = entry.getValue().fetchValues(source, doc, ignoredValues);
                        if (values.isEmpty() == false) {
                            result.put(entry.getKey(), values);
                        }
                    }
                    return result.isEmpty() ? List.of() : List.of(result);
                }

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }
            };
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(
                context,
                "[" + name() + "] field which is of type [" + typeName() + "], does not support term queries"
            );
        }
    }

    private SemanticInferenceEmbeddingsMetadataFieldsMapper() {
        super(FieldType.INSTANCE);
    }
}
