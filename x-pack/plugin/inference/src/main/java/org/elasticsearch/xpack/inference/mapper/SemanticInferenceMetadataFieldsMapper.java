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
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * An {@link InferenceMetadataFieldsMapper} that delegates parsing of underlying fields
 * to the corresponding {@link SemanticTextFieldMapper}.
 */
public class SemanticInferenceMetadataFieldsMapper extends InferenceMetadataFieldsMapper {
    private static final SemanticInferenceMetadataFieldsMapper INSTANCE = new SemanticInferenceMetadataFieldsMapper();

    public static final NodeFeature EXPLICIT_NULL_FIXES = new NodeFeature("semantic_text.inference_metadata_fields.explicit_null_fixes");
    public static final NodeFeature INFERENCE_METADATA_FIELDS_ENABLED_BY_DEFAULT = new NodeFeature(
        "semantic_text.inference_metadata_fields.enabled_by_default"
    );

    public static final TypeParser PARSER = new FixedTypeParser(
        c -> InferenceMetadataFieldsMapper.isEnabled(c.getSettings()) ? INSTANCE : null
    );

    static class FieldType extends InferenceMetadataFieldType {
        private static final FieldType INSTANCE = new FieldType();

        FieldType() {
            super();
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return valueFetcher(context.getMappingLookup(), context::bitsetFilter, context.searcher());
        }

        @Override
        public ValueFetcher valueFetcher(MappingLookup mappingLookup, Function<Query, BitSetProducer> bitSetCache, IndexSearcher searcher) {
            Map<String, ValueFetcher> fieldFetchers = new HashMap<>();
            for (var inferenceField : mappingLookup.inferenceFields().keySet()) {
                MappedFieldType ft = mappingLookup.getFieldType(inferenceField);
                if (ft instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
                    fieldFetchers.put(inferenceField, semanticTextFieldType.valueFetcherWithInferenceResults(bitSetCache, searcher));
                } else {
                    throw new IllegalArgumentException(
                        "Invalid inference field [" + ft.name() + "]. Expected field type [semantic_text] but got [" + ft.typeName() + "]"
                    );
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
                        if (values.size() > 0) {
                            assert values.size() == 1;
                            result.put(entry.getKey(), values.get(0));
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

    private SemanticInferenceMetadataFieldsMapper() {
        super(FieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final boolean isWithinLeaf = context.path().isWithinLeafObject();
        try {
            // make sure that we don't expand dots in field names while parsing
            context.path().setWithinLeafObject(true);
            XContentParser parser = context.parser();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                String fieldName = parser.currentName();

                // Set the path to that of semantic text field so the parser acts as if we are parsing the semantic text field value
                // directly. We can safely split on all "." chars because semantic text fields cannot be used when subobjects == false.
                String[] fieldNameParts = fieldName.split("\\.");
                setPath(context.path(), fieldNameParts);
                var mapper = context.mappingLookup().getMapper(fieldName);
                if (mapper instanceof SemanticTextFieldMapper fieldMapper) {
                    XContentLocation xContentLocation = context.parser().getTokenLocation();
                    var input = fieldMapper.parseSemanticTextField(context);
                    if (input != null) {
                        fieldMapper.parseCreateFieldFromContext(context, input, xContentLocation);
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Field [" + fieldName + "] is not a [" + SemanticTextFieldMapper.CONTENT_TYPE + "] field"
                    );
                }
            }
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
            setPath(context.path(), new String[] { InferenceMetadataFieldsMapper.NAME });
        }
    }

    private static void setPath(ContentPath contentPath, String[] newPath) {
        while (contentPath.length() > 0) {
            contentPath.remove();
        }

        for (String pathPart : newPath) {
            contentPath.add(pathPart);
        }
    }
}
