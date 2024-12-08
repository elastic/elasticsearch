/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class InferenceMetadataFieldsMapper extends MetadataFieldMapper {
    public static final FeatureFlag INFERENCE_METADATA_FIELDS_FEATURE_FLAG = new FeatureFlag("inference_metadata_fields");

    public static final String NAME = "_inference_fields";
    public static final String CONTENT_TYPE = "_inference_fields";

    private static final InferenceMetadataFieldsMapper INSTANCE = new InferenceMetadataFieldsMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    public static final class InferenceFieldType extends MappedFieldType {
        private static InferenceFieldType INSTANCE = new InferenceFieldType();

        public InferenceFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Map.of());
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // TODO: return the map from the individual semantic text fields?
            return null;
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

    private InferenceMetadataFieldsMapper() {
        super(InferenceFieldType.INSTANCE);
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
            context.path().setWithinLeafObject(true);
            XContentParser parser = context.parser();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                String fieldName = parser.currentName();
                Mapper mapper = context.mappingLookup().getMapper(fieldName);
                if (mapper instanceof InferenceFieldMapper && mapper instanceof FieldMapper fieldMapper) {
                    fieldMapper.parseCreateField(new InferenceMetadataFieldsParserContext(context.parent(), context, fieldName));
                } else {
                    throw new IllegalArgumentException("Field [" + fieldName + "] is not an inference field");
                }
            }
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }
    }

    private static class InferenceMetadataFieldsParserContext extends DocumentParserContext.Wrapper {
        private final ContentPath path = new ContentPath();

        InferenceMetadataFieldsParserContext(ObjectMapper parent, DocumentParserContext in, String inferenceFieldName) {
            super(parent, in);

            // Set the path as if we are parsing the inference field value directly
            for (String fieldNamePart : inferenceFieldName.split("\\.")) {
                path.add(fieldNamePart);
            }
        }

        @Override
        public boolean isWithinInferenceMetadata() {
            return true;
        }

        @Override
        public ContentPath path() {
            return path;
        }
    }
}
