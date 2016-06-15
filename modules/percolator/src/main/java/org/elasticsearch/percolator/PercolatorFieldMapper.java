/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.percolator;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.KeywordFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PercolatorFieldMapper extends FieldMapper {

    public final static XContentType QUERY_BUILDER_CONTENT_TYPE = XContentType.SMILE;
    public final static Setting<Boolean> INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING =
            Setting.boolSetting("index.percolator.map_unmapped_fields_as_string", false, Setting.Property.IndexScope);
    public static final String CONTENT_TYPE = "percolator";
    private static final PercolatorFieldType FIELD_TYPE = new PercolatorFieldType();

    public static final String EXTRACTED_TERMS_FIELD_NAME = "extracted_terms";
    public static final String UNKNOWN_QUERY_FIELD_NAME = "unknown_query";
    public static final String QUERY_BUILDER_FIELD_NAME = "query_builder_field";

    public static class Builder extends FieldMapper.Builder<Builder, PercolatorFieldMapper> {

        private final QueryShardContext queryShardContext;

        public Builder(String fieldName, QueryShardContext queryShardContext) {
            super(fieldName, FIELD_TYPE, FIELD_TYPE);
            this.queryShardContext = queryShardContext;
        }

        @Override
        public PercolatorFieldMapper build(BuilderContext context) {
            context.path().add(name());
            KeywordFieldMapper extractedTermsField = createExtractQueryFieldBuilder(EXTRACTED_TERMS_FIELD_NAME, context);
            ((PercolatorFieldType) fieldType).queryTermsField = extractedTermsField.fieldType();
            KeywordFieldMapper unknownQueryField = createExtractQueryFieldBuilder(UNKNOWN_QUERY_FIELD_NAME, context);
            ((PercolatorFieldType) fieldType).unknownQueryField = unknownQueryField.fieldType();
            BinaryFieldMapper queryBuilderField = createQueryBuilderFieldBuilder(context);
            ((PercolatorFieldType) fieldType).queryBuilderField = queryBuilderField.fieldType();
            context.path().remove();
            setupFieldType(context);
            return new PercolatorFieldMapper(name(), fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo, queryShardContext, extractedTermsField,
                    unknownQueryField, queryBuilderField);
        }

        static KeywordFieldMapper createExtractQueryFieldBuilder(String name, BuilderContext context) {
            KeywordFieldMapper.Builder queryMetaDataFieldBuilder = new KeywordFieldMapper.Builder(name);
            queryMetaDataFieldBuilder.docValues(false);
            queryMetaDataFieldBuilder.store(false);
            queryMetaDataFieldBuilder.indexOptions(IndexOptions.DOCS);
            return queryMetaDataFieldBuilder.build(context);
        }

        static BinaryFieldMapper createQueryBuilderFieldBuilder(BuilderContext context) {
            BinaryFieldMapper.Builder builder = new BinaryFieldMapper.Builder(QUERY_BUILDER_FIELD_NAME);
            builder.docValues(true);
            builder.indexOptions(IndexOptions.NONE);
            builder.store(false);
            builder.fieldType().setDocValuesType(DocValuesType.BINARY);
            return builder.build(context);
        }
    }

    public static class TypeParser implements FieldMapper.TypeParser {

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(name, parserContext.queryShardContext());
        }
    }

    public static class PercolatorFieldType extends MappedFieldType {

        private MappedFieldType queryTermsField;
        private MappedFieldType unknownQueryField;
        private MappedFieldType queryBuilderField;

        public PercolatorFieldType() {
            setIndexOptions(IndexOptions.NONE);
            setDocValuesType(DocValuesType.NONE);
            setStored(false);
        }

        public PercolatorFieldType(PercolatorFieldType ref) {
            super(ref);
            queryTermsField = ref.queryTermsField;
            unknownQueryField = ref.unknownQueryField;
            queryBuilderField = ref.queryBuilderField;
        }

        public String getExtractedTermsField() {
            return queryTermsField.name();
        }

        public String getUnknownQueryFieldName() {
            return unknownQueryField.name();
        }

        public String getQueryBuilderFieldName() {
            return queryBuilderField.name();
        }

        @Override
        public MappedFieldType clone() {
            return new PercolatorFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Percolator fields are not searchable directly, use a percolate query instead");
        }
    }

    private final boolean mapUnmappedFieldAsString;
    private final QueryShardContext queryShardContext;
    private KeywordFieldMapper queryTermsField;
    private KeywordFieldMapper unknownQueryField;
    private BinaryFieldMapper queryBuilderField;

    public PercolatorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                 Settings indexSettings, MultiFields multiFields, CopyTo copyTo, QueryShardContext queryShardContext,
                                 KeywordFieldMapper queryTermsField, KeywordFieldMapper unknownQueryField,
                                 BinaryFieldMapper queryBuilderField) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.queryShardContext = queryShardContext;
        this.queryTermsField = queryTermsField;
        this.unknownQueryField = unknownQueryField;
        this.queryBuilderField = queryBuilderField;
        this.mapUnmappedFieldAsString = INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING.get(indexSettings);
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        PercolatorFieldMapper updated = (PercolatorFieldMapper) super.updateFieldType(fullNameToFieldType);
        KeywordFieldMapper queryTermsUpdated = (KeywordFieldMapper) queryTermsField.updateFieldType(fullNameToFieldType);
        KeywordFieldMapper unknownQueryUpdated = (KeywordFieldMapper) unknownQueryField.updateFieldType(fullNameToFieldType);
        BinaryFieldMapper queryBuilderUpdated = (BinaryFieldMapper) queryBuilderField.updateFieldType(fullNameToFieldType);

        if (updated == this || queryTermsUpdated == queryTermsField || unknownQueryUpdated == unknownQueryField
                || queryBuilderUpdated == queryBuilderField) {
            return this;
        }
        if (updated == this) {
            updated = (PercolatorFieldMapper) updated.clone();
        }
        updated.queryTermsField = queryTermsUpdated;
        updated.unknownQueryField = unknownQueryUpdated;
        updated.queryBuilderField = queryBuilderUpdated;
        return updated;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        QueryShardContext queryShardContext = new QueryShardContext(this.queryShardContext);
        if (context.doc().getField(queryBuilderField.name()) != null) {
            // If a percolator query has been defined in an array object then multiple percolator queries
            // could be provided. In order to prevent this we fail if we try to parse more than one query
            // for the current document.
            throw new IllegalArgumentException("a document can only contain one percolator query");
        }

        XContentParser parser = context.parser();
        QueryBuilder queryBuilder = parseQueryBuilder(queryShardContext.newParseContext(parser), parser.getTokenLocation());
        // Fetching of terms, shapes and indexed scripts happen during this rewrite:
        queryBuilder = queryBuilder.rewrite(queryShardContext);

        try (XContentBuilder builder = XContentFactory.contentBuilder(QUERY_BUILDER_CONTENT_TYPE)) {
            queryBuilder.toXContent(builder, new MapParams(Collections.emptyMap()));
            builder.flush();
            byte[] queryBuilderAsBytes = builder.bytes().toBytes();
            context.doc().add(new Field(queryBuilderField.name(), queryBuilderAsBytes, queryBuilderField.fieldType()));
        }

        Query query = toQuery(queryShardContext, mapUnmappedFieldAsString, queryBuilder);
        ExtractQueryTermsService.extractQueryTerms(query, context.doc(), queryTermsField.name(), unknownQueryField.name(),
                queryTermsField.fieldType());
        return null;
    }

    public static Query parseQuery(QueryShardContext context, boolean mapUnmappedFieldsAsString, XContentParser parser) throws IOException {
        return toQuery(context, mapUnmappedFieldsAsString, parseQueryBuilder(context.newParseContext(parser), parser.getTokenLocation()));
    }

    static Query toQuery(QueryShardContext context, boolean mapUnmappedFieldsAsString, QueryBuilder queryBuilder) throws IOException {
        // This means that fields in the query need to exist in the mapping prior to registering this query
        // The reason that this is required, is that if a field doesn't exist then the query assumes defaults, which may be undesired.
        //
        // Even worse when fields mentioned in percolator queries do go added to map after the queries have been registered
        // then the percolator queries don't work as expected any more.
        //
        // Query parsing can't introduce new fields in mappings (which happens when registering a percolator query),
        // because field type can't be inferred from queries (like document do) so the best option here is to disallow
        // the usage of unmapped fields in percolator queries to avoid unexpected behaviour
        //
        // if index.percolator.map_unmapped_fields_as_string is set to true, query can contain unmapped fields which will be mapped
        // as an analyzed string.
        context.setAllowUnmappedFields(false);
        context.setMapUnmappedFieldAsString(mapUnmappedFieldsAsString);
        return queryBuilder.toQuery(context);
    }

    private static QueryBuilder parseQueryBuilder(QueryParseContext context, XContentLocation location) {
        try {
            return context.parseInnerQueryBuilder()
                    .orElseThrow(() -> new ParsingException(location, "Failed to parse inner query, was empty"));
        } catch (IOException e) {
            throw new ParsingException(location, "Failed to parse", e);
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Arrays.<Mapper>asList(queryTermsField, unknownQueryField, queryBuilderField).iterator();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
