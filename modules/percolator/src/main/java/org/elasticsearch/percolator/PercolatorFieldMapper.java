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
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class PercolatorFieldMapper extends FieldMapper {

    public static final XContentType QUERY_BUILDER_CONTENT_TYPE = XContentType.SMILE;
    public static final Setting<Boolean> INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING =
            Setting.boolSetting("index.percolator.map_unmapped_fields_as_string", false, Setting.Property.IndexScope);
    public static final String CONTENT_TYPE = "percolator";
    private static final FieldType FIELD_TYPE = new FieldType();

    static final byte FIELD_VALUE_SEPARATOR = 0;  // nul code point
    static final String EXTRACTION_COMPLETE = "complete";
    static final String EXTRACTION_PARTIAL = "partial";
    static final String EXTRACTION_FAILED = "failed";

    public static final String EXTRACTED_TERMS_FIELD_NAME = "extracted_terms";
    public static final String EXTRACTION_RESULT_FIELD_NAME = "extraction_result";
    public static final String QUERY_BUILDER_FIELD_NAME = "query_builder_field";

    public static class Builder extends FieldMapper.Builder<Builder, PercolatorFieldMapper> {

        private final Supplier<QueryShardContext> queryShardContext;

        public Builder(String fieldName, Supplier<QueryShardContext> queryShardContext) {
            super(fieldName, FIELD_TYPE, FIELD_TYPE);
            this.queryShardContext = queryShardContext;
        }

        @Override
        public PercolatorFieldMapper build(BuilderContext context) {
            context.path().add(name());
            FieldType fieldType = (FieldType) this.fieldType;
            KeywordFieldMapper extractedTermsField = createExtractQueryFieldBuilder(EXTRACTED_TERMS_FIELD_NAME, context);
            fieldType.queryTermsField = extractedTermsField.fieldType();
            KeywordFieldMapper extractionResultField = createExtractQueryFieldBuilder(EXTRACTION_RESULT_FIELD_NAME, context);
            fieldType.extractionResultField = extractionResultField.fieldType();
            BinaryFieldMapper queryBuilderField = createQueryBuilderFieldBuilder(context);
            fieldType.queryBuilderField = queryBuilderField.fieldType();
            context.path().remove();
            setupFieldType(context);
            return new PercolatorFieldMapper(name(), fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo, queryShardContext, extractedTermsField,
                    extractionResultField, queryBuilderField);
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
            return new Builder(name, parserContext.queryShardContextSupplier());
        }
    }

    public static class FieldType extends MappedFieldType {

        MappedFieldType queryTermsField;
        MappedFieldType extractionResultField;
        MappedFieldType queryBuilderField;

        public FieldType() {
            setIndexOptions(IndexOptions.NONE);
            setDocValuesType(DocValuesType.NONE);
            setStored(false);
        }

        public FieldType(FieldType ref) {
            super(ref);
            queryTermsField = ref.queryTermsField;
            extractionResultField = ref.extractionResultField;
            queryBuilderField = ref.queryBuilderField;
        }

        @Override
        public MappedFieldType clone() {
            return new FieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Percolator fields are not searchable directly, use a percolate query instead");
        }

        public Query percolateQuery(String documentType, PercolateQuery.QueryStore queryStore, BytesReference documentSource,
                                    IndexSearcher searcher) throws IOException {
            IndexReader indexReader = searcher.getIndexReader();
            Query candidateMatchesQuery = createCandidateQuery(indexReader);
            Query verifiedMatchesQuery;
            // We can only skip the MemoryIndex verification when percolating a single document.
            // When the document being percolated contains a nested object field then the MemoryIndex contains multiple
            // documents. In this case the term query that indicates whether memory index verification can be skipped
            // can incorrectly indicate that non nested queries would match, while their nested variants would not.
            if (indexReader.maxDoc() == 1) {
                verifiedMatchesQuery = new TermQuery(new Term(extractionResultField.name(), EXTRACTION_COMPLETE));
            } else {
                verifiedMatchesQuery = new MatchNoDocsQuery("nested docs, so no verified matches");
            }
            return new PercolateQuery(documentType, queryStore, documentSource, candidateMatchesQuery, searcher, verifiedMatchesQuery);
        }

        Query createCandidateQuery(IndexReader indexReader) throws IOException {
            List<BytesRef> extractedTerms = new ArrayList<>();
            LeafReader reader = indexReader.leaves().get(0).reader();
            Fields fields = reader.fields();
            for (String field : fields) {
                Terms terms = fields.terms(field);
                if (terms == null) {
                    continue;
                }

                BytesRef fieldBr = new BytesRef(field);
                TermsEnum tenum = terms.iterator();
                for (BytesRef term = tenum.next(); term != null; term = tenum.next()) {
                    BytesRefBuilder builder = new BytesRefBuilder();
                    builder.append(fieldBr);
                    builder.append(FIELD_VALUE_SEPARATOR);
                    builder.append(term);
                    extractedTerms.add(builder.toBytesRef());
                }
            }
            Query extractionSuccess = new TermInSetQuery(queryTermsField.name(), extractedTerms);
            // include extractionResultField:failed, because docs with this term have no extractedTermsField
            // and otherwise we would fail to return these docs. Docs that failed query term extraction
            // always need to be verified by MemoryIndex:
            Query extractionFailure = new TermQuery(new Term(extractionResultField.name(), EXTRACTION_FAILED));

            return new BooleanQuery.Builder()
                    .add(extractionSuccess, Occur.SHOULD)
                    .add(extractionFailure, Occur.SHOULD)
                    .build();
        }

    }

    private final boolean mapUnmappedFieldAsString;
    private final Supplier<QueryShardContext> queryShardContext;
    private KeywordFieldMapper queryTermsField;
    private KeywordFieldMapper extractionResultField;
    private BinaryFieldMapper queryBuilderField;

    public PercolatorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                 Settings indexSettings, MultiFields multiFields, CopyTo copyTo,
                                 Supplier<QueryShardContext> queryShardContext,
                                 KeywordFieldMapper queryTermsField, KeywordFieldMapper extractionResultField,
                                 BinaryFieldMapper queryBuilderField) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.queryShardContext = queryShardContext;
        this.queryTermsField = queryTermsField;
        this.extractionResultField = extractionResultField;
        this.queryBuilderField = queryBuilderField;
        this.mapUnmappedFieldAsString = INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING.get(indexSettings);
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        PercolatorFieldMapper updated = (PercolatorFieldMapper) super.updateFieldType(fullNameToFieldType);
        KeywordFieldMapper queryTermsUpdated = (KeywordFieldMapper) queryTermsField.updateFieldType(fullNameToFieldType);
        KeywordFieldMapper extractionResultUpdated = (KeywordFieldMapper) extractionResultField.updateFieldType(fullNameToFieldType);
        BinaryFieldMapper queryBuilderUpdated = (BinaryFieldMapper) queryBuilderField.updateFieldType(fullNameToFieldType);

        if (updated == this && queryTermsUpdated == queryTermsField && extractionResultUpdated == extractionResultField
                && queryBuilderUpdated == queryBuilderField) {
            return this;
        }
        if (updated == this) {
            updated = (PercolatorFieldMapper) updated.clone();
        }
        updated.queryTermsField = queryTermsUpdated;
        updated.extractionResultField = extractionResultUpdated;
        updated.queryBuilderField = queryBuilderUpdated;
        return updated;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        QueryShardContext queryShardContext = this.queryShardContext.get();
        if (context.doc().getField(queryBuilderField.name()) != null) {
            // If a percolator query has been defined in an array object then multiple percolator queries
            // could be provided. In order to prevent this we fail if we try to parse more than one query
            // for the current document.
            throw new IllegalArgumentException("a document can only contain one percolator query");
        }

        XContentParser parser = context.parser();
        QueryBuilder queryBuilder = parseQueryBuilder(
                queryShardContext.newParseContext(parser), parser.getTokenLocation()
        );
        verifyQuery(queryBuilder);
        // Fetching of terms, shapes and indexed scripts happen during this rewrite:
        queryBuilder = queryBuilder.rewrite(queryShardContext);

        try (XContentBuilder builder = XContentFactory.contentBuilder(QUERY_BUILDER_CONTENT_TYPE)) {
            queryBuilder.toXContent(builder, new MapParams(Collections.emptyMap()));
            builder.flush();
            byte[] queryBuilderAsBytes = BytesReference.toBytes(builder.bytes());
            context.doc().add(new Field(queryBuilderField.name(), queryBuilderAsBytes, queryBuilderField.fieldType()));
        }

        Query query = toQuery(queryShardContext, mapUnmappedFieldAsString, queryBuilder);
        processQuery(query, context);
        return null;
    }

    void processQuery(Query query, ParseContext context) {
        ParseContext.Document doc = context.doc();
        FieldType pft = (FieldType) this.fieldType();
        QueryAnalyzer.Result result;
        try {
            result = QueryAnalyzer.analyze(query);
        } catch (QueryAnalyzer.UnsupportedQueryException e) {
            doc.add(new Field(pft.extractionResultField.name(), EXTRACTION_FAILED, extractionResultField.fieldType()));
            return;
        }
        for (Term term : result.terms) {
            BytesRefBuilder builder = new BytesRefBuilder();
            builder.append(new BytesRef(term.field()));
            builder.append(FIELD_VALUE_SEPARATOR);
            builder.append(term.bytes());
            doc.add(new Field(queryTermsField.name(), builder.toBytesRef(), queryTermsField.fieldType()));
        }
        if (result.verified) {
            doc.add(new Field(extractionResultField.name(), EXTRACTION_COMPLETE, extractionResultField.fieldType()));
        } else {
            doc.add(new Field(extractionResultField.name(), EXTRACTION_PARTIAL, extractionResultField.fieldType()));
        }
    }

    public static Query parseQuery(QueryShardContext context, boolean mapUnmappedFieldsAsString, XContentParser parser) throws IOException {
        return parseQuery(context, mapUnmappedFieldsAsString, context.newParseContext(parser), parser);
    }

    public static Query parseQuery(QueryShardContext context, boolean mapUnmappedFieldsAsString, QueryParseContext queryParseContext,
                                   XContentParser parser) throws IOException {
        return toQuery(context, mapUnmappedFieldsAsString, parseQueryBuilder(queryParseContext, parser.getTokenLocation()));
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
            return context.parseInnerQueryBuilder();
        } catch (IOException e) {
            throw new ParsingException(location, "Failed to parse", e);
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Arrays.<Mapper>asList(queryTermsField, extractionResultField, queryBuilderField).iterator();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Fails if a percolator contains an unsupported query. The following queries are not supported:
     * 1) a range query with a date range based on current time
     * 2) a has_child query
     * 3) a has_parent query
     */
    static void verifyQuery(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof RangeQueryBuilder) {
            RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) queryBuilder;
            if (rangeQueryBuilder.from() instanceof String) {
                String from = (String) rangeQueryBuilder.from();
                if (from.contains("now")) {
                    throw new IllegalArgumentException("percolator queries containing time range queries based on the " +
                            "current time is unsupported");
                }
            }
            if (rangeQueryBuilder.to() instanceof String) {
                String to = (String) rangeQueryBuilder.to();
                if (to.contains("now")) {
                    throw new IllegalArgumentException("percolator queries containing time range queries based on the " +
                        "current time is unsupported");
                }
            }
        } else if (queryBuilder instanceof HasChildQueryBuilder) {
            throw new IllegalArgumentException("the [has_child] query is unsupported inside a percolator query");
        } else if (queryBuilder instanceof HasParentQueryBuilder) {
            throw new IllegalArgumentException("the [has_parent] query is unsupported inside a percolator query");
        } else if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            List<QueryBuilder> clauses = new ArrayList<>();
            clauses.addAll(boolQueryBuilder.filter());
            clauses.addAll(boolQueryBuilder.must());
            clauses.addAll(boolQueryBuilder.mustNot());
            clauses.addAll(boolQueryBuilder.should());
            for (QueryBuilder clause : clauses) {
                verifyQuery(clause);
            }
        } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
            verifyQuery(((ConstantScoreQueryBuilder) queryBuilder).innerQuery());
        } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
            verifyQuery(((FunctionScoreQueryBuilder) queryBuilder).query());
        } else if (queryBuilder instanceof BoostingQueryBuilder) {
            verifyQuery(((BoostingQueryBuilder) queryBuilder).negativeQuery());
            verifyQuery(((BoostingQueryBuilder) queryBuilder).positiveQuery());
        }
    }

}
