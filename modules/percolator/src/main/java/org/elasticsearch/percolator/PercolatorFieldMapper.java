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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
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
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.percolator.QueryExtractService.RangeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.search.BooleanClause.Occur.MUST;

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

    public static final String INT_FROM_FIELD = "extracted_int_from";
    public static final String INT_TO_FIELD = "extracted_int_to";
    public static final String LONG_FROM_FIELD = "extracted_long_from";
    public static final String LONG_TO_FIELD = "extracted_to_from";
    public static final String HALF_FLOAT_FROM_FIELD = "extracted_half_float_from";
    public static final String HALF_FLOAT_TO_FIELD = "extracted_half_float_to";
    public static final String FLOAT_FROM_FIELD = "extracted_float_from";
    public static final String FLOAT_TO_FIELD = "extracted_float_to";
    public static final String DOUBLE_FROM_FIELD = "extracted_double_from";
    public static final String DOUBLE_TO_FIELD = "extracted_double_to";
    public static final String IP_FROM_FIELD = "extracted_ip_from";
    public static final String IP_TO_FIELD = "extracted_ip_to";

    public static class Builder extends FieldMapper.Builder<Builder, PercolatorFieldMapper> {

        private final QueryShardContext queryShardContext;

        public Builder(String fieldName, QueryShardContext queryShardContext) {
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

            NumberFieldMapper intFromField = createExtractedRangeFieldBuilder(INT_FROM_FIELD, NumberType.INTEGER, context);
            NumberFieldMapper intToField = createExtractedRangeFieldBuilder(INT_TO_FIELD, NumberType.INTEGER, context);
            MappedRange intRange  = new MappedRange(RangeType.INT, intFromField.fieldType(), intToField.fieldType());
            fieldType.ranges.put("byte", intRange);
            fieldType.ranges.put("short", intRange);
            fieldType.ranges.put("integer", intRange);

            NumberFieldMapper longFromField = createExtractedRangeFieldBuilder(LONG_FROM_FIELD, NumberType.LONG, context);
            NumberFieldMapper longToField = createExtractedRangeFieldBuilder(LONG_TO_FIELD, NumberType.LONG, context);
            fieldType.ranges.put("long", new MappedRange(RangeType.LONG, longFromField.fieldType(), longToField.fieldType()));

            NumberFieldMapper halfFloatFromField = createExtractedRangeFieldBuilder(HALF_FLOAT_FROM_FIELD, NumberType.HALF_FLOAT, context);
            NumberFieldMapper halfFloatToField = createExtractedRangeFieldBuilder(HALF_FLOAT_TO_FIELD, NumberType.HALF_FLOAT, context);
            fieldType.ranges.put("half_float", new MappedRange(RangeType.HALF_FLOAT, halfFloatFromField.fieldType(),
                    halfFloatToField.fieldType()));

            NumberFieldMapper floatFromField = createExtractedRangeFieldBuilder(FLOAT_FROM_FIELD, NumberType.FLOAT, context);
            NumberFieldMapper floatToField = createExtractedRangeFieldBuilder(FLOAT_TO_FIELD, NumberType.FLOAT, context);
            fieldType.ranges.put("float", new MappedRange(RangeType.FLOAT, floatFromField.fieldType(), floatToField.fieldType()));

            NumberFieldMapper doubleFromField = createExtractedRangeFieldBuilder(DOUBLE_FROM_FIELD, NumberType.DOUBLE, context);
            NumberFieldMapper doubleToField = createExtractedRangeFieldBuilder(DOUBLE_TO_FIELD, NumberType.DOUBLE, context);
            fieldType.ranges.put("double", new MappedRange(RangeType.DOUBLE, doubleFromField.fieldType(), doubleToField.fieldType()));

            IpFieldMapper ipFromField = createExtractedIpFieldBuilder(IP_FROM_FIELD, context);
            IpFieldMapper ipToField = createExtractedIpFieldBuilder(IP_TO_FIELD, context);
            fieldType.ranges.put(IpFieldMapper.CONTENT_TYPE, new MappedRange(RangeType.IP, ipFromField.fieldType(), ipToField.fieldType()));

            context.path().remove();
            setupFieldType(context);
            return new PercolatorFieldMapper(name(), fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo, queryShardContext, extractedTermsField,
                    extractionResultField, queryBuilderField, intFromField, intToField, longFromField, longToField,
                    halfFloatFromField, halfFloatToField, floatFromField, floatToField, doubleFromField, doubleToField,
                    ipFromField, ipToField);
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

        static NumberFieldMapper createExtractedRangeFieldBuilder(String name, NumberType numberType, BuilderContext context) {
            NumberFieldMapper.Builder builder = new NumberFieldMapper.Builder(name, numberType);
            builder.docValues(false);
            builder.store(false);
            return builder.build(context);
        }

        static IpFieldMapper createExtractedIpFieldBuilder(String name, BuilderContext context) {
            IpFieldMapper.Builder builder = new IpFieldMapper.Builder(name);
            builder.docValues(false);
            builder.store(false);
            return builder.build(context);
        }

    }

    public static class TypeParser implements FieldMapper.TypeParser {

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(name, parserContext.queryShardContext());
        }
    }

    public static class FieldType extends MappedFieldType {

        MappedFieldType queryTermsField;
        MappedFieldType extractionResultField;
        MappedFieldType queryBuilderField;
        Map<String, MappedRange> ranges;

        public FieldType() {
            setIndexOptions(IndexOptions.NONE);
            setDocValuesType(DocValuesType.NONE);
            setStored(false);
            this.ranges = new HashMap<>();
        }

        public FieldType(FieldType ref) {
            super(ref);
            queryTermsField = ref.queryTermsField;
            extractionResultField = ref.extractionResultField;
            queryBuilderField = ref.queryBuilderField;
            ranges = ref.ranges;
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
                                    IndexSearcher searcher, DocumentMapper documentMapper) throws IOException {
            IndexReader indexReader = searcher.getIndexReader();
            Query candidateMatchesQuery = createCandidateQuery(indexReader, documentMapper);
            Query verifiedMatchesQuery;
            // We can only skip the MemoryIndex verification when percolating a single document.
            // When the document being percolated contains a nested object field then the MemoryIndex contains multiple
            // documents. In this case the term query that indicates whether memory index verification can be skipped
            // can incorrectly indicate that non nested queries would match, while their nested variants would not.
            if (indexReader.maxDoc() == 1) {
                verifiedMatchesQuery = new TermQuery(new Term(extractionResultField.name(), EXTRACTION_COMPLETE));
            } else {
                verifiedMatchesQuery = new MatchNoDocsQuery("nested docs, no verified matches");
            }
            return new PercolateQuery(documentType, queryStore, documentSource, candidateMatchesQuery, searcher, verifiedMatchesQuery);
        }

        Query createCandidateQuery(IndexReader indexReader, DocumentMapper documentMapper) throws IOException {
            List<Term> extractedTerms = new ArrayList<>();
            // include extractionResultField:failed, because docs with this term have no extractedTermsField
            // and otherwise we would fail to return these docs. Docs that failed query term extraction
            // always need to be verified by MemoryIndex:
            extractedTerms.add(new Term(extractionResultField.name(), EXTRACTION_FAILED));

            BooleanQuery.Builder bq = new BooleanQuery.Builder();
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
                    extractedTerms.add(new Term(queryTermsField.name(), builder.toBytesRef()));
                }
            }

            if (extractedTerms.size() != 0) {
                bq.add(new TermsQuery(extractedTerms), BooleanClause.Occur.SHOULD);
            }

            for (FieldInfo info : reader.getFieldInfos()) {
                if (info == null || info.getPointDimensionCount() == 0) {
                    continue;
                }

                FieldMapper fieldMapper = documentMapper.mappers().getMapper(info.name);
                if (fieldMapper == null) {
                    continue;
                }

                MappedFieldType fieldType = fieldMapper.fieldType();
                PointValues values = reader.getPointValues();
                if (values == null) {
                    continue;
                }
                MappedRange mappedRange = ranges.get(fieldType.typeName());
                byte[] packedValue = values.getMinPackedValue(info.name);
                Object value = mappedRange.rangeType.decodePackedValue(packedValue);
                BooleanQuery.Builder rangeBq = new BooleanQuery.Builder();
                rangeBq.add(mappedRange.fromField.rangeQuery(null, value, true, true), MUST);
                rangeBq.add(mappedRange.toField.rangeQuery(value, null, true, true), MUST);
                bq.add(rangeBq.build(), BooleanClause.Occur.SHOULD);
            }
            return bq.build();
        }

    }

    static class MappedRange {

        final RangeType rangeType;
        final MappedFieldType fromField;
        final MappedFieldType toField;

        MappedRange(RangeType rangeType, MappedFieldType fromField, MappedFieldType toField) {
            this.rangeType = rangeType;
            this.fromField = fromField;
            this.toField = toField;
        }

        Field createFromField(byte[] value) {
            return rangeType.createField(fromField.name(), value);
        }

        Field createToField(byte[] value) {
            return rangeType.createField(toField.name(), value);
        }

    }

    private final boolean mapUnmappedFieldAsString;
    private final QueryShardContext queryShardContext;
    private KeywordFieldMapper queryTermsField;
    private KeywordFieldMapper extractionResultField;
    private BinaryFieldMapper queryBuilderField;

    private NumberFieldMapper intFromField;
    private NumberFieldMapper intToField;
    private NumberFieldMapper longFromField;
    private NumberFieldMapper longToField;
    private NumberFieldMapper halfFloatFromField;
    private NumberFieldMapper halfFloatToField;
    private NumberFieldMapper floatFromField;
    private NumberFieldMapper floatToField;
    private NumberFieldMapper doubleFromField;
    private NumberFieldMapper doubleToField;
    private IpFieldMapper ipFromField;
    private IpFieldMapper ipToField;

    public PercolatorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                 Settings indexSettings, MultiFields multiFields, CopyTo copyTo, QueryShardContext queryShardContext,
                                 KeywordFieldMapper queryTermsField, KeywordFieldMapper extractionResultField,
                                 BinaryFieldMapper queryBuilderField, NumberFieldMapper intFromField, NumberFieldMapper intToField,
                                 NumberFieldMapper longFromField, NumberFieldMapper longToField, NumberFieldMapper halfFloatFromField,
                                 NumberFieldMapper halfFloatToField, NumberFieldMapper floatFromField, NumberFieldMapper floatToField,
                                 NumberFieldMapper doubleFromField, NumberFieldMapper doubleToField, IpFieldMapper ipFromField,
                                 IpFieldMapper ipToField) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.queryShardContext = queryShardContext;
        this.queryTermsField = queryTermsField;
        this.extractionResultField = extractionResultField;
        this.queryBuilderField = queryBuilderField;
        this.mapUnmappedFieldAsString = INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING.get(indexSettings);

        this.intFromField = intFromField;
        this.intToField = intToField;
        this.longFromField = longFromField;
        this.longToField = longToField;
        this.halfFloatFromField = halfFloatFromField;
        this.halfFloatToField = halfFloatToField;
        this.floatFromField = floatFromField;
        this.floatToField = floatToField;
        this.doubleFromField = doubleFromField;
        this.doubleToField = doubleToField;
        this.ipFromField = ipFromField;
        this.ipToField = ipToField;
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        PercolatorFieldMapper updated = (PercolatorFieldMapper) super.updateFieldType(fullNameToFieldType);
        KeywordFieldMapper queryTermsUpdated = (KeywordFieldMapper) queryTermsField.updateFieldType(fullNameToFieldType);
        KeywordFieldMapper extractionResultUpdated = (KeywordFieldMapper) extractionResultField.updateFieldType(fullNameToFieldType);
        BinaryFieldMapper queryBuilderUpdated = (BinaryFieldMapper) queryBuilderField.updateFieldType(fullNameToFieldType);

        NumberFieldMapper intFromFieldUpdated = (NumberFieldMapper) intFromField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper intToFieldUpdated = (NumberFieldMapper) intToField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper longFromFieldUpdated = (NumberFieldMapper) longFromField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper longToFieldUpdated = (NumberFieldMapper) longToField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper halfFloatFromFieldUpdated = (NumberFieldMapper) halfFloatFromField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper halfFloatToFieldUpdated = (NumberFieldMapper) halfFloatToField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper floatFromFieldUpdated = (NumberFieldMapper) floatFromField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper floatToFieldUpdated = (NumberFieldMapper) floatToField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper doubleFromFieldUpdated = (NumberFieldMapper) doubleFromField.updateFieldType(fullNameToFieldType);
        NumberFieldMapper doubleToFieldUpdated = (NumberFieldMapper) doubleToField.updateFieldType(fullNameToFieldType);
        IpFieldMapper ipFromFieldUpdated = (IpFieldMapper) ipFromField.updateFieldType(fullNameToFieldType);
        IpFieldMapper ipToFieldUpdated = (IpFieldMapper) ipToField.updateFieldType(fullNameToFieldType);

        if (updated == this && queryTermsUpdated == queryTermsField && extractionResultUpdated == extractionResultField
                && queryBuilderUpdated == queryBuilderField && intFromFieldUpdated == intFromField && intToFieldUpdated == intToField &&
                longFromFieldUpdated == longFromField && longToFieldUpdated == longToField &&
                halfFloatFromFieldUpdated == halfFloatFromField && halfFloatToFieldUpdated == halfFloatToField &&
                floatFromFieldUpdated == floatFromField && floatToFieldUpdated == floatToField &&
                doubleFromFieldUpdated == doubleFromField && doubleToFieldUpdated == doubleToField && ipFromFieldUpdated == ipFromField &&
                ipToFieldUpdated == ipToField) {
            return this;
        }
        if (updated == this) {
            updated = (PercolatorFieldMapper) updated.clone();
        }
        updated.queryTermsField = queryTermsUpdated;
        updated.extractionResultField = extractionResultUpdated;
        updated.queryBuilderField = queryBuilderUpdated;
        updated.intFromField = intFromFieldUpdated;
        updated.intToField = intToFieldUpdated;
        updated.longFromField = longFromFieldUpdated;
        updated.longToField = longToFieldUpdated;
        updated.halfFloatFromField= halfFloatFromFieldUpdated;
        updated.halfFloatToField= halfFloatToFieldUpdated;
        updated.floatFromField= floatFromFieldUpdated;
        updated.floatToField= floatToFieldUpdated;
        updated.doubleFromField= doubleFromFieldUpdated;
        updated.doubleToField= doubleToFieldUpdated;
        updated.ipFromField= ipFromFieldUpdated;
        updated.ipToField= ipToFieldUpdated;
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
        verifyRangeQueries(queryBuilder);
        // Fetching of terms, shapes and indexed scripts happen during this rewrite:
        queryBuilder = queryBuilder.rewrite(queryShardContext);

        try (XContentBuilder builder = XContentFactory.contentBuilder(QUERY_BUILDER_CONTENT_TYPE)) {
            queryBuilder.toXContent(builder, new MapParams(Collections.emptyMap()));
            builder.flush();
            byte[] queryBuilderAsBytes = BytesReference.toBytes(builder.bytes());
            context.doc().add(new Field(queryBuilderField.name(), queryBuilderAsBytes, queryBuilderField.fieldType()));
        }

        Query query = toQuery(queryShardContext, mapUnmappedFieldAsString, queryBuilder);
        extractTermsAndRanges(context, query);
        return null;
    }

    void extractTermsAndRanges(ParseContext context, Query query) {
        ParseContext.Document doc = context.doc();
        FieldType pft = (FieldType) this.fieldType();
        QueryExtractService.Result result;
        try {
            result = QueryExtractService.extractQueryTerms(query);
        } catch (QueryExtractService.UnsupportedQueryException e) {
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
        for (QueryExtractService.Range range : result.ranges) {
            MappedFieldType rangeFieldType = context.mapperService().fullName(range.fieldName);
            MappedRange mappedRange = pft.ranges.get(rangeFieldType.typeName());
            doc.add(mappedRange.createFromField(range.lowerPoint));
            doc.add(mappedRange.createToField(range.upperPoint));
        }
        if (result.verified) {
            doc.add(new Field(extractionResultField.name(), EXTRACTION_COMPLETE, extractionResultField.fieldType()));
        } else {
            doc.add(new Field(extractionResultField.name(), EXTRACTION_PARTIAL, extractionResultField.fieldType()));
        }
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
        return Arrays.<Mapper>asList(
                queryTermsField, extractionResultField, queryBuilderField, intFromField, intToField, longFromField,
                longToField, halfFloatFromField, halfFloatToField, floatFromField, floatToField, doubleFromField,
                doubleToField, ipFromField, ipToField
        ).iterator();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Fails if a range query with a date range is found based on current time
     */
    static void verifyRangeQueries(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof RangeQueryBuilder) {
            RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) queryBuilder;
            if (rangeQueryBuilder.from() instanceof String) {
                String from = (String) rangeQueryBuilder.from();
                String to = (String) rangeQueryBuilder.to();
                if (from.contains("now") || to.contains("now")) {
                    throw new IllegalArgumentException("Percolator queries containing time range queries based on the " +
                            "current time are forbidden");
                }
            }
        } else if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            List<QueryBuilder> clauses = new ArrayList<>();
            clauses.addAll(boolQueryBuilder.filter());
            clauses.addAll(boolQueryBuilder.must());
            clauses.addAll(boolQueryBuilder.mustNot());
            clauses.addAll(boolQueryBuilder.should());
            for (QueryBuilder clause : clauses) {
                verifyRangeQueries(clause);
            }
        } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
            verifyRangeQueries(((ConstantScoreQueryBuilder) queryBuilder).innerQuery());
        } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
            verifyRangeQueries(((FunctionScoreQueryBuilder) queryBuilder).query());
        } else if (queryBuilder instanceof BoostingQueryBuilder) {
            verifyRangeQueries(((BoostingQueryBuilder) queryBuilder).negativeQuery());
            verifyRangeQueries(((BoostingQueryBuilder) queryBuilder).positiveQuery());
        }
    }

}
