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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryRange;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CoveringQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class PercolatorFieldMapper extends FieldMapper {

    static final Setting<Boolean> INDEX_MAP_UNMAPPED_FIELDS_AS_TEXT_SETTING = Setting.boolSetting(
        "index.percolator.map_unmapped_fields_as_text", false, Setting.Property.IndexScope);
    static final String CONTENT_TYPE = "percolator";
    private static final FieldType FIELD_TYPE = new FieldType();

    static final byte FIELD_VALUE_SEPARATOR = 0;  // nul code point
    static final String EXTRACTION_COMPLETE = "complete";
    static final String EXTRACTION_PARTIAL = "partial";
    static final String EXTRACTION_FAILED = "failed";

    static final String EXTRACTED_TERMS_FIELD_NAME = "extracted_terms";
    static final String EXTRACTION_RESULT_FIELD_NAME = "extraction_result";
    static final String QUERY_BUILDER_FIELD_NAME = "query_builder_field";
    static final String RANGE_FIELD_NAME = "range_field";
    static final String MINIMUM_SHOULD_MATCH_FIELD_NAME = "minimum_should_match_field";

    static class Builder extends FieldMapper.Builder<Builder> {

        private final Supplier<QueryShardContext> queryShardContext;

        Builder(String fieldName, Supplier<QueryShardContext> queryShardContext) {
            super(fieldName, FIELD_TYPE);
            this.queryShardContext = queryShardContext;
        }

        @Override
        public PercolatorFieldMapper build(BuilderContext context) {
            String contextName = buildFullName(context);
            return new PercolatorFieldMapper(name(), fieldType,
                new PercolatorFieldType(contextName, getMapUnmappedFieldAsText(context.indexSettings()), meta),
                context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo, queryShardContext);
        }

        private static boolean getMapUnmappedFieldAsText(Settings indexSettings) {
            return INDEX_MAP_UNMAPPED_FIELDS_AS_TEXT_SETTING.get(indexSettings);
        }
    }

    static class TypeParser implements FieldMapper.TypeParser {

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(name, parserContext.queryShardContextSupplier());
        }
    }

    public static String queryTermsFieldName(String parent) {
        return parent + "." + EXTRACTED_TERMS_FIELD_NAME;
    }

    public static String extractionResultFieldName(String parent) {
        return parent + "." + EXTRACTION_RESULT_FIELD_NAME;
    }

    public static String queryBuilderFieldName(String parent) {
        return parent + "." + QUERY_BUILDER_FIELD_NAME;
    }

    public static String rangeFieldName(String parent) {
        return parent + "." + RANGE_FIELD_NAME;
    }

    public static String minimumShouldMatchFieldName(String parent) {
        return parent + "." + MINIMUM_SHOULD_MATCH_FIELD_NAME;
    }

    static class PercolatorFieldType extends MappedFieldType {

        final String queryTermsField;
        final String extractionResultField;
        final String queryBuilderField;
        final String minimumShouldMatchField;
        final String rangeField;
        final boolean mapUnmappedFieldsAsText;

        PercolatorFieldType(String name, boolean mapUnmappedFieldsAsText, Map<String, String> meta) {
            super(name, true, false, meta);
            this.mapUnmappedFieldsAsText = mapUnmappedFieldsAsText;
            this.queryTermsField = queryTermsFieldName(name);
            this.extractionResultField = extractionResultFieldName(name);
            this.queryBuilderField = queryBuilderFieldName(name);
            this.minimumShouldMatchField = minimumShouldMatchFieldName(name);
            this.rangeField = rangeFieldName(name);
        }

        PercolatorFieldType(PercolatorFieldType ref) {
            super(ref);
            queryTermsField = ref.queryTermsField;
            extractionResultField = ref.extractionResultField;
            queryBuilderField = ref.queryBuilderField;
            rangeField = ref.rangeField;
            minimumShouldMatchField = ref.minimumShouldMatchField;
            mapUnmappedFieldsAsText = ref.mapUnmappedFieldsAsText;
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
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Percolator fields are not searchable directly, use a percolate query instead");
        }

        Query percolateQuery(String name, PercolateQuery.QueryStore queryStore, List<BytesReference> documents,
                             IndexSearcher searcher, boolean excludeNestedDocuments, Version indexVersion) throws IOException {
            IndexReader indexReader = searcher.getIndexReader();
            Tuple<BooleanQuery, Boolean> t = createCandidateQuery(indexReader, indexVersion);
            Query candidateQuery = t.v1();
            boolean canUseMinimumShouldMatchField = t.v2();

            Query verifiedMatchesQuery;
            // We can only skip the MemoryIndex verification when percolating a single non nested document. We cannot
            // skip MemoryIndex verification when percolating multiple documents, because when terms and
            // ranges are extracted from IndexReader backed by a RamDirectory holding multiple documents we do
            // not know to which document the terms belong too and for certain queries we incorrectly emit candidate
            // matches as actual match.
            if (canUseMinimumShouldMatchField && indexReader.maxDoc() == 1) {
                verifiedMatchesQuery = new TermQuery(new Term(extractionResultField, EXTRACTION_COMPLETE));
            } else {
                verifiedMatchesQuery = new MatchNoDocsQuery("multiple or nested docs or CoveringQuery could not be used");
            }
            Query filter = null;
            if (excludeNestedDocuments) {
                filter = Queries.newNonNestedFilter();
            }
            return new PercolateQuery(name, queryStore, documents, candidateQuery, searcher, filter, verifiedMatchesQuery);
        }

        Tuple<BooleanQuery, Boolean> createCandidateQuery(IndexReader indexReader, Version indexVersion) throws IOException {
            Tuple<List<BytesRef>, Map<String, List<byte[]>>> t = extractTermsAndRanges(indexReader);
            List<BytesRef> extractedTerms = t.v1();
            Map<String, List<byte[]>> encodedPointValuesByField = t.v2();
            // `1 + ` is needed to take into account the EXTRACTION_FAILED should clause
            boolean canUseMinimumShouldMatchField = 1 + extractedTerms.size() + encodedPointValuesByField.size() <=
                BooleanQuery.getMaxClauseCount();

            List<Query> subQueries = new ArrayList<>();
            for (Map.Entry<String, List<byte[]>> entry : encodedPointValuesByField.entrySet()) {
                String rangeFieldName = entry.getKey();
                List<byte[]> encodedPointValues = entry.getValue();
                byte[] min = encodedPointValues.get(0);
                byte[] max = encodedPointValues.get(1);
                Query query = BinaryRange.newIntersectsQuery(rangeField, encodeRange(rangeFieldName, min, max));
                subQueries.add(query);
            }

            BooleanQuery.Builder candidateQuery = new BooleanQuery.Builder();
            if (canUseMinimumShouldMatchField) {
                LongValuesSource valuesSource = LongValuesSource.fromIntField(minimumShouldMatchField);
                for (BytesRef extractedTerm : extractedTerms) {
                    subQueries.add(new TermQuery(new Term(queryTermsField, extractedTerm)));
                }
                candidateQuery.add(new CoveringQuery(subQueries, valuesSource), BooleanClause.Occur.SHOULD);
            } else {
                candidateQuery.add(new TermInSetQuery(queryTermsField, extractedTerms), BooleanClause.Occur.SHOULD);
                for (Query subQuery : subQueries) {
                    candidateQuery.add(subQuery, BooleanClause.Occur.SHOULD);
                }
            }
            // include extractionResultField:failed, because docs with this term have no extractedTermsField
            // and otherwise we would fail to return these docs. Docs that failed query term extraction
            // always need to be verified by MemoryIndex:
            candidateQuery.add(new TermQuery(new Term(extractionResultField, EXTRACTION_FAILED)), BooleanClause.Occur.SHOULD);
            return new Tuple<>(candidateQuery.build(), canUseMinimumShouldMatchField);
        }

        // This was extracted the method above, because otherwise it is difficult to test what terms are included in
        // the query in case a CoveringQuery is used (it does not have a getter to retrieve the clauses)
        Tuple<List<BytesRef>, Map<String, List<byte[]>>> extractTermsAndRanges(IndexReader indexReader) throws IOException {
            List<BytesRef> extractedTerms = new ArrayList<>();
            Map<String, List<byte[]>> encodedPointValuesByField = new HashMap<>();

            LeafReader reader = indexReader.leaves().get(0).reader();
            for (FieldInfo info : reader.getFieldInfos()) {
                Terms terms = reader.terms(info.name);
                if (terms != null) {
                    BytesRef fieldBr = new BytesRef(info.name);
                    TermsEnum tenum = terms.iterator();
                    for (BytesRef term = tenum.next(); term != null; term = tenum.next()) {
                        BytesRefBuilder builder = new BytesRefBuilder();
                        builder.append(fieldBr);
                        builder.append(FIELD_VALUE_SEPARATOR);
                        builder.append(term);
                        extractedTerms.add(builder.toBytesRef());
                    }
                }
                if (info.getPointIndexDimensionCount() == 1) { // not != 0 because range fields are not supported
                    PointValues values = reader.getPointValues(info.name);
                    List<byte[]> encodedPointValues = new ArrayList<>();
                    encodedPointValues.add(values.getMinPackedValue().clone());
                    encodedPointValues.add(values.getMaxPackedValue().clone());
                    encodedPointValuesByField.put(info.name, encodedPointValues);
                }
            }
            return new Tuple<>(extractedTerms, encodedPointValuesByField);
        }

    }

    private final Supplier<QueryShardContext> queryShardContext;
    private final String queryTermsField;
    private final String extractionResultField;
    private final String queryBuilderField;
    private final String minimumShouldMatchFieldMapper;
    private final String rangeFieldMapper;
    private final boolean mapUnmappedFieldsAsText;


    PercolatorFieldMapper(String simpleName, FieldType fieldType, PercolatorFieldType mappedFieldType,
                          Settings indexSettings, MultiFields multiFields, CopyTo copyTo,
                          Supplier<QueryShardContext> queryShardContext) {
        super(simpleName, fieldType, mappedFieldType, indexSettings, multiFields, copyTo);
        this.queryShardContext = queryShardContext;
        this.queryTermsField = queryTermsFieldName(mappedFieldType.name());
        this.extractionResultField = extractionResultFieldName(mappedFieldType.name());
        this.queryBuilderField = queryBuilderFieldName(mappedFieldType.name());
        this.minimumShouldMatchFieldMapper = minimumShouldMatchFieldName(mappedFieldType.name());
        this.rangeFieldMapper = rangeFieldName(mappedFieldType.name());
        this.mapUnmappedFieldsAsText = mappedFieldType.mapUnmappedFieldsAsText;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        QueryShardContext queryShardContext = this.queryShardContext.get();
        if (context.doc().getField(queryBuilderField) != null) {
            // If a percolator query has been defined in an array object then multiple percolator queries
            // could be provided. In order to prevent this we fail if we try to parse more than one query
            // for the current document.
            throw new IllegalArgumentException("a document can only contain one percolator query");
        }

        configureContext(queryShardContext, isMapUnmappedFieldAsText());

        XContentParser parser = context.parser();
        QueryBuilder queryBuilder = parseQueryBuilder(
                parser, parser.getTokenLocation()
        );
        verifyQuery(queryBuilder);
        // Fetching of terms, shapes and indexed scripts happen during this rewrite:
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(queryBuilder, queryShardContext, future);
        queryBuilder = future.actionGet();

        Version indexVersion = context.mapperService().getIndexSettings().getIndexVersionCreated();
        createQueryBuilderField(indexVersion, queryBuilderField, queryBuilder, context);

        QueryBuilder queryBuilderForProcessing = queryBuilder.rewrite(new QueryShardContext(queryShardContext));
        Query query = queryBuilderForProcessing.toQuery(queryShardContext);
        processQuery(query, context);
    }

    static void createQueryBuilderField(Version indexVersion, String qbField,
                                        QueryBuilder queryBuilder, ParseContext context) throws IOException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            try (OutputStreamStreamOutput out  = new OutputStreamStreamOutput(stream)) {
                out.setVersion(indexVersion);
                out.writeVInt(1);
                out.writeVInt(1);   // TODO remove these, legacy cruft from when we used BinaryFieldMapper to write
                out.writeNamedWriteable(queryBuilder);
                byte[] queryBuilderAsBytes = stream.toByteArray();
                context.doc().add(new BinaryDocValuesField(qbField, new BytesRef(queryBuilderAsBytes)));
            }
        }
    }

    void processQuery(Query query, ParseContext context) {
        ParseContext.Document doc = context.doc();
        QueryAnalyzer.Result result;
        Version indexVersion = context.mapperService().getIndexSettings().getIndexVersionCreated();
        result = QueryAnalyzer.analyze(query, indexVersion);
        if (result == QueryAnalyzer.Result.UNKNOWN) {
            doc.add(new KeywordFieldMapper.KeywordField(extractionResultField, new BytesRef(EXTRACTION_FAILED)));
            return;
        }
        for (QueryAnalyzer.QueryExtraction extraction : result.extractions) {
            if (extraction.term != null) {
                BytesRefBuilder builder = new BytesRefBuilder();
                builder.append(new BytesRef(extraction.field()));
                builder.append(FIELD_VALUE_SEPARATOR);
                builder.append(extraction.bytes());
                doc.add(new KeywordFieldMapper.KeywordField(queryTermsField, builder.toBytesRef()));
            } else if (extraction.range != null) {
                byte[] min = extraction.range.lowerPoint;
                byte[] max = extraction.range.upperPoint;
                doc.add(new BinaryRange(rangeFieldMapper, encodeRange(extraction.range.fieldName, min, max)));
            }
        }

        if (result.matchAllDocs) {
            doc.add(new KeywordFieldMapper.KeywordField(extractionResultField, new BytesRef(EXTRACTION_FAILED)));
            if (result.verified) {
                doc.add(new KeywordFieldMapper.KeywordField(extractionResultField, new BytesRef(EXTRACTION_COMPLETE)));
            }
        } else if (result.verified) {
            doc.add(new KeywordFieldMapper.KeywordField(extractionResultField, new BytesRef(EXTRACTION_COMPLETE)));
        } else {
            doc.add(new KeywordFieldMapper.KeywordField(extractionResultField, new BytesRef(EXTRACTION_PARTIAL)));
        }

        createFieldNamesField(context);
        doc.add(new NumericDocValuesField(minimumShouldMatchFieldMapper, result.minimumShouldMatch));
    }

    static void configureContext(QueryShardContext context, boolean mapUnmappedFieldsAsString) {
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
    }

    private static QueryBuilder parseQueryBuilder(XContentParser parser, XContentLocation location) {
        try {
            return parseInnerQueryBuilder(parser);
        } catch (IOException e) {
            throw new ParsingException(location, "Failed to parse", e);
        }
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("should not be invoked");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    boolean isMapUnmappedFieldAsText() {
        return mapUnmappedFieldsAsText;
    }

    /**
     * Fails if a percolator contains an unsupported query. The following queries are not supported:
     * 1) a has_child query
     * 2) a has_parent query
     */
    static void verifyQuery(QueryBuilder queryBuilder) {
        if (queryBuilder.getName().equals("has_child")) {
            throw new IllegalArgumentException("the [has_child] query is unsupported inside a percolator query");
        } else if (queryBuilder.getName().equals("has_parent")) {
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
        } else if (queryBuilder instanceof DisMaxQueryBuilder) {
            DisMaxQueryBuilder disMaxQueryBuilder = (DisMaxQueryBuilder) queryBuilder;
            for (QueryBuilder innerQueryBuilder : disMaxQueryBuilder.innerQueries()) {
                verifyQuery(innerQueryBuilder);
            }
        }
    }

    static byte[] encodeRange(String rangeFieldName, byte[] minEncoded, byte[] maxEncoded) {
        assert minEncoded.length == maxEncoded.length;
        byte[] bytes = new byte[BinaryRange.BYTES * 2];

        // First compute hash for field name and write the full hash into the byte array
        BytesRef fieldAsBytesRef = new BytesRef(rangeFieldName);
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        MurmurHash3.hash128(fieldAsBytesRef.bytes, fieldAsBytesRef.offset, fieldAsBytesRef.length, 0, hash);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(hash.h1).putLong(hash.h2).putLong(hash.h1).putLong(hash.h2);
        assert bb.position() == bb.limit();

        // Secondly, overwrite the min and max encoded values in the byte array
        // This way we are able to reuse as much as possible from the hash for any range type.
        int offset = BinaryRange.BYTES - minEncoded.length;
        System.arraycopy(minEncoded, 0, bytes, offset, minEncoded.length);
        System.arraycopy(maxEncoded, 0, bytes, BinaryRange.BYTES + offset, maxEncoded.length);
        return bytes;
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }
}
