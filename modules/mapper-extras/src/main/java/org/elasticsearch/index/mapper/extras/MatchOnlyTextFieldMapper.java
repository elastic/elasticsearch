/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.StringStoredFieldFieldLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.TextDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link FieldMapper} for full-text fields that only indexes
 * {@link IndexOptions#DOCS} and runs positional queries by looking at the
 * _source.
 */
public class MatchOnlyTextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "match_only_text";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

    }

    public static class Builder extends FieldMapper.Builder {

        private final Version indexCreatedVersion;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final TextParams.Analyzers analyzers;

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            this(name, Version.CURRENT, indexAnalyzers);
        }

        public Builder(String name, Version indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((MatchOnlyTextFieldMapper) m).indexAnalyzer,
                m -> ((MatchOnlyTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        private MatchOnlyTextFieldType buildFieldType(MapperBuilderContext context) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            TextSearchInfo tsi = new TextSearchInfo(Defaults.FIELD_TYPE, null, searchAnalyzer, searchQuoteAnalyzer);
            MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType(
                context.buildFullName(name),
                tsi,
                indexAnalyzer,
                context.isSourceSynthetic(),
                meta.getValue()
            );
            return ft;
        }

        @Override
        public MatchOnlyTextFieldMapper build(MapperBuilderContext context) {
            MatchOnlyTextFieldType tft = buildFieldType(context);
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            return new MatchOnlyTextFieldMapper(
                name,
                Defaults.FIELD_TYPE,
                tft,
                multiFields,
                copyTo.build(),
                context.isSourceSynthetic(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

    public static class MatchOnlyTextFieldType extends StringFieldType {

        private final Analyzer indexAnalyzer;
        private final TextFieldType textFieldType;

        public MatchOnlyTextFieldType(
            String name,
            TextSearchInfo tsi,
            Analyzer indexAnalyzer,
            boolean isSyntheticSource,
            Map<String, String> meta
        ) {
            super(name, true, false, false, tsi, meta);
            this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
            this.textFieldType = new TextFieldType(name, isSyntheticSource);
        }

        public MatchOnlyTextFieldType(String name) {
            this(
                name,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                Lucene.STANDARD_ANALYZER,
                false,
                Collections.emptyMap()
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return TextFieldMapper.CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> getValueFetcherProvider(
            SearchExecutionContext searchExecutionContext
        ) {
            if (searchExecutionContext.isSourceEnabled() == false) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + CONTENT_TYPE + "] cannot run positional queries since [_source] is disabled."
                );
            }
            if (searchExecutionContext.isSourceSynthetic()) {
                String name = storedFieldNameForSyntheticSource();
                StoredFieldLoader loader = StoredFieldLoader.create(false, Set.of(name));
                return context -> {
                    LeafStoredFieldLoader leafLoader = loader.getLoader(context, null);
                    return docId -> {
                        leafLoader.advanceTo(docId);
                        return leafLoader.storedFields().get(name);
                    };
                };
            }
            ValueFetcher valueFetcher = valueFetcher(searchExecutionContext, null);
            SourceProvider sourceProvider = searchExecutionContext.lookup();
            return context -> {
                valueFetcher.setNextReader(context);
                return docID -> {
                    try {
                        return valueFetcher.fetchValues(sourceProvider.getSource(context, docID), docID, new ArrayList<>());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };
            };
        }

        private Query toQuery(Query query, SearchExecutionContext searchExecutionContext) {
            return new ConstantScoreQuery(
                new SourceConfirmedTextQuery(query, getValueFetcherProvider(searchExecutionContext), indexAnalyzer)
            );
        }

        private IntervalsSource toIntervalsSource(
            IntervalsSource source,
            Query approximation,
            SearchExecutionContext searchExecutionContext
        ) {
            return new SourceIntervalsSource(source, approximation, getValueFetcherProvider(searchExecutionContext), indexAnalyzer);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            // Disable scoring
            return new ConstantScoreQuery(super.termQuery(value, context));
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context,
            MultiTermQuery.RewriteMethod rewriteMethod
        ) {
            // Disable scoring
            return new ConstantScoreQuery(
                super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context, rewriteMethod)
            );
        }

        @Override
        public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
            return toIntervalsSource(Intervals.term(term), new TermQuery(new Term(name(), term)), context);
        }

        @Override
        public IntervalsSource prefixIntervals(BytesRef term, SearchExecutionContext context) {
            return toIntervalsSource(Intervals.prefix(term), new PrefixQuery(new Term(name(), term)), context);
        }

        @Override
        public IntervalsSource fuzzyIntervals(
            String term,
            int maxDistance,
            int prefixLength,
            boolean transpositions,
            SearchExecutionContext context
        ) {
            FuzzyQuery fuzzyQuery = new FuzzyQuery(
                new Term(name(), term),
                maxDistance,
                prefixLength,
                128,
                transpositions,
                MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
            );
            IntervalsSource fuzzyIntervals = Intervals.multiterm(fuzzyQuery.getAutomata(), term);
            return toIntervalsSource(fuzzyIntervals, fuzzyQuery, context);
        }

        @Override
        public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.wildcard(pattern),
                new MatchAllDocsQuery(), // wildcard queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, SearchExecutionContext queryShardContext)
            throws IOException {
            final Query query = textFieldType.phraseQuery(stream, slop, enablePosIncrements, queryShardContext);
            return toQuery(query, queryShardContext);
        }

        @Override
        public Query multiPhraseQuery(
            TokenStream stream,
            int slop,
            boolean enablePositionIncrements,
            SearchExecutionContext queryShardContext
        ) throws IOException {
            final Query query = textFieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, queryShardContext);
            return toQuery(query, queryShardContext);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext queryShardContext)
            throws IOException {
            final Query query = textFieldType.phrasePrefixQuery(stream, slop, maxExpansions, queryShardContext);
            return toQuery(query, queryShardContext);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (fieldDataContext.fielddataOperation() != FielddataOperation.SCRIPT) {
                throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support sorting and aggregations");
            }
            if (textFieldType.isSyntheticSource()) {
                return (cache, breaker) -> new StoredFieldSortedBinaryIndexFieldData(
                    storedFieldNameForSyntheticSource(),
                    CoreValuesSourceType.KEYWORD,
                    TextDocValuesField::new
                ) {
                    @Override
                    protected BytesRef storedToBytesRef(Object stored) {
                        return new BytesRef((String) stored);
                    }
                };
            }
            return new SourceValueFetcherSortedBinaryIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                SourceValueFetcher.toString(fieldDataContext.sourcePathsLookup().apply(name())),
                fieldDataContext.lookupSupplier().get(),
                TextDocValuesField::new
            );
        }

        private String storedFieldNameForSyntheticSource() {
            return name() + "._original";
        }
    }

    private final Version indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer indexAnalyzer;
    private final int positionIncrementGap;
    private final boolean storeSource;
    private final FieldType fieldType;

    private MatchOnlyTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        MatchOnlyTextFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        boolean storeSource,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, false, null);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        this.fieldType = fieldType;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
        this.storeSource = storeSource;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), indexCreatedVersion, indexAnalyzers).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();

        if (value == null) {
            return;
        }

        Field field = new Field(fieldType().name(), value, fieldType);
        context.doc().add(field);
        context.addToFieldNames(fieldType().name());

        if (storeSource) {
            context.doc().add(new StoredField(fieldType().storedFieldNameForSyntheticSource(), value));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public MatchOnlyTextFieldType fieldType() {
        return (MatchOnlyTextFieldType) super.fieldType();
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        return new StringStoredFieldFieldLoader(fieldType().storedFieldNameForSyntheticSource(), simpleName(), null) {
            @Override
            protected void write(XContentBuilder b, Object value) throws IOException {
                b.value((String) value);
            }
        };
    }
}
