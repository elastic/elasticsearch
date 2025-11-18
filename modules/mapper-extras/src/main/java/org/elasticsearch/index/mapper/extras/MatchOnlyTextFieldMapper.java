/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.UTF8DecodingReader;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFamilyFieldType;
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
import org.elasticsearch.xcontent.XContentString;

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
        public static final FieldType FIELD_TYPE;

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

    }

    public static class Builder extends TextFamilyBuilder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final TextParams.Analyzers analyzers;
        private final boolean storedFieldInBinaryFormat;

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexAnalyzers indexAnalyzers,
            boolean storedFieldInBinaryFormat,
            boolean isWithinMultiField
        ) {
            super(name, indexCreatedVersion, isWithinMultiField);
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((MatchOnlyTextFieldMapper) m).indexAnalyzer,
                m -> ((MatchOnlyTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
            this.storedFieldInBinaryFormat = storedFieldInBinaryFormat;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        private MatchOnlyTextFieldType buildFieldType(MapperBuilderContext context, MultiFields multiFields) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            TextSearchInfo tsi = new TextSearchInfo(Defaults.FIELD_TYPE, null, searchAnalyzer, searchQuoteAnalyzer);
            return new MatchOnlyTextFieldType(
                context.buildFullName(leafName()),
                tsi,
                indexAnalyzer,
                context.isSourceSynthetic(),
                meta.getValue(),
                isWithinMultiField(),
                storedFieldInBinaryFormat,
                // match only text fields are not stored by definition
                TextFieldMapper.SyntheticSourceHelper.syntheticSourceDelegate(false, multiFields)
            );
        }

        @Override
        public MatchOnlyTextFieldMapper build(MapperBuilderContext context) {
            BuilderParams builderParams = builderParams(this, context);
            MatchOnlyTextFieldType tft = buildFieldType(context, builderParams.multiFields());
            return new MatchOnlyTextFieldMapper(leafName(), Defaults.FIELD_TYPE, tft, builderParams, this);
        }
    }

    private static boolean isSyntheticSourceStoredFieldInBinaryFormat(IndexVersion indexCreatedVersion) {
        return indexCreatedVersion.onOrAfter(IndexVersions.MATCH_ONLY_TEXT_STORED_AS_BYTES)
            || indexCreatedVersion.between(
                IndexVersions.MATCH_ONLY_TEXT_STORED_AS_BYTES_BACKPORT_8_X,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            );
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(
            n,
            c.indexVersionCreated(),
            c.getIndexAnalyzers(),
            isSyntheticSourceStoredFieldInBinaryFormat(c.indexVersionCreated()),
            c.isWithinMultiField()
        )
    );

    public static class MatchOnlyTextFieldType extends TextFamilyFieldType {

        private final Analyzer indexAnalyzer;
        private final TextFieldType textFieldType;
        private final boolean storedFieldInBinaryFormat;

        public MatchOnlyTextFieldType(
            String name,
            TextSearchInfo tsi,
            Analyzer indexAnalyzer,
            boolean isSyntheticSource,
            Map<String, String> meta,
            boolean withinMultiField,
            boolean storedFieldInBinaryFormat,
            KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate
        ) {
            super(name, IndexType.terms(true, false), false, tsi, meta, isSyntheticSource, withinMultiField);
            this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
            this.textFieldType = new TextFieldType(name, isSyntheticSource, withinMultiField, syntheticSourceDelegate);
            this.storedFieldInBinaryFormat = storedFieldInBinaryFormat;
        }

        public MatchOnlyTextFieldType(String name) {
            this(
                name,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                Lucene.STANDARD_ANALYZER,
                false,
                Collections.emptyMap(),
                false,
                false,
                null
            );
        }

        /**
         * Returns whether this field can use its delegate keyword field for synthetic source.
         *
         * Note, this method is a copy of the one in {@link TextFieldType}. This is because match only text uses a more optimized
         * representation of a string, namely {@link XContentString}, which text currently does not.
         */
        private boolean canUseSyntheticSourceDelegateForSyntheticSource(final XContentString value) {
            if (textFieldType.syntheticSourceDelegate().isPresent()) {
                // if the keyword field is going to be ignored, then we can't rely on it for synthetic source
                return textFieldType.syntheticSourceDelegate().get().ignoreAbove().isIgnored(value) == false;
            }
            return false;
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

            // if synthetic source is enabled, then fetch the value from one of the valid source providers
            if (searchExecutionContext.isSourceSynthetic()) {
                if (isWithinMultiField()) {
                    // fetch the value from parent
                    return parentFieldFetcher(searchExecutionContext);
                } else if (textFieldType.syntheticSourceDelegate().isPresent()) {
                    // otherwise, if there is a delegate field, fetch the value from it
                    return delegateFieldFetcher(searchExecutionContext, textFieldType.syntheticSourceDelegate().get());
                } else {
                    // otherwise, fetch the value from self
                    return storedFieldFetcher(name(), syntheticSourceFallbackFieldName());
                }
            }

            // otherwise, synthetic source must be disabled, so fetch the value directly from _source
            return sourceFieldFetcher(searchExecutionContext);
        }

        /**
         * Returns a function that will fetch values directly from _source.
         */
        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> sourceFieldFetcher(
            final SearchExecutionContext searchExecutionContext
        ) {
            return context -> {
                ValueFetcher valueFetcher = valueFetcher(searchExecutionContext, null);
                SourceProvider sourceProvider = searchExecutionContext.lookup();
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

        /**
         * Returns a function that will fetch fields from the parent field.
         */
        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> parentFieldFetcher(
            final SearchExecutionContext searchExecutionContext
        ) {
            assert searchExecutionContext.isSourceSynthetic() : "Synthetic source should be enabled";

            String parentFieldName = searchExecutionContext.parentPath(name());
            var parent = searchExecutionContext.lookup().fieldType(parentFieldName);

            if (parent instanceof KeywordFieldMapper.KeywordFieldType keywordParent
                && keywordParent.ignoreAbove().valuesPotentiallyIgnored()) {
                final String parentFallbackFieldName = keywordParent.syntheticSourceFallbackFieldName();
                if (parent.isStored()) {
                    return storedFieldFetcher(parentFieldName, parentFallbackFieldName);
                } else if (parent.hasDocValues()) {
                    var ifd = searchExecutionContext.getForField(parent, MappedFieldType.FielddataOperation.SEARCH);
                    return combineFieldFetchers(docValuesFieldFetcher(ifd), storedFieldFetcher(parentFallbackFieldName));
                }
            }

            if (parent.isStored()) {
                return storedFieldFetcher(parentFieldName);
            } else if (parent.hasDocValues()) {
                var ifd = searchExecutionContext.getForField(parent, MappedFieldType.FielddataOperation.SEARCH);
                return docValuesFieldFetcher(ifd);
            } else {
                assert false : "parent field should either be stored or have doc values";
                return sourceFieldFetcher(searchExecutionContext);
            }
        }

        /**
         * Returns a function that will fetch the fields from the delegate field (ex. keyword multi field).
         */
        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> delegateFieldFetcher(
            final SearchExecutionContext searchExecutionContext,
            final KeywordFieldMapper.KeywordFieldType keywordDelegate
        ) {
            if (keywordDelegate.ignoreAbove().valuesPotentiallyIgnored()) {
                // because we don't know whether the delegate field will be ignored during parsing, we must also check the current field
                String fieldName = name();
                String fallbackName = syntheticSourceFallbackFieldName();

                // delegate field names
                String delegateFieldName = keywordDelegate.name();
                String delegateFieldFallbackName = keywordDelegate.syntheticSourceFallbackFieldName();

                if (keywordDelegate.isStored()) {
                    return storedFieldFetcher(delegateFieldName, delegateFieldFallbackName, fieldName, fallbackName);
                } else if (keywordDelegate.hasDocValues()) {
                    var ifd = searchExecutionContext.getForField(keywordDelegate, MappedFieldType.FielddataOperation.SEARCH);
                    return combineFieldFetchers(
                        docValuesFieldFetcher(ifd),
                        storedFieldFetcher(delegateFieldFallbackName, fieldName, fallbackName)
                    );
                }
            }

            if (keywordDelegate.isStored()) {
                return storedFieldFetcher(keywordDelegate.name());
            } else if (keywordDelegate.hasDocValues()) {
                var ifd = searchExecutionContext.getForField(keywordDelegate, MappedFieldType.FielddataOperation.SEARCH);
                return docValuesFieldFetcher(ifd);
            } else {
                assert false : "multi field should either be stored or have doc values";
                return sourceFieldFetcher(searchExecutionContext);
            }
        }

        private static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> docValuesFieldFetcher(
            IndexFieldData<?> ifd
        ) {
            return context -> {
                var sortedBinaryDocValues = ifd.load(context).getBytesValues();
                return docId -> {
                    if (sortedBinaryDocValues.advanceExact(docId)) {
                        var values = new ArrayList<>(sortedBinaryDocValues.docValueCount());
                        for (int i = 0; i < sortedBinaryDocValues.docValueCount(); i++) {
                            values.add(sortedBinaryDocValues.nextValue().utf8ToString());
                        }
                        return values;
                    } else {
                        return List.of();
                    }
                };
            };
        }

        private static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> storedFieldFetcher(String... names) {
            var loader = StoredFieldLoader.create(false, Set.of(names));
            return context -> {
                var leafLoader = loader.getLoader(context, null);
                return docId -> {
                    leafLoader.advanceTo(docId);
                    var storedFields = leafLoader.storedFields();
                    if (names.length == 1) {
                        return storedFields.get(names[0]);
                    }

                    List<Object> values = new ArrayList<>();
                    for (var name : names) {
                        var currValues = storedFields.get(name);
                        if (currValues != null) {
                            values.addAll(currValues);
                        }
                    }

                    return values;
                };
            };
        }

        private static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> combineFieldFetchers(
            IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> primaryFetcher,
            IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> secondaryFetcher
        ) {
            return context -> {
                var primaryGetter = primaryFetcher.apply(context);
                var secondaryGetter = secondaryFetcher.apply(context);
                return docId -> {
                    List<Object> values = new ArrayList<>();
                    var primary = primaryGetter.apply(docId);
                    if (primary != null) {
                        values.addAll(primary);
                    }

                    var secondary = secondaryGetter.apply(docId);
                    if (secondary != null) {
                        values.addAll(secondary);
                    }

                    assert primary != null || secondary != null;

                    return values;
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
            return toIntervalsSource(
                Intervals.prefix(term, IndexSearcher.getMaxClauseCount()),
                new PrefixQuery(new Term(name(), term)),
                context
            );
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
                IndexSearcher.getMaxClauseCount(),
                transpositions,
                MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
            );
            IntervalsSource fuzzyIntervals = Intervals.multiterm(fuzzyQuery.getAutomata(), IndexSearcher.getMaxClauseCount(), term);
            return toIntervalsSource(fuzzyIntervals, fuzzyQuery, context);
        }

        @Override
        public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.wildcard(pattern, IndexSearcher.getMaxClauseCount()),
                new MatchAllDocsQuery(), // wildcard queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.regexp(pattern, IndexSearcher.getMaxClauseCount()),
                new MatchAllDocsQuery(), // regexp queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public IntervalsSource rangeIntervals(
            BytesRef lowerTerm,
            BytesRef upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            return toIntervalsSource(
                Intervals.range(lowerTerm, upperTerm, includeLower, includeUpper, IndexSearcher.getMaxClauseCount()),
                new MatchAllDocsQuery(), // range queries can be expensive, what should the approximation be?
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

        static class BytesFromMixedStringsBytesRefBlockLoader extends BlockStoredFieldsReader.StoredFieldsBlockLoader {
            BytesFromMixedStringsBytesRefBlockLoader(String field) {
                super(field);
            }

            @Override
            public Builder builder(BlockFactory factory, int expectedCount) {
                return factory.bytesRefs(expectedCount);
            }

            @Override
            public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
                return new BlockStoredFieldsReader.Bytes(field) {
                    private final BytesRef scratch = new BytesRef();

                    @Override
                    protected BytesRef toBytesRef(Object v) {
                        if (v instanceof BytesRef b) {
                            return b;
                        } else {
                            assert v instanceof String;
                            return BlockSourceReader.toBytesRef(scratch, v.toString());
                        }
                    }
                };
            }
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (isSyntheticSourceEnabled()) {
                // if there is no synthetic source delegate, then this match only text field would've created StoredFields for us to use
                if (textFieldType.syntheticSourceDelegate().isEmpty()) {
                    if (storedFieldInBinaryFormat) {
                        return new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader(syntheticSourceFallbackFieldName());
                    } else {
                        return new BytesFromMixedStringsBytesRefBlockLoader(syntheticSourceFallbackFieldName());
                    }
                }

                // otherwise, delegate block loading to the synthetic source delegate if possible
                if (textFieldType.canUseSyntheticSourceDelegateForLoading()) {
                    return new BlockLoader.Delegating(textFieldType.syntheticSourceDelegate().get().blockLoader(blContext)) {
                        @Override
                        protected String delegatingTo() {
                            return textFieldType.syntheticSourceDelegate().get().name();
                        }
                    };
                }
            }

            /*
             * TODO: This duplicates code from TextFieldMapper
             * If this is a sub-text field try and return the parent's loader. Text
             * fields will always be slow to load and if the parent is exact then we
             * should use that instead.
             */
            String parentField = blContext.parentField(name());
            if (parentField != null) {
                MappedFieldType parent = blContext.lookup().fieldType(parentField);
                if (parent.typeName().equals(KeywordFieldMapper.CONTENT_TYPE)) {
                    KeywordFieldMapper.KeywordFieldType kwd = (KeywordFieldMapper.KeywordFieldType) parent;
                    if (kwd.hasNormalizer() == false && (kwd.hasDocValues() || kwd.isStored())) {
                        return new BlockLoader.Delegating(kwd.blockLoader(blContext)) {
                            @Override
                            protected String delegatingTo() {
                                return kwd.name();
                            }
                        };
                    }
                }
            }

            // fallback to _source (synthetic or not)
            SourceValueFetcher fetcher = SourceValueFetcher.toString(blContext.sourcePaths(name()), blContext.indexSettings());
            // MatchOnlyText never has norms, so we have to use the field names field
            BlockSourceReader.LeafIteratorLookup lookup = BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name());
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, lookup);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (fieldDataContext.fielddataOperation() != FielddataOperation.SCRIPT) {
                throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support sorting and aggregations");
            }
            if (isSyntheticSourceEnabled()) {
                return (cache, breaker) -> new StoredFieldSortedBinaryIndexFieldData(
                    syntheticSourceFallbackFieldName(),
                    CoreValuesSourceType.KEYWORD,
                    TextDocValuesField::new
                ) {
                    @Override
                    protected BytesRef storedToBytesRef(Object stored) {
                        if (stored instanceof BytesRef storedBytes) {
                            return storedBytes;
                        } else {
                            assert stored instanceof String;
                            return new BytesRef(stored.toString());
                        }
                    }
                };
            }
            return new SourceValueFetcherSortedBinaryIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                SourceValueFetcher.toString(fieldDataContext.sourcePathsLookup().apply(name()), fieldDataContext.indexSettings()),
                fieldDataContext.lookupSupplier().get(),
                TextDocValuesField::new
            );
        }
    }

    private final IndexVersion indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer indexAnalyzer;
    private final int positionIncrementGap;
    private final FieldType fieldType;
    private final boolean storedFieldInBinaryFormat;

    private MatchOnlyTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        MatchOnlyTextFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);

        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;

        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.indexCreatedVersion = builder.indexCreatedVersion();
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
        this.storedFieldInBinaryFormat = builder.storedFieldInBinaryFormat;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexAnalyzers, storedFieldInBinaryFormat, fieldType().isWithinMultiField())
            .init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final var value = context.parser().optimizedTextOrNull();

        if (value == null) {
            return;
        }

        final var utfBytes = value.bytes();
        Field field = new Field(fieldType().name(), new UTF8DecodingReader(utfBytes), fieldType);
        context.doc().add(field);
        context.addToFieldNames(fieldType().name());

        // match_only_text isn't stored, so if synthetic source needs to be supported, we must do something about it
        if (fieldType().textFieldType.storeFieldForSyntheticSource(indexCreatedVersion)) {
            // check if we can use the delegate
            if (fieldType().canUseSyntheticSourceDelegateForSyntheticSource(value)) {
                return;
            }

            // if not, then store this field explicitly so that synthetic source can load it
            final String fieldName = fieldType().syntheticSourceFallbackFieldName();
            if (storedFieldInBinaryFormat) {
                final var bytesRef = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
                context.doc().add(new StoredField(fieldName, bytesRef));
            } else {
                context.doc().add(new StoredField(fieldName, value.string()));
            }
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
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(() -> syntheticFieldLoader(fullPath(), leafName()));
    }

    private SourceLoader.SyntheticFieldLoader syntheticFieldLoader(String fullFieldName, String leafFieldName) {
        // we're using two field loaders here because we don't know who was responsible for storing this field to support synthetic source
        // on one hand, if a delegate keyword field exists, then it might've stored the field. However, this is not true if said field was
        // ignored during indexing (ex. it tripped ignore_above). Because of this uncertainty, we need multiple field loaders.

        // first field loader - to check whether the field's value was stored under this match_only_text field
        final String fieldName = fieldType().syntheticSourceFallbackFieldName();
        final var thisFieldLayer = new CompositeSyntheticFieldLoader.StoredFieldLayer(fieldName) {
            @Override
            protected void writeValue(Object value, XContentBuilder b) throws IOException {
                if (value instanceof BytesRef valueBytes) {
                    b.value(valueBytes.utf8ToString());
                } else {
                    assert value instanceof String;
                    b.value(value.toString());
                }
            }
        };

        final CompositeSyntheticFieldLoader fieldLoader = new CompositeSyntheticFieldLoader(leafFieldName, fullFieldName, thisFieldLayer);

        // second loader - to check whether the field's value was stored by a keyword delegate field
        var kwd = TextFieldMapper.SyntheticSourceHelper.getKeywordFieldMapperForSyntheticSource(this);
        if (kwd != null) {
            // merge the two field loaders into one
            var kwdFieldLoader = kwd.syntheticFieldLoader(fullPath(), leafName());
            return fieldLoader.mergedWith(kwdFieldLoader);
        }

        return fieldLoader;
    }
}
