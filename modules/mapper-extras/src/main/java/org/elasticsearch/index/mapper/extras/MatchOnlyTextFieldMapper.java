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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.text.UTF8DecodingReader;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BinaryDocValuesSyntheticFieldLoaderLayer;
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
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.SortedSetDocValuesSyntheticFieldLoaderLayer;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFamilyFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.blockloader.DelegatingBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.SlowCustomBinaryDocValuesTermInSetQuery;
import org.elasticsearch.lucene.queries.SlowCustomBinaryDocValuesTermQuery;
import org.elasticsearch.script.field.TextDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
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

    private static final FieldMapper.DocValuesParameter.Values DEFAULT_DOC_VALUES_PARAMS = new FieldMapper.DocValuesParameter.Values(
        false,
        FieldMapper.DocValuesParameter.Values.Cardinality.HIGH
    );

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
        private final FieldMapper.DocValuesParameter docValuesParameters = new FieldMapper.DocValuesParameter(
            DEFAULT_DOC_VALUES_PARAMS,
            m -> ((MatchOnlyTextFieldMapper) m).docValuesParameters
        );

        private final TextParams.Analyzers analyzers;
        private final boolean storedFieldInBinaryFormat;
        private final boolean usesBinaryDocValuesForFallbackFields;

        private Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexAnalyzers indexAnalyzers,
            boolean storedFieldInBinaryFormat,
            boolean isWithinMultiField,
            boolean usesBinaryDocValuesForFallbackFields
        ) {
            super(name, indexCreatedVersion, isWithinMultiField);
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((MatchOnlyTextFieldMapper) m).indexAnalyzer,
                m -> ((MatchOnlyTextFieldMapper) m).positionIncrementGap,
                indexCreatedVersion
            );
            this.storedFieldInBinaryFormat = storedFieldInBinaryFormat;
            this.usesBinaryDocValuesForFallbackFields = usesBinaryDocValuesForFallbackFields;
        }

        public Builder(String name, MappingParserContext context) {
            this(
                name,
                context.indexVersionCreated(),
                context.getIndexAnalyzers(),
                isSyntheticSourceStoredFieldInBinaryFormat(context.indexVersionCreated()),
                context.isWithinMultiField(),
                usesBinaryDocValuesForFallbackFields(context.getIndexSettings())
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            // when EXTENDED_DOC_VALUES_PARAMS_FF is disabled, exclude docValuesParameters from parsing
            // so doc_values configuration in the mapping is ignored and the default (disabled) is used
            if (FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled()) {
                return new Parameter<?>[] { docValuesParameters, meta };
            } else {
                return new Parameter<?>[] { meta };
            }
        }

        private static boolean usesBinaryDocValuesForFallbackFields(final IndexSettings indexSettings) {
            return indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STORE_FALLBACK_MOT_FIELDS_IN_BINARY_DOC_VALUES)
                && indexSettings.useTimeSeriesDocValuesFormat();
        }

        boolean usesBinaryDocValues() {
            return docValuesParameters.getValue().enabled()
                && docValuesParameters.getValue().cardinality() == FieldMapper.DocValuesParameter.Values.Cardinality.HIGH;
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
                TextFieldMapper.SyntheticSourceHelper.syntheticSourceDelegate(false, multiFields),
                usesBinaryDocValuesForFallbackFields,
                docValuesParameters.getValue().enabled(),
                usesBinaryDocValues()
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

    public static final TypeParser PARSER = new TypeParser(Builder::new);

    public static class MatchOnlyTextFieldType extends TextFamilyFieldType {

        private final Analyzer indexAnalyzer;
        private final TextFieldType textFieldType;
        private final boolean storedFieldInBinaryFormat;
        private final boolean usesBinaryDocValuesForFallbackFields;
        private final boolean usesBinaryDocValues;

        public MatchOnlyTextFieldType(
            String name,
            TextSearchInfo tsi,
            Analyzer indexAnalyzer,
            boolean isSyntheticSource,
            Map<String, String> meta,
            boolean withinMultiField,
            boolean storedFieldInBinaryFormat,
            KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate,
            boolean usesBinaryDocValuesForFallbackFields,
            boolean hasDocValues,
            boolean usesBinaryDocValues
        ) {
            super(name, IndexType.terms(true, hasDocValues), false, tsi, meta, isSyntheticSource, withinMultiField);
            this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer);
            this.textFieldType = new TextFieldType(name, isSyntheticSource, withinMultiField, syntheticSourceDelegate);
            this.storedFieldInBinaryFormat = storedFieldInBinaryFormat;
            this.usesBinaryDocValuesForFallbackFields = usesBinaryDocValuesForFallbackFields;
            this.usesBinaryDocValues = usesBinaryDocValues;
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
                null,
                false,
                false,
                false
            );
        }

        public boolean usesBinaryDocValues() {
            return usesBinaryDocValues;
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
            // if doc_values are enabled, fetch directly from them
            if (hasDocValues()) {
                if (usesBinaryDocValues) {
                    return binaryDocValuesFieldFetcher(name());
                } else {
                    var ifd = searchExecutionContext.getForField(this, MappedFieldType.FielddataOperation.SEARCH);
                    return docValuesFieldFetcher(ifd);
                }
            }

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
                    // otherwise, fetch the value from fallback fields
                    if (usesBinaryDocValuesForFallbackFields) {
                        return binaryDocValuesFieldFetcher(syntheticSourceFallbackFieldName());
                    }
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

                // bc we don't know whether the parent field will ignore a value, we must also check a potential fallback field created by
                // the parent field
                String fallbackFieldName = keywordParent.syntheticSourceFallbackFieldName();

                // The parent fallback field might be stored in binary doc values or in a stored field, we need to check which one
                var fallbackFetcher = keywordParent.usesBinaryDocValuesForIgnoredFields()
                    ? binaryDocValuesFieldFetcher(fallbackFieldName)
                    : storedFieldFetcher(fallbackFieldName);

                if (parent.isStored()) {
                    return combineFieldFetchers(storedFieldFetcher(parentFieldName), fallbackFetcher);
                } else if (parent.hasDocValues()) {
                    var ifd = searchExecutionContext.getForField(parent, MappedFieldType.FielddataOperation.SEARCH);
                    return combineFieldFetchers(docValuesFieldFetcher(ifd), fallbackFetcher);
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
                String delegateFieldName = keywordDelegate.name();
                // bc we don't know whether the delegate will ignore a value, we must also check the fallback field created by this
                // match_only_text field
                String fallbackName = syntheticSourceFallbackFieldName();

                // The fallback field may be stored in binary doc values or stored fields depending on index version
                var fallbackFetcher = usesBinaryDocValuesForFallbackFields
                    ? binaryDocValuesFieldFetcher(fallbackName)
                    : storedFieldFetcher(fallbackName);

                if (keywordDelegate.isStored()) {
                    return combineFieldFetchers(storedFieldFetcher(delegateFieldName), fallbackFetcher);
                } else if (keywordDelegate.hasDocValues()) {
                    var ifd = searchExecutionContext.getForField(keywordDelegate, MappedFieldType.FielddataOperation.SEARCH);
                    return combineFieldFetchers(docValuesFieldFetcher(ifd), fallbackFetcher);
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

        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> docValuesFieldFetcher(IndexFieldData<?> ifd) {
            return context -> {
                SortedBinaryDocValues indexedValuesDocValues = ifd.load(context).getBytesValues();
                return docId -> getValuesFromDocValues(indexedValuesDocValues, docId);
            };
        }

        private IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> binaryDocValuesFieldFetcher(String fieldName) {
            return context -> {
                SortedBinaryDocValues binaryDocValues = MultiValuedSortedBinaryDocValues.from(context.reader(), fieldName);
                return docId -> getValuesFromDocValues(binaryDocValues, docId);
            };
        }

        private List<Object> getValuesFromDocValues(SortedBinaryDocValues docValues, int docId) throws IOException {
            if (docValues.advanceExact(docId)) {
                var values = new ArrayList<>(docValues.docValueCount());
                for (int i = 0; i < docValues.docValueCount(); i++) {
                    values.add(docValues.nextValue().utf8ToString());
                }
                return values;
            } else {
                return List.of();
            }
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
        public boolean isSearchable() {
            return indexType().hasTerms() || hasDocValues();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            if (indexType().hasTerms()) {
                return new ConstantScoreQuery(super.termQuery(value, context));
            }

            failIfNotIndexedNorDocValuesFallback(context);

            if (usesBinaryDocValues) {
                return new SlowCustomBinaryDocValuesTermQuery(name(), indexedValueForSearch(value));
            } else {
                return SortedSetDocValuesField.newSlowExactQuery(name(), indexedValueForSearch(value));
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            if (indexType().hasTerms()) {
                return super.termsQuery(values, context);
            }

            failIfNotIndexedNorDocValuesFallback(context);

            List<BytesRef> bytesRefs = values.stream().map(this::indexedValueForSearch).toList();
            if (usesBinaryDocValues) {
                return new SlowCustomBinaryDocValuesTermInSetQuery(name(), bytesRefs);
            } else {
                return SortedSetDocValuesField.newSlowSetQuery(name(), bytesRefs);
            }
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
                Queries.ALL_DOCS_INSTANCE, // wildcard queries can be expensive, what should the approximation be?
                context
            );
        }

        @Override
        public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
            return toIntervalsSource(
                Intervals.regexp(pattern, IndexSearcher.getMaxClauseCount()),
                Queries.ALL_DOCS_INSTANCE, // regexp queries can be expensive, what should the approximation be?
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
                Queries.ALL_DOCS_INSTANCE, // range queries can be expensive, what should the approximation be?
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
            // Check if we can load from doc values
            if (hasDocValues()) {
                if (usesBinaryDocValues) {
                    return new BytesRefsFromBinaryMultiSeparateCountBlockLoader(name());
                } else {
                    return new BytesRefsFromOrdsBlockLoader(name());
                }
            }

            if (isSyntheticSourceEnabled()) {
                // if there is no delegate, load from a fallback field we created
                if (textFieldType.syntheticSourceDelegate().isEmpty()) {
                    if (usesBinaryDocValuesForFallbackFields) {
                        return new BytesRefsFromCustomBinaryBlockLoader(syntheticSourceFallbackFieldName());
                    } else {
                        // for bwc - load from a StoredField
                        if (storedFieldInBinaryFormat) {
                            return new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader(syntheticSourceFallbackFieldName());
                        } else {
                            return new BytesFromMixedStringsBytesRefBlockLoader(syntheticSourceFallbackFieldName());
                        }
                    }
                }

                // otherwise, delegate block loading to the synthetic source delegate if possible
                if (textFieldType.canUseSyntheticSourceDelegateForLoading()) {
                    return new DelegatingBlockLoader(textFieldType.syntheticSourceDelegate().get().blockLoader(blContext)) {
                        @Override
                        public String delegatingTo() {
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
                        return new DelegatingBlockLoader(kwd.blockLoader(blContext)) {

                            @Override
                            public String delegatingTo() {
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
        public boolean isAggregatable() {
            return hasDocValues();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (hasDocValues()) {
                return fieldDataFromDocValues();
            }

            if (fieldDataContext.fielddataOperation() != FielddataOperation.SCRIPT) {
                throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support sorting and aggregations");
            }

            if (isSyntheticSourceEnabled()) {
                if (usesBinaryDocValuesForFallbackFields) {
                    // For newer indexes, fallback data is stored in binary doc values
                    return (cache, breaker) -> new BytesBinaryIndexFieldData(
                        syntheticSourceFallbackFieldName(),
                        CoreValuesSourceType.KEYWORD,
                        TextDocValuesField::new
                    );
                }
                // For older indexes, fallback data is stored in stored fields
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

        private IndexFieldData.Builder fieldDataFromDocValues() {
            if (usesBinaryDocValues) {
                return new BytesBinaryIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, TextDocValuesField::new);
            } else {
                return new SortedSetOrdinalsIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    (dv, n) -> new TextDocValuesField(FieldData.toString(dv), n)
                );
            }
        }
    }

    private final IndexVersion indexCreatedVersion;
    private final IndexAnalyzers indexAnalyzers;
    private final NamedAnalyzer indexAnalyzer;
    private final int positionIncrementGap;
    private final FieldType fieldType;
    private final boolean storedFieldInBinaryFormat;
    private final boolean usesBinaryDocValuesForFallbackFields;
    private final FieldMapper.DocValuesParameter.Values docValuesParameters;

    private MatchOnlyTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        MatchOnlyTextFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);

        assert mappedFieldType.getTextSearchInfo().isTokenized();

        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.indexCreatedVersion = builder.indexCreatedVersion();
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
        this.storedFieldInBinaryFormat = builder.storedFieldInBinaryFormat;
        this.usesBinaryDocValuesForFallbackFields = builder.usesBinaryDocValuesForFallbackFields;
        this.docValuesParameters = builder.docValuesParameters.getValue();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            leafName(),
            indexCreatedVersion,
            indexAnalyzers,
            storedFieldInBinaryFormat,
            fieldType().isWithinMultiField(),
            usesBinaryDocValuesForFallbackFields
        ).init(this);
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

        // Add doc_values if enabled
        if (docValuesParameters.enabled()) {
            BytesRef binaryValue = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
            if (fieldType().usesBinaryDocValues()) {
                MultiValuedBinaryDocValuesField.SeparateCount.addToSeparateCountMultiBinaryFieldInDoc(
                    context.doc(),
                    fieldType().name(),
                    binaryValue
                );
            } else if (binaryValue.length > IndexWriter.MAX_TERM_LENGTH) {
                // if the binary value's length exceeds Lucene's max term length, then we cannot store it in SortedSetDocValuesField
                // in such cases, store the value in binary doc values instead, which don't have these length limitations
                storeValueInFallbackField(fieldType().syntheticSourceFallbackFieldName(), binaryValue, context);
            } else {
                context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
            }
        } else {
            // only add to field names when doc_values are disabled (doc_values track field existence implicitly)
            context.addToFieldNames(fieldType().name());
        }

        // match only text isn't stored, so if synthetic source needs to be supported, we must find an alternative way of loading the field
        if (fieldType().textFieldType.needsFallbackStorageForSyntheticSource(indexCreatedVersion)) {
            // check if we can use the delegate
            if (fieldType().canUseSyntheticSourceDelegateForSyntheticSource(value)) {
                return;
            }

            // check if we can use doc_values
            if (docValuesParameters.enabled()) {
                return;
            }

            // otherwise, store the field ourselves
            final String fallbackFieldName = fieldType().syntheticSourceFallbackFieldName();

            if (usesBinaryDocValuesForFallbackFields) {
                final var bytesRef = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
                storeValueInFallbackField(fallbackFieldName, bytesRef, context);
            } else {
                // otherwise for bwc, store the value in a stored fields like we used to
                if (storedFieldInBinaryFormat) {
                    final var bytesRef = new BytesRef(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
                    context.doc().add(new StoredField(fallbackFieldName, bytesRef));
                } else {
                    context.doc().add(new StoredField(fallbackFieldName, value.string()));
                }
            }
        }
    }

    private void storeValueInFallbackField(String fallbackFieldName, BytesRef bytesRef, DocumentParserContext context) {
        // store the value in a binary doc values field, create one if it doesn't exist
        MultiValuedBinaryDocValuesField field = (MultiValuedBinaryDocValuesField) context.doc().getByKey(fallbackFieldName);
        if (field == null) {
            field = new MultiValuedBinaryDocValuesField.IntegratedCount(fallbackFieldName, true);
            context.doc().addWithKey(fallbackFieldName, field);
        }
        field.add(bytesRef);
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
        if (docValuesParameters.enabled()) {
            return new SyntheticSourceSupport.Native(this::syntheticFieldLoaderFromDocValues);
        }
        return new SyntheticSourceSupport.Native(() -> syntheticFieldLoader(fullPath(), leafName()));
    }

    private CompositeSyntheticFieldLoader syntheticFieldLoaderFromDocValues() {
        var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>();
        if (fieldType().usesBinaryDocValues()) {
            layers.add(new BinaryDocValuesSyntheticFieldLoaderLayer(fullPath()));
        } else {
            layers.add(new SortedSetDocValuesSyntheticFieldLoaderLayer(fullPath()) {
                @Override
                public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
                    // match_only_text fields with doc_values may have all values stored in fallback fields if every value exceeds
                    // MAX_TERM_LENGTH. In that case, there will be no SortedSetDocValues, but the "main" field might still be indexed.
                    // As a result, we can't use SortedSetDocValuesSyntheticFieldLoaderLayer since it uses DocValues.getSortedSet(),
                    // which will throw.
                    if (reader.getSortedSetDocValues(fieldName()) == null) {
                        return null;
                    }
                    return super.docValuesLoader(reader, docIdsInLeaf);
                }

                @Override
                protected BytesRef convert(BytesRef value) {
                    return value;
                }

                @Override
                protected BytesRef preserve(BytesRef value) {
                    return BytesRef.deepCopyOf(value);
                }
            });

            // also load from fallback field for values that exceeded MAX_TERM_LENGTH
            layers.add(new BinaryDocValuesSyntheticFieldLoaderLayer(fieldType().syntheticSourceFallbackFieldName()));
        }
        return new CompositeSyntheticFieldLoader(leafName(), fullPath(), layers);
    }

    private SourceLoader.SyntheticFieldLoader syntheticFieldLoader(String fullFieldName, String leafFieldName) {
        var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>();

        // layer for loading from a fallback field created during indexing by this text field mapper
        final String fallbackFieldName = fieldType().syntheticSourceFallbackFieldName();
        if (usesBinaryDocValuesForFallbackFields) {
            layers.add(new BinaryDocValuesSyntheticFieldLoaderLayer(fallbackFieldName));
        } else {
            // for bwc - fallback fields were originally stored in StoredFields
            layers.add(new CompositeSyntheticFieldLoader.StoredFieldLayer(fallbackFieldName) {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    if (value instanceof BytesRef valueBytes) {
                        b.value(valueBytes.utf8ToString());
                    } else {
                        assert value instanceof String;
                        b.value(value.toString());
                    }
                }
            });
        }

        // because we don't know whether the delegate can be used for loading fields (ex. the delegate ignored some values or the delegate
        // doesn't even exist in the first place), we must check both the current field, as well as the delegate
        var kwd = TextFieldMapper.SyntheticSourceHelper.getKeywordFieldMapperForSyntheticSource(this);
        if (kwd != null) {
            layers.addAll(kwd.syntheticFieldLoaderLayers());
        }

        return new CompositeSyntheticFieldLoader(leafFieldName, fullFieldName, layers);
    }
}
