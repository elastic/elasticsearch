/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.StoredFieldSortedBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.TextDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/** A {@link FieldMapper} for full-text fields. */
public final class TextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "text";
    private static final String FAST_PHRASE_SUFFIX = "._index_phrase";
    private static final String FAST_PREFIX_SUFFIX = "._index_prefix";

    public static class Defaults {
        public static final double FIELDDATA_MIN_FREQUENCY = 0;
        public static final double FIELDDATA_MAX_FREQUENCY = Integer.MAX_VALUE;
        public static final int FIELDDATA_MIN_SEGMENT_SIZE = 0;
        public static final int INDEX_PREFIX_MIN_CHARS = 2;
        public static final int INDEX_PREFIX_MAX_CHARS = 5;

        public static final FieldType FIELD_TYPE;

        static {
            FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(false);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }

        /**
         * The default position_increment_gap is set to 100 so that phrase
         * queries of reasonably high slop will not match across field values.
         */
        public static final int POSITION_INCREMENT_GAP = 100;
    }

    private static final class PrefixConfig implements ToXContent {
        final int minChars;
        final int maxChars;

        private PrefixConfig(int minChars, int maxChars) {
            this.minChars = minChars;
            this.maxChars = maxChars;
            if (minChars > maxChars) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be less than max_chars [" + maxChars + "]");
            }
            if (minChars < 1) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be greater than zero");
            }
            if (maxChars >= 20) {
                throw new IllegalArgumentException("max_chars [" + maxChars + "] must be less than 20");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrefixConfig that = (PrefixConfig) o;
            return minChars == that.minChars && maxChars == that.maxChars;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minChars, maxChars);
        }

        @Override
        public String toString() {
            return "{ min_chars=" + minChars + ", max_chars=" + maxChars + " }";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("min_chars", minChars);
            builder.field("max_chars", maxChars);
            builder.endObject();
            return builder;
        }
    }

    private static PrefixConfig parsePrefixConfig(String propName, MappingParserContext parserContext, Object propNode) {
        if (propNode == null) {
            return null;
        }
        Map<?, ?> indexPrefix = (Map<?, ?>) propNode;
        int minChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("min_chars"), Defaults.INDEX_PREFIX_MIN_CHARS);
        int maxChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("max_chars"), Defaults.INDEX_PREFIX_MAX_CHARS);
        MappingParser.checkNoRemainingFields(propName, indexPrefix);
        return new PrefixConfig(minChars, maxChars);
    }

    private static final class FielddataFrequencyFilter implements ToXContent {
        final double minFreq;
        final double maxFreq;
        final int minSegmentSize;

        private FielddataFrequencyFilter(double minFreq, double maxFreq, int minSegmentSize) {
            this.minFreq = minFreq;
            this.maxFreq = maxFreq;
            this.minSegmentSize = minSegmentSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FielddataFrequencyFilter that = (FielddataFrequencyFilter) o;
            return Double.compare(that.minFreq, minFreq) == 0
                && Double.compare(that.maxFreq, maxFreq) == 0
                && minSegmentSize == that.minSegmentSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minFreq, maxFreq, minSegmentSize);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("min", minFreq);
            builder.field("max", maxFreq);
            builder.field("min_segment_size", minSegmentSize);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "{ min=" + minFreq + ", max=" + maxFreq + ", min_segment_size=" + minSegmentSize + " }";
        }
    }

    private static final FielddataFrequencyFilter DEFAULT_FILTER = new FielddataFrequencyFilter(
        Defaults.FIELDDATA_MIN_FREQUENCY,
        Defaults.FIELDDATA_MAX_FREQUENCY,
        Defaults.FIELDDATA_MIN_SEGMENT_SIZE
    );

    private static FielddataFrequencyFilter parseFrequencyFilter(String name, MappingParserContext parserContext, Object node) {
        Map<?, ?> frequencyFilter = (Map<?, ?>) node;
        double minFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("min"), 0);
        double maxFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("max"), Integer.MAX_VALUE);
        int minSegmentSize = XContentMapValues.nodeIntegerValue(frequencyFilter.remove("min_segment_size"), 0);
        MappingParser.checkNoRemainingFields(name, frequencyFilter);
        return new FielddataFrequencyFilter(minFrequency, maxFrequency, minSegmentSize);
    }

    public static class Builder extends FieldMapper.Builder {

        private final IndexVersion indexCreatedVersion;
        private final Parameter<Boolean> store;

        private final boolean isSyntheticSourceEnabled;

        private final Parameter<Boolean> index = Parameter.indexParam(m -> ((TextFieldMapper) m).index, true);

        final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> ((TextFieldMapper) m).similarity);

        final Parameter<String> indexOptions = TextParams.textIndexOptions(m -> ((TextFieldMapper) m).indexOptions);
        final Parameter<Boolean> norms = TextParams.norms(true, m -> ((TextFieldMapper) m).norms);
        final Parameter<String> termVectors = TextParams.termVectors(m -> ((TextFieldMapper) m).termVectors);

        final Parameter<Boolean> fieldData = Parameter.boolParam("fielddata", true, m -> ((TextFieldMapper) m).fieldData, false);
        final Parameter<FielddataFrequencyFilter> freqFilter = new Parameter<>(
            "fielddata_frequency_filter",
            true,
            () -> DEFAULT_FILTER,
            TextFieldMapper::parseFrequencyFilter,
            m -> ((TextFieldMapper) m).freqFilter,
            XContentBuilder::field,
            Objects::toString
        );
        final Parameter<Boolean> eagerGlobalOrdinals = Parameter.boolParam(
            "eager_global_ordinals",
            true,
            m -> ((TextFieldMapper) m).fieldType().eagerGlobalOrdinals,
            false
        );

        final Parameter<Boolean> indexPhrases = Parameter.boolParam(
            "index_phrases",
            false,
            m -> ((TextFieldMapper) m).fieldType().indexPhrases,
            false
        );
        final Parameter<PrefixConfig> indexPrefixes = new Parameter<>(
            "index_prefixes",
            false,
            () -> null,
            TextFieldMapper::parsePrefixConfig,
            m -> ((TextFieldMapper) m).indexPrefixes,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull();

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final TextParams.Analyzers analyzers;

        private final boolean withinMultiField;

        public Builder(String name, IndexAnalyzers indexAnalyzers, boolean isSyntheticSourceEnabled) {
            this(name, IndexVersion.current(), indexAnalyzers, isSyntheticSourceEnabled, false);
        }

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexAnalyzers indexAnalyzers,
            boolean isSyntheticSourceEnabled,
            boolean withinMultiField
        ) {
            super(name);

            // If synthetic source is used we need to either store this field
            // to recreate the source or use keyword multi-fields for that.
            // So if there are no suitable multi-fields we will default to
            // storing the field without requiring users to explicitly set 'store'.
            //
            // If 'store' parameter was explicitly provided we'll reject the request.
            // Note that if current builder is a multi field, then we don't need to store, given that responsibility lies with parent field
            this.withinMultiField = withinMultiField;
            this.store = Parameter.storeParam(m -> ((TextFieldMapper) m).store, () -> {
                if (indexCreatedVersion.onOrAfter(IndexVersions.MAPPER_TEXT_MATCH_ONLY_MULTI_FIELDS_DEFAULT_NOT_STORED)) {
                    return isSyntheticSourceEnabled
                        && this.withinMultiField == false
                        && multiFieldsBuilder.hasSyntheticSourceCompatibleKeywordField() == false;
                } else {
                    return isSyntheticSourceEnabled && multiFieldsBuilder.hasSyntheticSourceCompatibleKeywordField() == false;
                }
            });
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((TextFieldMapper) m).indexAnalyzer,
                m -> (((TextFieldMapper) m).positionIncrementGap),
                indexCreatedVersion
            );
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
        }

        public Builder index(boolean index) {
            this.index.setValue(index);
            return this;
        }

        public Builder store(boolean store) {
            this.store.setValue(store);
            return this;
        }

        public Builder fielddata(boolean fielddata) {
            this.fieldData.setValue(fielddata);
            return this;
        }

        public Builder fielddataFrequencyFilter(double min, double max, int segs) {
            this.freqFilter.setValue(new FielddataFrequencyFilter(min, max, segs));
            return this;
        }

        public Builder addMultiField(FieldMapper.Builder builder) {
            this.multiFieldsBuilder.add(builder);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                index,
                store,
                indexOptions,
                norms,
                termVectors,
                analyzers.indexAnalyzer,
                analyzers.searchAnalyzer,
                analyzers.searchQuoteAnalyzer,
                similarity,
                analyzers.positionIncrementGap,
                fieldData,
                freqFilter,
                eagerGlobalOrdinals,
                indexPhrases,
                indexPrefixes,
                meta };
        }

        private TextFieldType buildFieldType(
            FieldType fieldType,
            MultiFields multiFields,
            MapperBuilderContext context,
            IndexVersion indexCreatedVersion
        ) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            if (analyzers.positionIncrementGap.isConfigured()) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException(
                        "Cannot set position_increment_gap on field [" + leafName() + "] without positions enabled"
                    );
                }
            }
            TextSearchInfo tsi = new TextSearchInfo(fieldType, similarity.getValue(), searchAnalyzer, searchQuoteAnalyzer);
            TextFieldType ft;
            if (indexCreatedVersion.isLegacyIndexVersion()) {
                ft = new LegacyTextFieldType(context.buildFullName(leafName()), index.getValue(), store.getValue(), tsi, meta.getValue());
                // ignore fieldData and eagerGlobalOrdinals
            } else {
                ft = new TextFieldType(
                    context.buildFullName(leafName()),
                    index.getValue(),
                    store.getValue(),
                    tsi,
                    context.isSourceSynthetic(),
                    SyntheticSourceHelper.syntheticSourceDelegate(fieldType, multiFields),
                    meta.getValue(),
                    eagerGlobalOrdinals.getValue(),
                    indexPhrases.getValue()
                );
                if (fieldData.getValue()) {
                    ft.setFielddata(true, freqFilter.getValue());
                }
            }
            return ft;
        }

        private SubFieldInfo buildPrefixInfo(MapperBuilderContext context, FieldType fieldType, TextFieldType tft) {
            if (indexPrefixes.get() == null) {
                return null;
            }
            if (index.getValue() == false) {
                throw new IllegalArgumentException("Cannot set index_prefixes on unindexed field [" + leafName() + "]");
            }
            /*
             * Mappings before v7.2.1 use {@link Builder#name} instead of {@link Builder#fullName}
             * to build prefix field names so we preserve the name that was used at creation time
             * even if it is different from the expected one (in case the field is nested under an object
             * or a multi-field). This way search will continue to work on old indices and new indices
             * will use the expected full name.
             */
            String fullName = indexCreatedVersion.before(IndexVersions.V_7_2_1) ? leafName() : context.buildFullName(leafName());
            // Copy the index options of the main field to allow phrase queries on
            // the prefix field.
            FieldType pft = new FieldType(fieldType);
            pft.setOmitNorms(true);
            if (fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS) {
                // frequencies are not needed because prefix queries always use a constant score
                pft.setIndexOptions(IndexOptions.DOCS);
            } else {
                pft.setIndexOptions(fieldType.indexOptions());
            }
            if (fieldType.storeTermVectorOffsets()) {
                pft.setStoreTermVectorOffsets(true);
            }
            tft.setIndexPrefixes(indexPrefixes.get().minChars, indexPrefixes.get().maxChars);
            return new SubFieldInfo(
                fullName + "._index_prefix",
                pft,
                new PrefixWrappedAnalyzer(
                    analyzers.getIndexAnalyzer().analyzer(),
                    analyzers.positionIncrementGap.get(),
                    indexPrefixes.get().minChars,
                    indexPrefixes.get().maxChars
                )
            );
        }

        private SubFieldInfo buildPhraseInfo(FieldType fieldType, TextFieldType parent) {
            if (indexPhrases.get() == false) {
                return null;
            }
            if (index.get() == false) {
                throw new IllegalArgumentException("Cannot set index_phrases on unindexed field [" + leafName() + "]");
            }
            if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot set index_phrases on field [" + leafName() + "] if positions are not enabled");
            }
            FieldType phraseFieldType = new FieldType(fieldType);
            PhraseWrappedAnalyzer a = new PhraseWrappedAnalyzer(
                analyzers.getIndexAnalyzer().analyzer(),
                analyzers.positionIncrementGap.get()
            );
            return new SubFieldInfo(parent.name() + FAST_PHRASE_SUFFIX, phraseFieldType, a);
        }

        @Override
        public TextFieldMapper build(MapperBuilderContext context) {
            FieldType fieldType = TextParams.buildFieldType(
                index,
                store,
                indexOptions,
                // legacy indices do not have access to norms
                indexCreatedVersion.isLegacyIndexVersion() ? () -> false : norms,
                termVectors
            );
            BuilderParams builderParams = builderParams(this, context);
            TextFieldType tft = buildFieldType(fieldType, builderParams.multiFields(), context, indexCreatedVersion);
            SubFieldInfo phraseFieldInfo = buildPhraseInfo(fieldType, tft);
            SubFieldInfo prefixFieldInfo = buildPrefixInfo(context, fieldType, tft);
            for (Mapper mapper : builderParams.multiFields()) {
                if (mapper.fullPath().endsWith(FAST_PHRASE_SUFFIX) || mapper.fullPath().endsWith(FAST_PREFIX_SUFFIX)) {
                    throw new MapperParsingException("Cannot use reserved field name [" + mapper.fullPath() + "]");
                }
            }
            return new TextFieldMapper(leafName(), fieldType, tft, prefixFieldInfo, phraseFieldInfo, builderParams, this);
        }
    }

    public static final TypeParser PARSER = createTypeParserWithLegacySupport(
        (n, c) -> new Builder(
            n,
            c.indexVersionCreated(),
            c.getIndexAnalyzers(),
            SourceFieldMapper.isSynthetic(c.getIndexSettings()),
            c.isWithinMultiField()
        )
    );

    private static class PhraseWrappedAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final int posIncGap;

        PhraseWrappedAnalyzer(Analyzer delegate, int posIncGap) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.posIncGap = posIncGap;
        }

        @Override
        public int getPositionIncrementGap(String fieldName) {
            return posIncGap;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            return new TokenStreamComponents(components.getSource(), new FixedShingleFilter(components.getTokenStream(), 2));
        }
    }

    private static class PrefixWrappedAnalyzer extends AnalyzerWrapper {

        private final int minChars;
        private final int maxChars;
        private final int posIncGap;
        private final Analyzer delegate;

        PrefixWrappedAnalyzer(Analyzer delegate, int posIncGap, int minChars, int maxChars) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.posIncGap = posIncGap;
            this.minChars = minChars;
            this.maxChars = maxChars;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        public int getPositionIncrementGap(String fieldName) {
            return posIncGap;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            TokenFilter filter = new EdgeNGramTokenFilter(components.getTokenStream(), minChars, maxChars, false);
            return new TokenStreamComponents(components.getSource(), filter);
        }
    }

    private static final class PrefixFieldType extends StringFieldType {

        final int minChars;
        final int maxChars;
        final TextFieldType parentField;

        PrefixFieldType(TextFieldType parentField, int minChars, int maxChars) {
            super(parentField.name() + FAST_PREFIX_SUFFIX, true, false, false, parentField.getTextSearchInfo(), Collections.emptyMap());
            this.minChars = minChars;
            this.maxChars = maxChars;
            this.parentField = parentField;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return false;
        }

        boolean accept(int length) {
            return length >= minChars - 1 && length <= maxChars;
        }

        @Override
        public Query prefixQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            if (value.length() >= minChars) {
                if (caseInsensitive) {
                    return super.termQueryCaseInsensitive(value, context);
                }
                return super.termQuery(value, context);
            }
            List<Automaton> automata = new ArrayList<>();
            if (caseInsensitive) {
                automata.add(AutomatonQueries.toCaseInsensitiveString(value));
            } else {
                automata.add(Automata.makeString(value));
            }

            for (int i = value.length(); i < minChars; i++) {
                automata.add(Automata.makeAnyChar());
            }
            Automaton automaton = Operations.concatenate(automata);
            AutomatonQuery query = method == null
                ? new AutomatonQuery(new Term(name(), value + "*"), automaton, false)
                : new AutomatonQuery(new Term(name(), value + "*"), automaton, false, method);
            return new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(parentField.name(), value)), BooleanClause.Occur.SHOULD)
                .build();
        }

        public IntervalsSource intervals(BytesRef term) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over a field [" + name() + "] without indexed positions");
            }
            if (term.length > maxChars) {
                return Intervals.prefix(term);
            }
            if (term.length >= minChars) {
                return Intervals.fixField(name(), Intervals.term(term));
            }
            String wildcardTerm = term.utf8ToString() + "?".repeat(Math.max(0, minChars - term.length));
            return Intervals.or(
                Intervals.fixField(name(), Intervals.wildcard(new BytesRef(wildcardTerm), IndexSearcher.getMaxClauseCount())),
                Intervals.term(term)
            );
        }

        @Override
        public String typeName() {
            return "prefix";
        }

        @Override
        public String toString() {
            return super.toString() + ",prefixChars=" + minChars + ":" + maxChars;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class SubFieldInfo {

        private final Analyzer analyzer;
        private final FieldType fieldType;
        private final String field;

        SubFieldInfo(String field, FieldType fieldType, Analyzer analyzer) {
            this.fieldType = Mapper.freezeAndDeduplicateFieldType(fieldType);
            this.analyzer = analyzer;
            this.field = field;
        }

    }

    public static class TextFieldType extends StringFieldType {

        private boolean fielddata;
        private FielddataFrequencyFilter filter;
        private PrefixFieldType prefixFieldType;
        private final boolean indexPhrases;
        private final boolean eagerGlobalOrdinals;
        private final boolean isSyntheticSource;
        /**
         * In some configurations text fields use a sub-keyword field to provide
         * their values for synthetic source. This is that field. Or null if we're
         * not running in synthetic _source or synthetic source doesn't need it.
         */
        private final KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate;

        public TextFieldType(
            String name,
            boolean indexed,
            boolean stored,
            TextSearchInfo tsi,
            boolean isSyntheticSource,
            KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate,
            Map<String, String> meta,
            boolean eagerGlobalOrdinals,
            boolean indexPhrases
        ) {
            super(name, indexed, stored, false, tsi, meta);
            fielddata = false;
            this.isSyntheticSource = isSyntheticSource;
            // TODO block loader could use a "fast loading" delegate which isn't always the same - but frequently is.
            this.syntheticSourceDelegate = syntheticSourceDelegate;
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            this.indexPhrases = indexPhrases;
        }

        public TextFieldType(String name, boolean indexed, boolean stored, Map<String, String> meta) {
            super(
                name,
                indexed,
                stored,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                meta
            );
            fielddata = false;
            isSyntheticSource = false;
            syntheticSourceDelegate = null;
            eagerGlobalOrdinals = false;
            indexPhrases = false;
        }

        public TextFieldType(String name, boolean isSyntheticSource) {
            this(
                name,
                true,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                isSyntheticSource,
                null,
                Collections.emptyMap(),
                false,
                false
            );
        }

        public boolean fielddata() {
            return fielddata;
        }

        @Override
        public boolean eagerGlobalOrdinals() {
            return eagerGlobalOrdinals;
        }

        public void setFielddata(boolean fielddata, FielddataFrequencyFilter filter) {
            this.fielddata = fielddata;
            this.filter = filter;
        }

        public void setFielddata(boolean fielddata) {
            this.setFielddata(fielddata, DEFAULT_FILTER);
        }

        double fielddataMinFrequency() {
            return filter.minFreq;
        }

        double fielddataMaxFrequency() {
            return filter.maxFreq;
        }

        int fielddataMinSegmentSize() {
            return filter.minSegmentSize;
        }

        void setIndexPrefixes(int minChars, int maxChars) {
            this.prefixFieldType = new PrefixFieldType(this, minChars, maxChars);
        }

        public PrefixFieldType getPrefixFieldType() {
            return this.prefixFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public Query prefixQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            if (prefixFieldType == null || prefixFieldType.accept(value.length()) == false) {
                return super.prefixQuery(value, method, caseInsensitive, context);
            }
            Query tq = prefixFieldType.prefixQuery(value, method, caseInsensitive, context);
            if (method == null
                || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE
                || method == MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE) {
                return new ConstantScoreQuery(tq);
            }
            return tq;
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
            failIfNotIndexed();
            if (prefixFieldType != null
                && value.length() >= prefixFieldType.minChars
                && value.length() <= prefixFieldType.maxChars
                && prefixFieldType.getTextSearchInfo().hasPositions()) {

                return new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixFieldType.name(), indexedValueForSearch(value))), name());
            } else {
                SpanMultiTermQueryWrapper<?> spanMulti = new SpanMultiTermQueryWrapper<>(
                    new PrefixQuery(new Term(name(), indexedValueForSearch(value)))
                );
                if (method != null) {
                    spanMulti.setRewriteMethod(method);
                }
                return spanMulti;
            }
        }

        @Override
        public IntervalsSource termIntervals(BytesRef term, SearchExecutionContext context) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            return Intervals.term(term);
        }

        @Override
        public IntervalsSource prefixIntervals(BytesRef term, SearchExecutionContext context) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            if (prefixFieldType != null) {
                return prefixFieldType.intervals(term);
            }
            return Intervals.prefix(term, IndexSearcher.getMaxClauseCount());
        }

        @Override
        public IntervalsSource fuzzyIntervals(
            String term,
            int maxDistance,
            int prefixLength,
            boolean transpositions,
            SearchExecutionContext context
        ) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            FuzzyQuery fq = new FuzzyQuery(
                new Term(name(), term),
                maxDistance,
                prefixLength,
                IndexSearcher.getMaxClauseCount(),
                transpositions
            );
            return Intervals.multiterm(fq.getAutomata(), IndexSearcher.getMaxClauseCount(), term);
        }

        @Override
        public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            return Intervals.wildcard(pattern, IndexSearcher.getMaxClauseCount());
        }

        @Override
        public IntervalsSource regexpIntervals(BytesRef pattern, SearchExecutionContext context) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            return Intervals.regexp(pattern, IndexSearcher.getMaxClauseCount());
        }

        @Override
        public IntervalsSource rangeIntervals(
            BytesRef lowerTerm,
            BytesRef upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            return Intervals.range(lowerTerm, upperTerm, includeLower, includeUpper, IndexSearcher.getMaxClauseCount());
        }

        private void checkForPositions(boolean multi) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException(
                    "field:[" + name() + "] was indexed without position data; cannot run " + (multi ? "MultiPhraseQuery" : "PhraseQuery")
                );
            }
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, SearchExecutionContext context)
            throws IOException {
            String field = name();
            checkForPositions(false);
            // we can't use the index_phrases shortcut with slop, if there are gaps in the stream,
            // or if the incoming token stream is the output of a token graph due to
            // https://issues.apache.org/jira/browse/LUCENE-8916
            if (indexPhrases && slop == 0 && hasGaps(stream) == false && stream.hasAttribute(BytesTermAttribute.class) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            builder.setSlop(slop);

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
            int position = -1;

            stream.reset();
            while (stream.incrementToken()) {
                if (termAtt.getBytesRef() == null) {
                    throw new IllegalStateException("Null term while building phrase query");
                }
                if (enablePosIncrements) {
                    position += posIncrAtt.getPositionIncrement();
                } else {
                    position += 1;
                }
                builder.add(new Term(field, termAtt.getBytesRef()), position);
            }

            return builder.build();
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
            throws IOException {
            String field = name();
            checkForPositions(true);
            if (indexPhrases && slop == 0 && hasGaps(stream) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            return createPhraseQuery(stream, field, slop, enablePositionIncrements);
        }

        private static int countTokens(TokenStream ts) throws IOException {
            ts.reset();
            int count = 0;
            while (ts.incrementToken()) {
                count++;
            }
            ts.end();
            return count;
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
            if (countTokens(stream) > 1) {
                checkForPositions(false);
            }
            return analyzePhrasePrefix(stream, slop, maxExpansions);
        }

        private Query analyzePhrasePrefix(TokenStream stream, int slop, int maxExpansions) throws IOException {
            String prefixField = prefixFieldType == null || slop > 0 ? null : prefixFieldType.name();
            IntPredicate usePrefix = (len) -> len >= prefixFieldType.minChars && len <= prefixFieldType.maxChars;
            return createPhrasePrefixQuery(stream, name(), slop, maxExpansions, prefixField, usePrefix);
        }

        public static boolean hasGaps(TokenStream stream) throws IOException {
            assert stream instanceof CachingTokenFilter;
            PositionIncrementAttribute posIncAtt = stream.getAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                if (posIncAtt.getPositionIncrement() > 1) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isAggregatable() {
            return fielddata;
        }

        /**
         * Returns true if the delegate sub-field can be used for loading.
         * A delegate by definition must have doc_values or be stored so most of the time it can be used for loading.
         */
        public boolean canUseSyntheticSourceDelegateForLoading() {
            return syntheticSourceDelegate != null && syntheticSourceDelegate.ignoreAbove() == Integer.MAX_VALUE;
        }

        /**
         * Returns true if the delegate sub-field can be used for querying only (ie. isIndexed must be true)
         */
        public boolean canUseSyntheticSourceDelegateForQuerying() {
            return syntheticSourceDelegate != null
                && syntheticSourceDelegate.ignoreAbove() == Integer.MAX_VALUE
                && syntheticSourceDelegate.isIndexed();
        }

        /**
         * Returns true if the delegate sub-field can be used for querying only (ie. isIndexed must be true)
         */
        public boolean canUseSyntheticSourceDelegateForQueryingEquality(String str) {
            if (syntheticSourceDelegate == null
                // Can't push equality to an index if there isn't an index
                || syntheticSourceDelegate.isIndexed() == false
                // ESQL needs docs values to push equality
                || syntheticSourceDelegate.hasDocValues() == false) {
                return false;
            }
            // Can't push equality if the field we're checking for is so big we'd ignore it.
            return str.length() <= syntheticSourceDelegate.ignoreAbove();
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (canUseSyntheticSourceDelegateForLoading()) {
                return new BlockLoader.Delegating(syntheticSourceDelegate.blockLoader(blContext)) {
                    @Override
                    protected String delegatingTo() {
                        return syntheticSourceDelegate.name();
                    }
                };
            }
            /*
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
            if (isStored()) {
                return new BlockStoredFieldsReader.BytesFromStringsBlockLoader(name());
            }

            // _ignored_source field will contain entries for this field if it is not stored
            // and there is no syntheticSourceDelegate.
            // See #syntheticSourceSupport().
            // But if a text field is a multi field it won't have an entry in _ignored_source.
            // The parent might, but we don't have enough context here to figure this out.
            // So we bail.
            if (isSyntheticSource && syntheticSourceDelegate == null && parentField == null) {
                return fallbackSyntheticSourceBlockLoader();
            }

            SourceValueFetcher fetcher = SourceValueFetcher.toString(blContext.sourcePaths(name()));
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, blockReaderDisiLookup(blContext));
        }

        FallbackSyntheticSourceBlockLoader fallbackSyntheticSourceBlockLoader() {
            var reader = new FallbackSyntheticSourceBlockLoader.SingleValueReader<BytesRef>(null) {
                @Override
                public void convertValue(Object value, List<BytesRef> accumulator) {
                    if (value != null) {
                        accumulator.add(new BytesRef(value.toString()));
                    }
                }

                @Override
                protected void parseNonNullValue(XContentParser parser, List<BytesRef> accumulator) throws IOException {
                    var text = parser.textOrNull();

                    if (text != null) {
                        accumulator.add(new BytesRef(text));
                    }
                }

                @Override
                public void writeToBlock(List<BytesRef> values, BlockLoader.Builder blockBuilder) {
                    var bytesRefBuilder = (BlockLoader.BytesRefBuilder) blockBuilder;

                    for (var value : values) {
                        bytesRefBuilder.appendBytesRef(value);
                    }
                }
            };

            return new FallbackSyntheticSourceBlockLoader(reader, name()) {
                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.bytesRefs(expectedCount);
                }
            };
        }

        /**
         * Build an iterator of documents that have the field. This mirrors parseCreateField,
         * using whatever
         */
        private BlockSourceReader.LeafIteratorLookup blockReaderDisiLookup(BlockLoaderContext blContext) {
            if (isSyntheticSource && syntheticSourceDelegate != null) {
                // Since we are using synthetic source and a delegate, we can't use this field
                // to determine if the delegate has values in the document (f.e. handling of `null` is different
                // between text and keyword).
                return BlockSourceReader.lookupMatchingAll();
            }

            if (isIndexed()) {
                if (getTextSearchInfo().hasNorms()) {
                    return BlockSourceReader.lookupFromNorms(name());
                }
            } else if (isStored() == false) {
                return BlockSourceReader.lookupMatchingAll();
            }
            return BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            FielddataOperation operation = fieldDataContext.fielddataOperation();
            if (operation == FielddataOperation.SEARCH) {
                if (fielddata == false) {
                    throw new IllegalArgumentException(
                        "Fielddata is disabled on ["
                            + name()
                            + "] in ["
                            + fieldDataContext.fullyQualifiedIndexName()
                            + "]. Text fields are not optimised for operations that require per-document "
                            + "field data like aggregations and sorting, so these operations are disabled by default. Please use a "
                            + "keyword field instead. Alternatively, set fielddata=true on ["
                            + name()
                            + "] in order to load "
                            + "field data by uninverting the inverted index. Note that this can use significant memory."
                    );
                }
                return new PagedBytesIndexFieldData.Builder(
                    name(),
                    filter.minFreq,
                    filter.maxFreq,
                    filter.minSegmentSize,
                    CoreValuesSourceType.KEYWORD,
                    (dv, n) -> new DelegateDocValuesField(
                        new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                        n
                    )
                );
            }

            if (operation != FielddataOperation.SCRIPT) {
                throw new IllegalStateException("unknown field data operation [" + operation.name() + "]");
            }
            if (isSyntheticSource) {
                if (isStored()) {
                    return (cache, breaker) -> new StoredFieldSortedBinaryIndexFieldData(
                        name(),
                        CoreValuesSourceType.KEYWORD,
                        TextDocValuesField::new
                    ) {
                        @Override
                        protected BytesRef storedToBytesRef(Object stored) {
                            return new BytesRef((String) stored);
                        }
                    };
                }
                if (syntheticSourceDelegate != null) {
                    return syntheticSourceDelegate.fielddataBuilder(fieldDataContext);
                }
                /*
                 * We *shouldn't fall to this exception. The mapping should be
                 * rejected because we've enabled synthetic source but not configured
                 * the index properly. But we give it a nice message anyway just in
                 * case.
                 */
                throw new IllegalArgumentException(
                    "fetching values from a text field ["
                        + name()
                        + "] is not supported because synthetic _source is enabled and we don't have a way to load the fields"
                );
            }
            return new SourceValueFetcherSortedBinaryIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                SourceValueFetcher.toString(fieldDataContext.sourcePathsLookup().apply(name())),
                fieldDataContext.lookupSupplier().get(),
                TextDocValuesField::new
            );
        }

        public boolean isSyntheticSource() {
            return isSyntheticSource;
        }

        public KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate() {
            return syntheticSourceDelegate;
        }
    }

    public static class ConstantScoreTextFieldType extends TextFieldType {

        public ConstantScoreTextFieldType(String name, boolean indexed, boolean stored, TextSearchInfo tsi, Map<String, String> meta) {
            super(name, indexed, stored, tsi, false, null, meta, false, false);
        }

        public ConstantScoreTextFieldType(String name) {
            this(
                name,
                true,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                Collections.emptyMap()
            );
        }

        public ConstantScoreTextFieldType(String name, boolean indexed, boolean stored, Map<String, String> meta) {
            this(
                name,
                indexed,
                stored,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                meta
            );
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
            @Nullable MultiTermQuery.RewriteMethod rewriteMethod
        ) {
            // Disable scoring
            return new ConstantScoreQuery(
                super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context, rewriteMethod)
            );
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, SearchExecutionContext queryShardContext)
            throws IOException {
            // Disable scoring
            return new ConstantScoreQuery(super.phraseQuery(stream, slop, enablePosIncrements, queryShardContext));
        }

        @Override
        public Query multiPhraseQuery(
            TokenStream stream,
            int slop,
            boolean enablePositionIncrements,
            SearchExecutionContext queryShardContext
        ) throws IOException {
            // Disable scoring
            return new ConstantScoreQuery(super.multiPhraseQuery(stream, slop, enablePositionIncrements, queryShardContext));
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext queryShardContext)
            throws IOException {
            // Disable scoring
            return new ConstantScoreQuery(super.phrasePrefixQuery(stream, slop, maxExpansions, queryShardContext));
        }

    }

    static class LegacyTextFieldType extends ConstantScoreTextFieldType {

        private final MappedFieldType existQueryFieldType;

        LegacyTextFieldType(String name, boolean indexed, boolean stored, TextSearchInfo tsi, Map<String, String> meta) {
            super(name, indexed, stored, tsi, meta);
            // norms are not available, neither are doc-values, so fall back to _source to run exists query
            existQueryFieldType = KeywordScriptFieldType.sourceOnly(name()).asMappedFieldTypes().findFirst().get();
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
            throw new IllegalArgumentException("Cannot use span prefix queries on text field " + name() + " of a legacy index");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new ElasticsearchException(
                    "runtime-computed exists query cannot be executed while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
                );
            }
            return existQueryFieldType.existsQuery(context);
        }

    }

    private final IndexVersion indexCreatedVersion;
    private final boolean index;
    private final boolean store;
    private final String indexOptions;
    private final boolean norms;
    private final String termVectors;
    private final SimilarityProvider similarity;
    private final NamedAnalyzer indexAnalyzer;
    private final IndexAnalyzers indexAnalyzers;
    private final int positionIncrementGap;
    private final PrefixConfig indexPrefixes;
    private final FielddataFrequencyFilter freqFilter;
    private final boolean fieldData;
    private final FieldType fieldType;
    private final SubFieldInfo prefixFieldInfo;
    private final SubFieldInfo phraseFieldInfo;

    private final boolean isSyntheticSourceEnabled;
    private final boolean isWithinMultiField;

    private TextFieldMapper(
        String simpleName,
        FieldType fieldType,
        TextFieldType mappedFieldType,
        SubFieldInfo prefixFieldInfo,
        SubFieldInfo phraseFieldInfo,
        BuilderParams builderParams,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        if (fieldType.indexOptions() == IndexOptions.NONE && fieldType().fielddata()) {
            throw new IllegalArgumentException("Cannot enable fielddata on a [text] field that is not indexed: [" + fullPath() + "]");
        }
        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.prefixFieldInfo = prefixFieldInfo;
        this.phraseFieldInfo = phraseFieldInfo;
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzer = builder.analyzers.getIndexAnalyzer();
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.positionIncrementGap = builder.analyzers.positionIncrementGap.getValue();
        this.index = builder.index.getValue();
        this.store = builder.store.getValue();
        this.similarity = builder.similarity.getValue();
        this.indexOptions = builder.indexOptions.getValue();
        this.norms = builder.norms.getValue();
        this.termVectors = builder.termVectors.getValue();
        this.indexPrefixes = builder.indexPrefixes.getValue();
        this.freqFilter = builder.freqFilter.getValue();
        this.fieldData = builder.fieldData.get();
        this.isSyntheticSourceEnabled = builder.isSyntheticSourceEnabled;
        this.isWithinMultiField = builder.withinMultiField;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        Map<String, NamedAnalyzer> analyzersMap = new HashMap<>();
        analyzersMap.put(fullPath(), indexAnalyzer);
        if (phraseFieldInfo != null) {
            analyzersMap.put(
                phraseFieldInfo.field,
                new NamedAnalyzer(indexAnalyzer.name() + "_phrase", AnalyzerScope.INDEX, phraseFieldInfo.analyzer)
            );
        }
        if (prefixFieldInfo != null) {
            analyzersMap.put(
                prefixFieldInfo.field,
                new NamedAnalyzer(indexAnalyzer.name() + "_prefix", AnalyzerScope.INDEX, prefixFieldInfo.analyzer)
            );
        }
        return analyzersMap;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexAnalyzers, isSyntheticSourceEnabled, isWithinMultiField).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();

        if (value == null) {
            return;
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), value, fieldType);
            context.doc().add(field);
            if (fieldType.omitNorms()) {
                context.addToFieldNames(fieldType().name());
            }
            if (prefixFieldInfo != null) {
                context.doc().add(new Field(prefixFieldInfo.field, value, prefixFieldInfo.fieldType));
            }
            if (phraseFieldInfo != null) {
                context.doc().add(new Field(phraseFieldInfo.field, value, phraseFieldInfo.fieldType));
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public TextFieldType fieldType() {
        return (TextFieldType) super.fieldType();
    }

    public static Query createPhraseQuery(TokenStream stream, String field, int slop, boolean enablePositionIncrements) throws IOException {
        MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
        mpqb.setSlop(slop);

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
        int position = -1;

        List<Term> multiTerms = new ArrayList<>();
        stream.reset();
        while (stream.incrementToken()) {
            int positionIncrement = posIncrAtt.getPositionIncrement();

            if (positionIncrement > 0 && multiTerms.size() > 0) {
                if (enablePositionIncrements) {
                    mpqb.add(multiTerms.toArray(new Term[0]), position);
                } else {
                    mpqb.add(multiTerms.toArray(new Term[0]));
                }
                multiTerms.clear();
            }
            position += positionIncrement;
            multiTerms.add(new Term(field, termAtt.getBytesRef()));
        }

        if (enablePositionIncrements) {
            mpqb.add(multiTerms.toArray(new Term[0]), position);
        } else {
            mpqb.add(multiTerms.toArray(new Term[0]));
        }
        return mpqb.build();
    }

    public static Query createPhrasePrefixQuery(
        TokenStream stream,
        String field,
        int slop,
        int maxExpansions,
        String prefixField,
        IntPredicate usePrefixField
    ) throws IOException {
        MultiPhrasePrefixQuery builder = new MultiPhrasePrefixQuery(field);
        builder.setSlop(slop);
        builder.setMaxExpansions(maxExpansions);

        List<Term> currentTerms = new ArrayList<>();

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);

        stream.reset();
        int position = -1;
        while (stream.incrementToken()) {
            if (posIncrAtt.getPositionIncrement() != 0) {
                if (currentTerms.isEmpty() == false) {
                    builder.add(currentTerms.toArray(new Term[0]), position);
                }
                position += posIncrAtt.getPositionIncrement();
                currentTerms.clear();
            }
            currentTerms.add(new Term(field, termAtt.getBytesRef()));
        }
        builder.add(currentTerms.toArray(new Term[0]), position);
        if (prefixField == null) {
            return builder;
        }

        int lastPos = builder.getTerms().length - 1;
        final Term[][] terms = builder.getTerms();
        final int[] positions = builder.getPositions();
        for (Term term : terms[lastPos]) {
            String value = term.text();
            if (usePrefixField.test(value.length()) == false) {
                return builder;
            }
        }

        if (terms.length == 1) {
            SynonymQuery.Builder sb = new SynonymQuery.Builder(prefixField);
            Arrays.stream(terms[0]).map(term -> new Term(prefixField, term.bytes())).forEach(sb::addTerm);
            return sb.build();
        }

        SpanNearQuery.Builder spanQuery = new SpanNearQuery.Builder(field, true);
        spanQuery.setSlop(slop);
        int previousPos = -1;
        for (int i = 0; i < terms.length; i++) {
            Term[] posTerms = terms[i];
            int posInc = positions[i] - previousPos;
            previousPos = positions[i];
            if (posInc > 1) {
                spanQuery.addGap(posInc - 1);
            }
            if (i == lastPos) {
                if (posTerms.length == 1) {
                    FieldMaskingSpanQuery fieldMask = new FieldMaskingSpanQuery(
                        new SpanTermQuery(new Term(prefixField, posTerms[0].bytes())),
                        field
                    );
                    spanQuery.addClause(fieldMask);
                } else {
                    SpanQuery[] queries = Arrays.stream(posTerms)
                        .map(term -> new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixField, term.bytes())), field))
                        .toArray(SpanQuery[]::new);
                    spanQuery.addClause(new SpanOrQuery(queries));
                }
            } else {
                if (posTerms.length == 1) {
                    spanQuery.addClause(new SpanTermQuery(posTerms[0]));
                } else {
                    SpanTermQuery[] queries = Arrays.stream(posTerms).map(SpanTermQuery::new).toArray(SpanTermQuery[]::new);
                    spanQuery.addClause(new SpanOrQuery(queries));
                }
            }
        }
        return spanQuery.build();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // this is a pain, but we have to do this to maintain BWC
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        builder.field("type", contentType());
        final Builder b = (Builder) getMergeBuilder();
        b.index.toXContent(builder, includeDefaults);
        b.store.toXContent(builder, includeDefaults);
        multiFields().toXContent(builder, params);
        copyTo().toXContent(builder);
        if (sourceKeepMode().isPresent()) {
            sourceKeepMode().get().toXContent(builder);
        }
        b.meta.toXContent(builder, includeDefaults);
        b.indexOptions.toXContent(builder, includeDefaults);
        b.termVectors.toXContent(builder, includeDefaults);
        b.norms.toXContent(builder, includeDefaults);
        b.analyzers.indexAnalyzer.toXContent(builder, includeDefaults);
        b.analyzers.searchAnalyzer.toXContent(builder, includeDefaults);
        b.analyzers.searchQuoteAnalyzer.toXContent(builder, includeDefaults);
        b.similarity.toXContent(builder, includeDefaults);
        b.eagerGlobalOrdinals.toXContent(builder, includeDefaults);
        b.analyzers.positionIncrementGap.toXContent(builder, includeDefaults);
        b.fieldData.toXContent(builder, includeDefaults);
        b.freqFilter.toXContent(builder, includeDefaults);
        b.indexPrefixes.toXContent(builder, includeDefaults);
        b.indexPhrases.toXContent(builder, includeDefaults);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (store) {
            return new SyntheticSourceSupport.Native(() -> new StringStoredFieldFieldLoader(fullPath(), leafName()) {
                @Override
                protected void write(XContentBuilder b, Object value) throws IOException {
                    b.value((String) value);
                }
            });
        }

        var kwd = SyntheticSourceHelper.getKeywordFieldMapperForSyntheticSource(this);
        if (kwd != null) {
            return new SyntheticSourceSupport.Native(() -> kwd.syntheticFieldLoader(fullPath(), leafName()));
        }

        return super.syntheticSourceSupport();
    }

    public static class SyntheticSourceHelper {
        public static KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate(FieldType fieldType, MultiFields multiFields) {
            if (fieldType.stored()) {
                return null;
            }
            var kwd = getKeywordFieldMapperForSyntheticSource(multiFields);
            if (kwd != null) {
                return kwd.fieldType();
            }
            return null;
        }

        public static KeywordFieldMapper getKeywordFieldMapperForSyntheticSource(Iterable<? extends Mapper> multiFields) {
            for (Mapper sub : multiFields) {
                if (sub.typeName().equals(KeywordFieldMapper.CONTENT_TYPE)) {
                    KeywordFieldMapper kwd = (KeywordFieldMapper) sub;
                    if (kwd.hasNormalizer() == false && (kwd.fieldType().hasDocValues() || kwd.fieldType().isStored())) {
                        return kwd;
                    }
                }
            }

            return null;
        }
    }
}
