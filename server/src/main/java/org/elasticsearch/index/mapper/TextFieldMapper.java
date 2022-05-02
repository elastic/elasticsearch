/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

/** A {@link FieldMapper} for full-text fields. */
public class TextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "text";
    private static final String FAST_PHRASE_SUFFIX = "._index_phrase";
    private static final String FAST_PREFIX_SUFFIX = "._index_prefix";

    public static class Defaults {
        public static final double FIELDDATA_MIN_FREQUENCY = 0;
        public static final double FIELDDATA_MAX_FREQUENCY = Integer.MAX_VALUE;
        public static final int FIELDDATA_MIN_SEGMENT_SIZE = 0;
        public static final int INDEX_PREFIX_MIN_CHARS = 2;
        public static final int INDEX_PREFIX_MAX_CHARS = 5;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
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

        private final Version indexCreatedVersion;

        private final Parameter<Boolean> index = Parameter.indexParam(m -> ((TextFieldMapper) m).index, true);
        private final Parameter<Boolean> store = Parameter.storeParam(m -> ((TextFieldMapper) m).store, false);

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
            m -> ((TextFieldMapper) m).eagerGlobalOrdinals,
            false
        );

        final Parameter<Boolean> indexPhrases = Parameter.boolParam("index_phrases", false, m -> ((TextFieldMapper) m).indexPhrases, false);
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

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            this(name, Version.CURRENT, indexAnalyzers);
        }

        public Builder(String name, Version indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> ((TextFieldMapper) m).indexAnalyzer,
                m -> (((TextFieldMapper) m).positionIncrementGap)
            );
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
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(
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
                meta
            );
        }

        private TextFieldType buildFieldType(FieldType fieldType, MapperBuilderContext context) {
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            if (analyzers.positionIncrementGap.isConfigured()) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException(
                        "Cannot set position_increment_gap on field [" + name + "] without positions enabled"
                    );
                }
            }
            TextSearchInfo tsi = new TextSearchInfo(fieldType, similarity.getValue(), searchAnalyzer, searchQuoteAnalyzer);
            TextFieldType ft = new TextFieldType(context.buildFullName(name), index.getValue(), store.getValue(), tsi, meta.getValue());
            ft.eagerGlobalOrdinals = eagerGlobalOrdinals.getValue();
            if (fieldData.getValue()) {
                ft.setFielddata(true, freqFilter.getValue());
            }
            return ft;
        }

        private SubFieldInfo buildPrefixInfo(MapperBuilderContext context, FieldType fieldType, TextFieldType tft) {
            if (indexPrefixes.get() == null) {
                return null;
            }
            if (index.getValue() == false) {
                throw new IllegalArgumentException("Cannot set index_prefixes on unindexed field [" + name() + "]");
            }
            /*
             * Mappings before v7.2.1 use {@link Builder#name} instead of {@link Builder#fullName}
             * to build prefix field names so we preserve the name that was used at creation time
             * even if it is different from the expected one (in case the field is nested under an object
             * or a multi-field). This way search will continue to work on old indices and new indices
             * will use the expected full name.
             */
            String fullName = indexCreatedVersion.before(Version.V_7_2_1) ? name() : context.buildFullName(name);
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
                throw new IllegalArgumentException("Cannot set index_phrases on unindexed field [" + name() + "]");
            }
            if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot set index_phrases on field [" + name() + "] if positions are not enabled");
            }
            FieldType phraseFieldType = new FieldType(fieldType);
            parent.setIndexPhrases();
            PhraseWrappedAnalyzer a = new PhraseWrappedAnalyzer(
                analyzers.getIndexAnalyzer().analyzer(),
                analyzers.positionIncrementGap.get()
            );
            return new SubFieldInfo(parent.name() + FAST_PHRASE_SUFFIX, phraseFieldType, a);
        }

        public Map<String, NamedAnalyzer> indexAnalyzers(String name, SubFieldInfo phraseFieldInfo, SubFieldInfo prefixFieldInfo) {
            Map<String, NamedAnalyzer> analyzers = new HashMap<>();
            NamedAnalyzer main = this.analyzers.getIndexAnalyzer();
            analyzers.put(name, main);
            if (phraseFieldInfo != null) {
                analyzers.put(
                    phraseFieldInfo.field,
                    new NamedAnalyzer(main.name() + "_phrase", AnalyzerScope.INDEX, phraseFieldInfo.analyzer)
                );
            }
            if (prefixFieldInfo != null) {
                analyzers.put(
                    prefixFieldInfo.field,
                    new NamedAnalyzer(main.name() + "_prefix", AnalyzerScope.INDEX, prefixFieldInfo.analyzer)
                );
            }
            return analyzers;
        }

        @Override
        public TextFieldMapper build(MapperBuilderContext context) {
            FieldType fieldType = TextParams.buildFieldType(index, store, indexOptions, norms, termVectors);
            TextFieldType tft = buildFieldType(fieldType, context);
            SubFieldInfo phraseFieldInfo = buildPhraseInfo(fieldType, tft);
            SubFieldInfo prefixFieldInfo = buildPrefixInfo(context, fieldType, tft);
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            for (Mapper mapper : multiFields) {
                if (mapper.name().endsWith(FAST_PHRASE_SUFFIX) || mapper.name().endsWith(FAST_PREFIX_SUFFIX)) {
                    throw new MapperParsingException("Cannot use reserved field name [" + mapper.name() + "]");
                }
            }
            return new TextFieldMapper(
                name,
                fieldType,
                tft,
                indexAnalyzers(tft.name(), phraseFieldInfo, prefixFieldInfo),
                prefixFieldInfo,
                phraseFieldInfo,
                multiFields,
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

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
                automata.add(AutomatonQueries.toCaseInsensitiveString(value, Integer.MAX_VALUE));
            } else {
                automata.add(Automata.makeString(value));
            }

            for (int i = value.length(); i < minChars; i++) {
                automata.add(Automata.makeAnyChar());
            }
            Automaton automaton = Operations.concatenate(automata);
            AutomatonQuery query = new AutomatonQuery(new Term(name(), value + "*"), automaton);
            query.setRewriteMethod(method);
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
            return Intervals.or(Intervals.fixField(name(), Intervals.wildcard(new BytesRef(wildcardTerm))), Intervals.term(term));
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
            this.fieldType = fieldType;
            this.analyzer = analyzer;
            this.field = field;
        }

    }

    public static class TextFieldType extends StringFieldType {

        private boolean fielddata;
        private FielddataFrequencyFilter filter;
        private PrefixFieldType prefixFieldType;
        private boolean indexPhrases = false;
        private boolean eagerGlobalOrdinals = false;

        public TextFieldType(String name, boolean indexed, boolean stored, TextSearchInfo tsi, Map<String, String> meta) {
            super(name, indexed, stored, false, tsi, meta);
            fielddata = false;
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
        }

        public TextFieldType(String name) {
            this(
                name,
                true,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                Collections.emptyMap()
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

        void setIndexPhrases() {
            this.indexPhrases = true;
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
                || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
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
                spanMulti.setRewriteMethod(method);
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
            return Intervals.prefix(term);
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
            FuzzyQuery fq = new FuzzyQuery(new Term(name(), term), maxDistance, prefixLength, 128, transpositions);
            return Intervals.multiterm(fq.getAutomata(), term);
        }

        @Override
        public IntervalsSource wildcardIntervals(BytesRef pattern, SearchExecutionContext context) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            return Intervals.wildcard(pattern);
        }

        private void checkForPositions() {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalStateException("field:[" + name() + "] was indexed without position data; cannot run PhraseQuery");
            }
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, SearchExecutionContext context)
            throws IOException {
            String field = name();
            checkForPositions();
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
                checkForPositions();
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            if (fielddata == false) {
                throw new IllegalArgumentException(
                    "Text fields are not optimised for operations that require per-document "
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

    }

    private final Version indexCreatedVersion;
    private final boolean index;
    private final boolean store;
    private final String indexOptions;
    private final boolean norms;
    private final String termVectors;
    private final SimilarityProvider similarity;
    private final NamedAnalyzer indexAnalyzer;
    private final IndexAnalyzers indexAnalyzers;
    private final int positionIncrementGap;
    private final boolean eagerGlobalOrdinals;
    private final PrefixConfig indexPrefixes;
    private final FielddataFrequencyFilter freqFilter;
    private final boolean fieldData;
    private final boolean indexPhrases;
    private final FieldType fieldType;
    private final SubFieldInfo prefixFieldInfo;
    private final SubFieldInfo phraseFieldInfo;

    protected TextFieldMapper(
        String simpleName,
        FieldType fieldType,
        TextFieldType mappedFieldType,
        Map<String, NamedAnalyzer> indexAnalyzers,
        SubFieldInfo prefixFieldInfo,
        SubFieldInfo phraseFieldInfo,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, indexAnalyzers, multiFields, copyTo, false, null);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        if (fieldType.indexOptions() == IndexOptions.NONE && fieldType().fielddata()) {
            throw new IllegalArgumentException("Cannot enable fielddata on a [text] field that is not indexed: [" + name() + "]");
        }
        this.fieldType = fieldType;
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
        this.eagerGlobalOrdinals = builder.eagerGlobalOrdinals.getValue();
        this.indexPrefixes = builder.indexPrefixes.getValue();
        this.freqFilter = builder.freqFilter.getValue();
        this.fieldData = builder.fieldData.get();
        this.indexPhrases = builder.indexPhrases.getValue();
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
        this.multiFields.toXContent(builder, params);
        this.copyTo.toXContent(builder);
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
}
