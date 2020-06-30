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
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.IntervalBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;

import static org.elasticsearch.index.mapper.TypeParsers.checkNull;
import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

/** A {@link FieldMapper} for full-text fields. */
public class TextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "text";
    private static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;

    public static final String FAST_PHRASE_SUFFIX = "._index_phrase";

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

    public static class Builder extends FieldMapper.Builder<Builder> {

        private int positionIncrementGap = POSITION_INCREMENT_GAP_USE_ANALYZER;
        private int minPrefixChars = -1;
        private int maxPrefixChars = -1;
        private boolean fielddata = false;
        private boolean indexPhrases = false;
        private boolean eagerGlobalOrdinals = false;
        private double fielddataMinFreq = Defaults.FIELDDATA_MIN_FREQUENCY;
        private double fielddataMaxFreq = Defaults.FIELDDATA_MAX_FREQUENCY;
        private int fielddataMinSegSize = Defaults.FIELDDATA_MIN_SEGMENT_SIZE;
        private SimilarityProvider similarity;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder positionIncrementGap(int positionIncrementGap) {
            if (positionIncrementGap < 0) {
                throw new MapperParsingException("[positions_increment_gap] must be positive, got " + positionIncrementGap);
            }
            this.positionIncrementGap = positionIncrementGap;
            return this;
        }

        public Builder fielddata(boolean fielddata) {
            this.fielddata = fielddata;
            return builder;
        }

        public Builder indexPhrases(boolean indexPhrases) {
            this.indexPhrases = indexPhrases;
            return builder;
        }

        public void similarity(SimilarityProvider similarity) {
            this.similarity = similarity;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues) {
                throw new IllegalArgumentException("[text] fields do not support doc values");
            }
            return super.docValues(docValues);
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return builder;
        }

        public Builder fielddataFrequencyFilter(double minFreq, double maxFreq, int minSegmentSize) {
            this.fielddataMinFreq = minFreq;
            this.fielddataMaxFreq = maxFreq;
            this.fielddataMinSegSize = minSegmentSize;
            return builder;
        }

        public Builder indexPrefixes(int minChars, int maxChars) {

            if (minChars > maxChars) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be less than max_chars [" + maxChars + "]");
            }
            if (minChars < 1) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be greater than zero");
            }
            if (maxChars >= 20) {
                throw new IllegalArgumentException("max_chars [" + maxChars + "] must be less than 20");
            }
            this.minPrefixChars = minChars;
            this.maxPrefixChars = maxChars;
            return this;
        }

        private TextFieldType buildFieldType(BuilderContext context) {
            TextFieldType ft = new TextFieldType(buildFullName(context), fieldType, similarity, meta);
            ft.setIndexAnalyzer(indexAnalyzer);
            ft.setSearchAnalyzer(searchAnalyzer);
            ft.setSearchQuoteAnalyzer(searchQuoteAnalyzer);
            ft.setEagerGlobalOrdinals(eagerGlobalOrdinals);
            if (fielddata) {
                ft.setFielddata(true);
                ft.setFielddataMinFrequency(fielddataMinFreq);
                ft.setFielddataMaxFrequency(fielddataMaxFreq);
                ft.setFielddataMinSegmentSize(fielddataMinSegSize);
            }
            return ft;
        }

        private PrefixFieldMapper buildPrefixMapper(BuilderContext context, TextFieldType tft) {
            if (minPrefixChars == -1) {
                return null;
            }
            if (indexed == false) {
                throw new IllegalArgumentException("Cannot set index_prefixes on unindexed field [" + name() + "]");
            }
            /*
             * Mappings before v7.2.1 use {@link Builder#name} instead of {@link Builder#fullName}
             * to build prefix field names so we preserve the name that was used at creation time
             * even if it is different from the expected one (in case the field is nested under an object
             * or a multi-field). This way search will continue to work on old indices and new indices
             * will use the expected full name.
             */
            String fullName = context.indexCreatedVersion().before(Version.V_7_2_1) ? name() : buildFullName(context);
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
            PrefixFieldType prefixFieldType = new PrefixFieldType(tft, fullName + "._index_prefix",
                minPrefixChars, maxPrefixChars, pft.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0);
            prefixFieldType.setAnalyzer(indexAnalyzer);
            return new PrefixFieldMapper(pft, prefixFieldType);
        }

        private PhraseFieldMapper buildPhraseMapper(BuilderContext context, TextFieldType parent) {
            if (indexPhrases == false) {
                return null;
            }
            if (indexed == false) {
                throw new IllegalArgumentException("Cannot set index_phrases on unindexed field [" + name() + "]");
            }
            if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot set index_phrases on field [" + name() + "] if positions are not enabled");
            }
            FieldType phraseFieldType = new FieldType(fieldType);
            return new PhraseFieldMapper(phraseFieldType, new PhraseFieldType(parent));
        }

        @Override
        public TextFieldMapper build(BuilderContext context) {
            if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException("Cannot set position_increment_gap on field ["
                        + name + "] without positions enabled");
                }
                indexAnalyzer = new NamedAnalyzer(indexAnalyzer, positionIncrementGap);
                searchAnalyzer = new NamedAnalyzer(searchAnalyzer, positionIncrementGap);
                searchQuoteAnalyzer = new NamedAnalyzer(searchQuoteAnalyzer, positionIncrementGap);
            }
            TextFieldType tft = buildFieldType(context);
            return new TextFieldMapper(name,
                    fieldType, tft, positionIncrementGap, buildPrefixMapper(context, tft), buildPhraseMapper(context, tft),
                    multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String fieldName, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TextFieldMapper.Builder builder = new TextFieldMapper.Builder(fieldName);
            builder.indexAnalyzer(parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer());
            builder.searchAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchAnalyzer());
            builder.searchQuoteAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                checkNull(propName, propNode);
                if (propName.equals("position_increment_gap")) {
                    int newPositionIncrementGap = XContentMapValues.nodeIntegerValue(propNode, -1);
                    builder.positionIncrementGap(newPositionIncrementGap);
                    iterator.remove();
                } else if (propName.equals("fielddata")) {
                    builder.fielddata(XContentMapValues.nodeBooleanValue(propNode, "fielddata"));
                    iterator.remove();
                } else if (propName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(propNode, "eager_global_ordinals"));
                    iterator.remove();
                } else if (propName.equals("fielddata_frequency_filter")) {
                    Map<?,?> frequencyFilter = (Map<?, ?>) propNode;
                    double minFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("min"), 0);
                    double maxFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("max"), Integer.MAX_VALUE);
                    int minSegmentSize = XContentMapValues.nodeIntegerValue(frequencyFilter.remove("min_segment_size"), 0);
                    builder.fielddataFrequencyFilter(minFrequency, maxFrequency, minSegmentSize);
                    DocumentMapperParser.checkNoRemainingFields(propName, frequencyFilter, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (propName.equals("index_prefixes")) {
                    Map<?, ?> indexPrefix = (Map<?, ?>) propNode;
                    int minChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("min_chars"),
                        Defaults.INDEX_PREFIX_MIN_CHARS);
                    int maxChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("max_chars"),
                        Defaults.INDEX_PREFIX_MAX_CHARS);
                    builder.indexPrefixes(minChars, maxChars);
                    DocumentMapperParser.checkNoRemainingFields(propName, indexPrefix, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (propName.equals("index_phrases")) {
                    builder.indexPhrases(XContentMapValues.nodeBooleanValue(propNode, "index_phrases"));
                    iterator.remove();
                } else if (propName.equals("similarity")) {
                    SimilarityProvider similarityProvider = TypeParsers.resolveSimilarity(parserContext, fieldName, propNode.toString());
                    builder.similarity(similarityProvider);
                    iterator.remove();
                }
            }
            parseTextField(builder, fieldName, node, parserContext);
            return builder;
        }
    }

    private static class PhraseWrappedAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;

        PhraseWrappedAnalyzer(Analyzer delegate) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
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
        private final Analyzer delegate;

        PrefixWrappedAnalyzer(Analyzer delegate, int minChars, int maxChars) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.minChars = minChars;
            this.maxChars = maxChars;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            TokenFilter filter = new EdgeNGramTokenFilter(components.getTokenStream(), minChars, maxChars, false);
            return new TokenStreamComponents(components.getSource(), filter);
        }
    }

    private static final class PhraseFieldType extends StringFieldType {

        final TextFieldType parent;

        PhraseFieldType(TextFieldType parent) {
            super(parent.name() + FAST_PHRASE_SUFFIX, true, false, parent.getTextSearchInfo(), Collections.emptyMap());
            setAnalyzer(parent.indexAnalyzer().name(), parent.indexAnalyzer().analyzer());
            this.parent = parent;
        }

        void setAnalyzer(String name, Analyzer delegate) {
            setIndexAnalyzer(new NamedAnalyzer(name, AnalyzerScope.INDEX, new PhraseWrappedAnalyzer(delegate)));
        }

        @Override
        public MappedFieldType clone() {
            return new PhraseFieldType(parent);
        }

        @Override
        public String typeName() {
            return "phrase";
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }
    }

    static final class PrefixFieldType extends StringFieldType {

        final int minChars;
        final int maxChars;
        final TextFieldType parentField;
        final boolean hasPositions;

        PrefixFieldType(TextFieldType parentField, String name, int minChars, int maxChars, boolean hasPositions) {
            super(name, true, false, parentField.getTextSearchInfo(), Collections.emptyMap());
            this.minChars = minChars;
            this.maxChars = maxChars;
            this.parentField = parentField;
            this.hasPositions = hasPositions;
        }

        void setAnalyzer(NamedAnalyzer delegate) {
            setIndexAnalyzer(new NamedAnalyzer(delegate.name(), AnalyzerScope.INDEX,
                new PrefixWrappedAnalyzer(delegate.analyzer(), minChars, maxChars)));
        }

        boolean accept(int length) {
            return length >= minChars - 1 && length <= maxChars;
        }

        void doXContent(XContentBuilder builder) throws IOException {
            builder.startObject("index_prefixes");
            builder.field("min_chars", minChars);
            builder.field("max_chars", maxChars);
            builder.endObject();
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (value.length() >= minChars) {
                return super.termQuery(value, context);
            }
            List<Automaton> automata = new ArrayList<>();
            automata.add(Automata.makeString(value));
            for (int i = value.length(); i < minChars; i++) {
                automata.add(Automata.makeAnyChar());
            }
            Automaton automaton = Operations.concatenate(automata);
            AutomatonQuery query = new AutomatonQuery(new Term(name(), value + "*"), automaton);
            query.setRewriteMethod(method);
            return new BooleanQuery.Builder()
                .add(query, BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(parentField.name(), value)), BooleanClause.Occur.SHOULD)
                .build();
        }

        public IntervalsSource intervals(BytesRef term) {
            if (hasPositions == false) {
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
        public PrefixFieldType clone() {
            return new PrefixFieldType(parentField, name(), minChars, maxChars, hasPositions);
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
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            PrefixFieldType that = (PrefixFieldType) o;
            return minChars == that.minChars &&
                maxChars == that.maxChars;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), minChars, maxChars);
        }
    }

    private static final class PhraseFieldMapper extends FieldMapper {

        PhraseFieldMapper(FieldType fieldType, PhraseFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        protected void parseCreateField(ParseContext context) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void mergeOptions(FieldMapper other, List<String> conflicts) {

        }

        @Override
        protected String contentType() {
            return "phrase";
        }
    }

    private static final class PrefixFieldMapper extends FieldMapper {

        protected PrefixFieldMapper(FieldType fieldType, PrefixFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            context.doc().add(new Field(fieldType().name(), value, fieldType));
        }

        @Override
        protected void parseCreateField(ParseContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void mergeOptions(FieldMapper other, List<String> conflicts) {

        }

        @Override
        protected String contentType() {
            return "prefix";
        }

        @Override
        public String toString() {
            return fieldType().toString();
        }
    }

    public static class TextFieldType extends StringFieldType {

        private boolean fielddata;
        private double fielddataMinFrequency;
        private double fielddataMaxFrequency;
        private int fielddataMinSegmentSize;
        private PrefixFieldType prefixFieldType;
        private boolean indexPhrases = false;
        private final FieldType indexedFieldType;

        public TextFieldType(String name, FieldType indexedFieldType, SimilarityProvider similarity, Map<String, String> meta) {
            super(name, indexedFieldType.indexOptions() != IndexOptions.NONE, false,
                new TextSearchInfo(indexedFieldType, similarity), meta);
            this.indexedFieldType = indexedFieldType;
            fielddata = false;
            fielddataMinFrequency = Defaults.FIELDDATA_MIN_FREQUENCY;
            fielddataMaxFrequency = Defaults.FIELDDATA_MAX_FREQUENCY;
            fielddataMinSegmentSize = Defaults.FIELDDATA_MIN_SEGMENT_SIZE;
        }

        public TextFieldType(String name, boolean indexed, Map<String, String> meta) {
            super(name, indexed, false, new TextSearchInfo(Defaults.FIELD_TYPE, null), meta);
            this.indexedFieldType = Defaults.FIELD_TYPE;
            fielddata = false;
        }

        public TextFieldType(String name) {
            this(name, Defaults.FIELD_TYPE, null, Collections.emptyMap());
        }

        protected TextFieldType(TextFieldType ref) {
            super(ref);
            this.fielddata = ref.fielddata;
            this.fielddataMinFrequency = ref.fielddataMinFrequency;
            this.fielddataMaxFrequency = ref.fielddataMaxFrequency;
            this.fielddataMinSegmentSize = ref.fielddataMinSegmentSize;
            this.indexPhrases = ref.indexPhrases;
            if (ref.prefixFieldType != null) {
                this.prefixFieldType = ref.prefixFieldType.clone();
            }
            this.indexedFieldType = ref.indexedFieldType;
        }

        @Override
        public TextFieldType clone() {
            return new TextFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            TextFieldType that = (TextFieldType) o;
            return fielddata == that.fielddata
                    && indexPhrases == that.indexPhrases
                    && Objects.equals(prefixFieldType, that.prefixFieldType)
                    && fielddataMinFrequency == that.fielddataMinFrequency
                    && fielddataMaxFrequency == that.fielddataMaxFrequency
                    && fielddataMinSegmentSize == that.fielddataMinSegmentSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fielddata, indexPhrases, prefixFieldType,
                    fielddataMinFrequency, fielddataMaxFrequency, fielddataMinSegmentSize);
        }

        public boolean fielddata() {
            return fielddata;
        }

        public void setFielddata(boolean fielddata) {
            this.fielddata = fielddata;
        }

        public double fielddataMinFrequency() {
            return fielddataMinFrequency;
        }

        public void setFielddataMinFrequency(double fielddataMinFrequency) {
            this.fielddataMinFrequency = fielddataMinFrequency;
        }

        public double fielddataMaxFrequency() {
            return fielddataMaxFrequency;
        }

        public void setFielddataMaxFrequency(double fielddataMaxFrequency) {
            this.fielddataMaxFrequency = fielddataMaxFrequency;
        }

        public int fielddataMinSegmentSize() {
            return fielddataMinSegmentSize;
        }

        public void setFielddataMinSegmentSize(int fielddataMinSegmentSize) {
            this.fielddataMinSegmentSize = fielddataMinSegmentSize;
        }

        void setPrefixFieldType(PrefixFieldType prefixFieldType) {
            this.prefixFieldType = prefixFieldType;
        }

        void setIndexPhrases(boolean indexPhrases) {
            this.indexPhrases = indexPhrases;
        }

        public PrefixFieldType getPrefixFieldType() {
            return this.prefixFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (prefixFieldType == null || prefixFieldType.accept(value.length()) == false) {
                return super.prefixQuery(value, method, context);
            }
            Query tq = prefixFieldType.prefixQuery(value, method, context);
            if (method == null || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
                return new ConstantScoreQuery(tq);
            }
            return tq;
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
            failIfNotIndexed();
            if (prefixFieldType != null
                    && value.length() >= prefixFieldType.minChars
                    && value.length() <= prefixFieldType.maxChars
                    && prefixFieldType.getTextSearchInfo().hasPositions()) {

                return new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixFieldType.name(), indexedValueForSearch(value))), name());
            } else {
                SpanMultiTermQueryWrapper<?> spanMulti =
                    new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(name(), indexedValueForSearch(value))));
                spanMulti.setRewriteMethod(method);
                return spanMulti;
            }
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (indexedFieldType.omitNorms()) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }

        @Override
        public IntervalsSource intervals(String text, int maxGaps, boolean ordered,
                                         NamedAnalyzer analyzer, boolean prefix) throws IOException {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            if (analyzer == null) {
                analyzer = searchAnalyzer();
            }
            if (prefix) {
                BytesRef normalizedTerm = analyzer.normalize(name(), text);
                if (prefixFieldType != null) {
                    return prefixFieldType.intervals(normalizedTerm);
                }
                return Intervals.prefix(normalizedTerm);
            }
            IntervalBuilder builder = new IntervalBuilder(name(), analyzer == null ? searchAnalyzer() : analyzer);
            return builder.analyzeText(text, maxGaps, ordered);
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements) throws IOException {
            String field = name();
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
                }
                else {
                    position += 1;
                }
                builder.add(new Term(field, termAtt.getBytesRef()), position);
            }

            return builder.build();
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            String field = name();
            if (indexPhrases && slop == 0 && hasGaps(stream) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            return createPhraseQuery(stream, field, slop, enablePositionIncrements);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            if (fielddata == false) {
                throw new IllegalArgumentException("Text fields are not optimised for operations that require per-document "
                    + "field data like aggregations and sorting, so these operations are disabled by default. Please use a "
                    + "keyword field instead. Alternatively, set fielddata=true on [" + name() + "] in order to load "
                    + "field data by uninverting the inverted index. Note that this can use significant memory.");
            }
            return new PagedBytesIndexFieldData.Builder(
                fielddataMinFrequency,
                fielddataMaxFrequency,
                fielddataMinSegmentSize,
                CoreValuesSourceType.BYTES
            );
        }

    }

    private final int positionIncrementGap;
    private PrefixFieldMapper prefixFieldMapper;
    private PhraseFieldMapper phraseFieldMapper;

    protected TextFieldMapper(String simpleName, FieldType fieldType, TextFieldType mappedFieldType,
                                int positionIncrementGap, PrefixFieldMapper prefixFieldMapper,
                                PhraseFieldMapper phraseFieldMapper,
                                MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        assert fieldType.tokenized();
        assert mappedFieldType.hasDocValues() == false;
        if (fieldType.indexOptions() == IndexOptions.NONE && fieldType().fielddata()) {
            throw new IllegalArgumentException("Cannot enable fielddata on a [text] field that is not indexed: [" + name() + "]");
        }
        this.positionIncrementGap = positionIncrementGap;
        this.prefixFieldMapper = prefixFieldMapper;
        this.phraseFieldMapper = phraseFieldMapper;
        if (prefixFieldMapper != null) {
            mappedFieldType.setPrefixFieldType((PrefixFieldType)prefixFieldMapper.mappedFieldType);
        }
        mappedFieldType.setIndexPhrases(phraseFieldMapper != null);
    }

    @Override
    protected TextFieldMapper clone() {
        return (TextFieldMapper) super.clone();
    }

    public int getPositionIncrementGap() {
        return this.positionIncrementGap;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), value, fieldType);
            context.doc().add(field);
            if (fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
            if (prefixFieldMapper != null) {
                prefixFieldMapper.addField(context, value);
            }
            if (phraseFieldMapper != null) {
                context.doc().add(new Field(phraseFieldMapper.fieldType().name(), value, phraseFieldMapper.fieldType));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        if (prefixFieldMapper != null) {
            subIterators.add(prefixFieldMapper);
        }
        if (phraseFieldMapper != null) {
            subIterators.add(phraseFieldMapper);
        }
        if (subIterators.size() == 0) {
            return super.iterator();
        }
        return Iterators.concat(super.iterator(), subIterators.iterator());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        TextFieldMapper mw = (TextFieldMapper) other;
        if (Objects.equals(mw.fieldType().getTextSearchInfo().getSimilarity(),
            this.fieldType().getTextSearchInfo().getSimilarity()) == false) {
            conflicts.add("mapper [" + name() + "] has different [similarity] settings");
        }
        if (mw.fieldType().indexPhrases != this.fieldType().indexPhrases) {
            conflicts.add("mapper [" + name() + "] has different [index_phrases] settings");
        }
        if (Objects.equals(mw.fieldType().prefixFieldType, this.fieldType().prefixFieldType) == false) {
            conflicts.add("mapper [" + name() + "] has different [index_prefixes] settings");
        }
        if (this.prefixFieldMapper != null && mw.prefixFieldMapper != null) {
            this.prefixFieldMapper = (PrefixFieldMapper) this.prefixFieldMapper.merge(mw.prefixFieldMapper);
        }
        if (this.phraseFieldMapper != null && mw.phraseFieldMapper != null) {
            this.phraseFieldMapper = (PhraseFieldMapper) this.phraseFieldMapper.merge(mw.phraseFieldMapper);
        }
        this.fieldType().setSearchAnalyzer(mw.fieldType().searchAnalyzer());
        this.fieldType().setSearchQuoteAnalyzer(mw.fieldType().searchQuoteAnalyzer());
    }

    @Override
    public TextFieldType fieldType() {
        return (TextFieldType) super.fieldType();
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (fieldType.indexOptions() != IndexOptions.NONE
            && (includeDefaults || fieldType.indexOptions() != Defaults.FIELD_TYPE.indexOptions())) {
            builder.field("index_options", indexOptionToString(fieldType.indexOptions()));
        }
        if (includeDefaults || fieldType.storeTermVectors() != Defaults.FIELD_TYPE.storeTermVectors()) {
            builder.field("term_vector", termVectorOptionsToString(fieldType));
        }
        if (includeDefaults || fieldType.omitNorms()) {
            builder.field("norms", fieldType.omitNorms() == false);
        }
        doXContentAnalyzers(builder, includeDefaults);
        if (fieldType().getTextSearchInfo().getSimilarity() != null) {
            builder.field("similarity", fieldType().getTextSearchInfo().getSimilarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityService.DEFAULT_SIMILARITY);
        }
        if (includeDefaults || fieldType().eagerGlobalOrdinals()) {
            builder.field("eager_global_ordinals", fieldType().eagerGlobalOrdinals());
        }
        if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
            builder.field("position_increment_gap", positionIncrementGap);
        }

        if (includeDefaults || fieldType().fielddata() != false) {
            builder.field("fielddata", fieldType().fielddata());
        }
        if (fieldType().fielddata()) {
            if (includeDefaults
                    || fieldType().fielddataMinFrequency() != Defaults.FIELDDATA_MIN_FREQUENCY
                    || fieldType().fielddataMaxFrequency() != Defaults.FIELDDATA_MAX_FREQUENCY
                    || fieldType().fielddataMinSegmentSize() != Defaults.FIELDDATA_MIN_SEGMENT_SIZE) {
                builder.startObject("fielddata_frequency_filter");
                if (includeDefaults || fieldType().fielddataMinFrequency() != Defaults.FIELDDATA_MIN_FREQUENCY) {
                    builder.field("min", fieldType().fielddataMinFrequency());
                }
                if (includeDefaults || fieldType().fielddataMaxFrequency() != Defaults.FIELDDATA_MAX_FREQUENCY) {
                    builder.field("max", fieldType().fielddataMaxFrequency());
                }
                if (includeDefaults || fieldType().fielddataMinSegmentSize() != Defaults.FIELDDATA_MIN_SEGMENT_SIZE) {
                    builder.field("min_segment_size", fieldType().fielddataMinSegmentSize());
                }
                builder.endObject();
            }
        }
        if (fieldType().prefixFieldType != null) {
            fieldType().prefixFieldType.doXContent(builder);
        }
        if (fieldType().indexPhrases) {
            builder.field("index_phrases", fieldType().indexPhrases);
        }
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

    public static Query createPhrasePrefixQuery(TokenStream stream, String field, int slop, int maxExpansions,
                                                String prefixField, IntPredicate usePrefixField) throws IOException {
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
            Term[] newTerms = Arrays.stream(terms[0])
                .map(term -> new Term(prefixField, term.bytes()))
                .toArray(Term[]::new);
            return new SynonymQuery(newTerms);
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
                    FieldMaskingSpanQuery fieldMask =
                        new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixField, posTerms[0].bytes())), field);
                    spanQuery.addClause(fieldMask);
                } else {
                    SpanQuery[] queries = Arrays.stream(posTerms)
                        .map(term -> new FieldMaskingSpanQuery(
                            new SpanTermQuery(new Term(prefixField, term.bytes())), field)
                        )
                        .toArray(SpanQuery[]::new);
                    spanQuery.addClause(new SpanOrQuery(queries));
                }
            } else {
                if (posTerms.length == 1) {
                    spanQuery.addClause(new SpanTermQuery(posTerms[0]));
                } else {
                    SpanTermQuery[] queries = Arrays.stream(posTerms)
                        .map(SpanTermQuery::new)
                        .toArray(SpanTermQuery[]::new);
                    spanQuery.addClause(new SpanOrQuery(queries));
                }
            }
        }
        return spanQuery.build();
    }
}
