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
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType.hasGaps;
import static org.elasticsearch.index.mapper.TypeParsers.checkNull;
import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

/**
 * Mapper for a text field that optimizes itself for as-you-type completion by indexing its content into subfields. Each subfield
 * modifies the analysis chain of the root field to index terms the user would create as they type out the value in the root field
 *
 * The structure of these fields is
 *
 * <pre>
 *     [ SearchAsYouTypeFieldMapper, SearchAsYouTypeFieldType, unmodified analysis ]
 *     ├── [ ShingleFieldMapper, ShingleFieldType, analysis wrapped with 2-shingles ]
 *     ├── ...
 *     ├── [ ShingleFieldMapper, ShingleFieldType, analysis wrapped with max_shingle_size-shingles ]
 *     └── [ PrefixFieldMapper, PrefixFieldType, analysis wrapped with max_shingle_size-shingles and edge-ngrams ]
 * </pre>
 */
public class SearchAsYouTypeFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "search_as_you_type";
    private static final int MAX_SHINGLE_SIZE_LOWER_BOUND = 2;
    private static final int MAX_SHINGLE_SIZE_UPPER_BOUND = 4;
    private static final String PREFIX_FIELD_SUFFIX = "._index_prefix";

    public static class Defaults {

        public static final int MIN_GRAM = 1;
        public static final int MAX_GRAM = 20;
        public static final int MAX_SHINGLE_SIZE = 3;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name,
                                          Map<String, Object> node,
                                          ParserContext parserContext) throws MapperParsingException {

            final Builder builder = new Builder(name);

            builder.indexAnalyzer(parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer());
            builder.searchAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchAnalyzer());
            builder.searchQuoteAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                final Map.Entry<String, Object> entry = iterator.next();
                final String fieldName = entry.getKey();
                final Object fieldNode = entry.getValue();
                checkNull(fieldName, fieldNode);
                if (fieldName.equals("max_shingle_size")) {
                    builder.maxShingleSize(nodeIntegerValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("similarity")) {
                    SimilarityProvider similarityProvider = TypeParsers.resolveSimilarity(parserContext, fieldName, fieldNode.toString());
                    builder.similarity(similarityProvider);
                    iterator.remove();
                }
                // TODO should we allow to configure the prefix field
            }
            parseTextField(builder, name, node, parserContext);
            return builder;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        private int maxShingleSize = Defaults.MAX_SHINGLE_SIZE;
        private SimilarityProvider similarity;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public void similarity(SimilarityProvider similarity) {
            this.similarity = similarity;
        }

        public Builder maxShingleSize(int maxShingleSize) {
            if (maxShingleSize < MAX_SHINGLE_SIZE_LOWER_BOUND || maxShingleSize > MAX_SHINGLE_SIZE_UPPER_BOUND) {
                throw new MapperParsingException("[max_shingle_size] must be at least [" + MAX_SHINGLE_SIZE_LOWER_BOUND + "] and at most " +
                    "[" + MAX_SHINGLE_SIZE_UPPER_BOUND + "], got [" + maxShingleSize + "]");
            }
            this.maxShingleSize = maxShingleSize;
            return builder;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues) {
                throw new IllegalArgumentException("mapper [" + name() + "] of type [search_as_you_type] does not support doc values");
            }
            return this;
        }

        @Override
        public SearchAsYouTypeFieldMapper build(Mapper.BuilderContext context) {

            SearchAsYouTypeFieldType ft = new SearchAsYouTypeFieldType(buildFullName(context), fieldType, similarity,
                searchAnalyzer, searchQuoteAnalyzer, meta);
            ft.setIndexAnalyzer(indexAnalyzer);

            // set up the prefix field
            FieldType prefixft = new FieldType(fieldType);
            prefixft.setStoreTermVectors(false);
            prefixft.setOmitNorms(true);
            prefixft.setStored(false);
            final String fullName = buildFullName(context);
            // wrap the root field's index analyzer with shingles and edge ngrams
            final Analyzer prefixIndexWrapper =
                SearchAsYouTypeAnalyzer.withShingleAndPrefix(indexAnalyzer.analyzer(), maxShingleSize);
            // wrap the root field's search analyzer with only shingles
            final NamedAnalyzer prefixSearchWrapper = new NamedAnalyzer(searchAnalyzer.name(), searchAnalyzer.scope(),
                SearchAsYouTypeAnalyzer.withShingle(searchAnalyzer.analyzer(), maxShingleSize));
            // don't wrap the root field's search quote analyzer as prefix field doesn't support phrase queries
            TextSearchInfo prefixSearchInfo = new TextSearchInfo(prefixft, similarity, prefixSearchWrapper, searchQuoteAnalyzer);
            final PrefixFieldType prefixFieldType
                = new PrefixFieldType(fullName, prefixSearchInfo, Defaults.MIN_GRAM, Defaults.MAX_GRAM);
            prefixFieldType.setIndexAnalyzer(new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, prefixIndexWrapper));
            final PrefixFieldMapper prefixFieldMapper = new PrefixFieldMapper(prefixft, prefixFieldType);


            // set up the shingle fields
            final ShingleFieldMapper[] shingleFieldMappers = new ShingleFieldMapper[maxShingleSize - 1];
            final ShingleFieldType[] shingleFieldTypes = new ShingleFieldType[maxShingleSize - 1];
            for (int i = 0; i < shingleFieldMappers.length; i++) {
                final int shingleSize = i + 2;
                FieldType shingleft = new FieldType(fieldType);
                shingleft.setStored(false);
                String fieldName = getShingleFieldName(buildFullName(context), shingleSize);
                // wrap the root field's index, search, and search quote analyzers with shingles
                final SearchAsYouTypeAnalyzer shingleIndexWrapper =
                    SearchAsYouTypeAnalyzer.withShingle(indexAnalyzer.analyzer(), shingleSize);
                final NamedAnalyzer shingleSearchWrapper = new NamedAnalyzer(searchAnalyzer.name(), searchAnalyzer.scope(),
                    SearchAsYouTypeAnalyzer.withShingle(searchAnalyzer.analyzer(), shingleSize));
                final NamedAnalyzer shingleSearchQuoteWrapper = new NamedAnalyzer(searchQuoteAnalyzer.name(), searchQuoteAnalyzer.scope(),
                    SearchAsYouTypeAnalyzer.withShingle(searchQuoteAnalyzer.analyzer(), shingleSize));
                TextSearchInfo textSearchInfo
                    = new TextSearchInfo(shingleft, similarity, shingleSearchWrapper, shingleSearchQuoteWrapper);
                final ShingleFieldType shingleFieldType = new ShingleFieldType(fieldName, shingleSize, textSearchInfo);
                shingleFieldType.setIndexAnalyzer(new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, shingleIndexWrapper));
                shingleFieldType.setPrefixFieldType(prefixFieldType);
                shingleFieldTypes[i] = shingleFieldType;
                shingleFieldMappers[i] = new ShingleFieldMapper(shingleft, shingleFieldType);
            }
            ft.setPrefixField(prefixFieldType);
            ft.setShingleFields(shingleFieldTypes);
            return new SearchAsYouTypeFieldMapper(name, fieldType, ft, copyTo,
                maxShingleSize, prefixFieldMapper, shingleFieldMappers);
        }
    }

    private static int countPosition(TokenStream stream) throws IOException {
        assert stream instanceof CachingTokenFilter;
        PositionIncrementAttribute posIncAtt = stream.getAttribute(PositionIncrementAttribute.class);
        stream.reset();
        int positionCount = 0;
        while (stream.incrementToken()) {
            if (posIncAtt.getPositionIncrement() != 0) {
                positionCount += posIncAtt.getPositionIncrement();
            }
        }
        return positionCount;
    }

    /**
     * The root field type, which most queries should target as it will delegate queries to subfields better optimized for the query. When
     * handling phrase queries, it analyzes the query text to find the appropriate sized shingle subfield to delegate to. When handling
     * prefix or phrase prefix queries, it delegates to the prefix subfield
     */
    static class SearchAsYouTypeFieldType extends StringFieldType {

        PrefixFieldType prefixField;
        ShingleFieldType[] shingleFields = new ShingleFieldType[0];

        SearchAsYouTypeFieldType(String name, FieldType fieldType, SimilarityProvider similarity,
                                 NamedAnalyzer searchAnalyzer, NamedAnalyzer searchQuoteAnalyzer, Map<String, String> meta) {
            super(name, fieldType.indexOptions() != IndexOptions.NONE, false,
                new TextSearchInfo(fieldType, similarity, searchAnalyzer, searchQuoteAnalyzer), meta);
        }

        public void setPrefixField(PrefixFieldType prefixField) {
            this.prefixField = prefixField;
        }

        public void setShingleFields(ShingleFieldType[] shingleFields) {
            this.shingleFields = shingleFields;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private ShingleFieldType shingleFieldForPositions(int positions) {
            final int indexFromShingleSize = Math.max(positions - 2, 0);
            return shingleFields[Math.min(indexFromShingleSize, shingleFields.length - 1)];
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (getTextSearchInfo().hasNorms() == false) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (prefixField == null || prefixField.termLengthWithinBounds(value.length()) == false) {
                return super.prefixQuery(value, method, context);
            } else {
                final Query query = prefixField.prefixQuery(value, method, context);
                if (method == null
                    || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                    || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
                    return new ConstantScoreQuery(query);
                } else {
                    return query;
                }
            }
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            int numPos = countPosition(stream);
            if (shingleFields.length == 0 || slop > 0 || hasGaps(stream) || numPos <= 1) {
                return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
            }
            final ShingleFieldType shingleField = shingleFieldForPositions(numPos);
            stream = new FixedShingleFilter(stream, shingleField.shingleSize);
            return shingleField.phraseQuery(stream, 0, true);
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            int numPos = countPosition(stream);
            if (shingleFields.length == 0 || slop > 0 || hasGaps(stream) || numPos <= 1) {
                return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
            }
            final ShingleFieldType shingleField = shingleFieldForPositions(numPos);
            stream = new FixedShingleFilter(stream, shingleField.shingleSize);
            return shingleField.multiPhraseQuery(stream, 0, true);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
            int numPos = countPosition(stream);
            if (shingleFields.length == 0 || slop > 0 || hasGaps(stream) || numPos <= 1) {
                return TextFieldMapper.createPhrasePrefixQuery(stream, name(), slop, maxExpansions,
                    null, null);
            }
            final ShingleFieldType shingleField = shingleFieldForPositions(numPos);
            stream = new FixedShingleFilter(stream, shingleField.shingleSize);
            return shingleField.phrasePrefixQuery(stream, 0, maxExpansions);
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
            if (prefixField != null && prefixField.termLengthWithinBounds(value.length())) {
                return new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixField.name(), indexedValueForSearch(value))), name());
            } else {
                SpanMultiTermQueryWrapper<?> spanMulti =
                    new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(name(), indexedValueForSearch(value))));
                spanMulti.setRewriteMethod(method);
                return spanMulti;
            }
        }
    }

    /**
     * The prefix field type handles prefix and phrase prefix queries that are delegated to it by the other field types in a
     * search_as_you_type structure
     */
    static final class PrefixFieldType extends StringFieldType {

        final int minChars;
        final int maxChars;
        final String parentField;

        PrefixFieldType(String parentField, TextSearchInfo textSearchInfo, int minChars, int maxChars) {
            super(parentField + PREFIX_FIELD_SUFFIX, true, false, textSearchInfo, Collections.emptyMap());
            this.minChars = minChars;
            this.maxChars = maxChars;
            this.parentField = parentField;
        }

        boolean termLengthWithinBounds(int length) {
            return length >= minChars - 1 && length <= maxChars;
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
                .add(new TermQuery(new Term(parentField, value)), BooleanClause.Occur.SHOULD)
                .build();
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
    }

    static final class PrefixFieldMapper extends FieldMapper {

        PrefixFieldMapper(FieldType fieldType, PrefixFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        public PrefixFieldType fieldType() {
            return (PrefixFieldType) super.fieldType();
        }

        FieldType getLuceneFieldType() {
            return fieldType;
        }

        @Override
        protected void parseCreateField(ParseContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object parseSourceValue(Object value, String format) {
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

    static final class ShingleFieldMapper extends FieldMapper {

        ShingleFieldMapper(FieldType fieldType, ShingleFieldType mappedFieldtype) {
            super(mappedFieldtype.name(), fieldType, mappedFieldtype, MultiFields.empty(), CopyTo.empty());
        }

        FieldType getLuceneFieldType() {
            return fieldType;
        }

        @Override
        public ShingleFieldType fieldType() {
            return (ShingleFieldType) super.fieldType();
        }

        @Override
        protected void parseCreateField(ParseContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void mergeOptions(FieldMapper other, List<String> conflicts) {

        }

        @Override
        protected Object parseSourceValue(Object value, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String contentType() {
            return "shingle";
        }
    }

    /**
     * The shingle field type handles phrase queries and delegates prefix and phrase prefix queries to the prefix field
     */
    static class ShingleFieldType extends StringFieldType {
        final int shingleSize;
        PrefixFieldType prefixFieldType;

        ShingleFieldType(String name, int shingleSize, TextSearchInfo textSearchInfo) {
            super(name, true, false, textSearchInfo, Collections.emptyMap());
            this.shingleSize = shingleSize;
        }

        void setPrefixFieldType(PrefixFieldType prefixFieldType) {
            this.prefixFieldType = prefixFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (getTextSearchInfo().hasNorms() == false) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            if (prefixFieldType == null || prefixFieldType.termLengthWithinBounds(value.length()) == false) {
                return super.prefixQuery(value, method, context);
            } else {
                final Query query = prefixFieldType.prefixQuery(value, method, context);
                if (method == null
                    || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                    || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
                    return new ConstantScoreQuery(query);
                } else {
                    return query;
                }
            }
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
            final String prefixFieldName = slop > 0
                ? null
                : prefixFieldType.name();
            return TextFieldMapper.createPhrasePrefixQuery(stream, name(), slop, maxExpansions,
                prefixFieldName, prefixFieldType::termLengthWithinBounds);
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
            if (prefixFieldType != null && prefixFieldType.termLengthWithinBounds(value.length())) {
                return new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixFieldType.name(), indexedValueForSearch(value))), name());
            } else {
                SpanMultiTermQueryWrapper<?> spanMulti =
                    new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term(name(), indexedValueForSearch(value))));
                spanMulti.setRewriteMethod(method);
                return spanMulti;
            }
        }
    }

    private final int maxShingleSize;
    private PrefixFieldMapper prefixField;
    private final ShingleFieldMapper[] shingleFields;

    public SearchAsYouTypeFieldMapper(String simpleName,
                                      FieldType fieldType,
                                      SearchAsYouTypeFieldType mappedFieldType,
                                      CopyTo copyTo,
                                      int maxShingleSize,
                                      PrefixFieldMapper prefixField,
                                      ShingleFieldMapper[] shingleFields) {
        super(simpleName, fieldType, mappedFieldType, MultiFields.empty(), copyTo);
        this.prefixField = prefixField;
        this.shingleFields = shingleFields;
        this.maxShingleSize = maxShingleSize;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        final String value = context.externalValueSet() ? context.externalValue().toString() : context.parser().textOrNull();
        if (value == null) {
            return;
        }

        context.doc().add(new Field(fieldType().name(), value, fieldType));
        for (ShingleFieldMapper subFieldMapper : shingleFields) {
            context.doc().add(new Field(subFieldMapper.fieldType().name(), value, subFieldMapper.getLuceneFieldType()));
        }
        context.doc().add(new Field(prefixField.fieldType().name(), value, prefixField.getLuceneFieldType()));
        if (fieldType.omitNorms()) {
            createFieldNamesField(context);
        }
    }

    @Override
    protected String parseSourceValue(Object value, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        return value.toString();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        final SearchAsYouTypeFieldMapper m = (SearchAsYouTypeFieldMapper) other;
        if (this.shingleFields.length != m.shingleFields.length) {
            conflicts.add("mapper [" + name() + "] has a different [max_shingle_size]");
        } else {
            this.prefixField = (PrefixFieldMapper) this.prefixField.merge(m.prefixField);
            for (int i = 0; i < m.shingleFields.length; i++) {
                this.shingleFields[i] = (ShingleFieldMapper) this.shingleFields[i].merge(m.shingleFields[i]);
            }
        }
        if (Objects.equals(this.fieldType().getTextSearchInfo().getSimilarity(),
            other.fieldType().getTextSearchInfo().getSimilarity()) == false) {
            conflicts.add("mapper [" + name() + "] has different [similarity] settings");
        }
    }

    public static String getShingleFieldName(String parentField, int shingleSize) {
        return parentField + "._" + shingleSize + "gram";
    }

    @Override
    public SearchAsYouTypeFieldType fieldType() {
        return (SearchAsYouTypeFieldType) super.fieldType();
    }

    public int maxShingleSize() {
        return maxShingleSize;
    }

    public PrefixFieldMapper prefixField() {
        return prefixField;
    }

    public ShingleFieldMapper[] shingleFields() {
        return shingleFields;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        doXContentAnalyzers(builder, includeDefaults);
        if (fieldType().getTextSearchInfo().getSimilarity() != null) {
            builder.field("similarity", fieldType().getTextSearchInfo().getSimilarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityService.DEFAULT_SIMILARITY);
        }
        builder.field("max_shingle_size", maxShingleSize);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(prefixField);
        subIterators.addAll(Arrays.asList(shingleFields));
        @SuppressWarnings("unchecked") Iterator<Mapper> concat = Iterators.concat(super.iterator(), subIterators.iterator());
        return concat;
    }

    /**
     * An analyzer wrapper to add a shingle token filter, an edge ngram token filter or both to its wrapped analyzer. When adding an edge
     * ngrams token filter, it also adds a {@link TrailingShingleTokenFilter} to add extra position increments at the end of the stream
     * to induce the shingle token filter to create tokens at the end of the stream smaller than the shingle size
     */
    static class SearchAsYouTypeAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final int shingleSize;
        private final boolean indexPrefixes;

        private SearchAsYouTypeAnalyzer(Analyzer delegate,
                                        int shingleSize,
                                        boolean indexPrefixes) {

            super(delegate.getReuseStrategy());
            this.delegate = Objects.requireNonNull(delegate);
            this.shingleSize = shingleSize;
            this.indexPrefixes = indexPrefixes;
        }

        static SearchAsYouTypeAnalyzer withShingle(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, shingleSize, false);
        }

        static SearchAsYouTypeAnalyzer withShingleAndPrefix(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, shingleSize, true);
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            TokenStream tokenStream = components.getTokenStream();
            if (indexPrefixes) {
                tokenStream = new TrailingShingleTokenFilter(tokenStream, shingleSize - 1);
            }
            tokenStream = new FixedShingleFilter(tokenStream, shingleSize, " ", "");
            if (indexPrefixes) {
                tokenStream = new EdgeNGramTokenFilter(tokenStream, Defaults.MIN_GRAM, Defaults.MAX_GRAM, true);
            }
            return new TokenStreamComponents(components.getSource(), tokenStream);
        }

        public int shingleSize() {
            return shingleSize;
        }

        public boolean indexPrefixes() {
            return indexPrefixes;
        }

        @Override
        public String toString() {
            return "<" + getClass().getCanonicalName() + " shingleSize=[" + shingleSize + "] indexPrefixes=[" + indexPrefixes + "]>";
        }

        private static class TrailingShingleTokenFilter extends TokenFilter {

            private final int extraPositionIncrements;
            private final PositionIncrementAttribute positionIncrementAttribute;

            TrailingShingleTokenFilter(TokenStream input, int extraPositionIncrements) {
                super(input);
                this.extraPositionIncrements = extraPositionIncrements;
                this.positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
            }

            @Override
            public boolean incrementToken() throws IOException {
                return input.incrementToken();
            }

            @Override
            public void end() throws IOException {
                super.end();
                positionIncrementAttribute.setPositionIncrement(extraPositionIncrements);
            }
        }
    }
}
