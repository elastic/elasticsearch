/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

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
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType.hasGaps;

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
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

    private static Builder builder(FieldMapper in) {
        return ((SearchAsYouTypeFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> index = Parameter.indexParam(m -> builder(m).index.get(), true);
        private final Parameter<Boolean> store = Parameter.storeParam(m -> builder(m).store.get(), false);

        // This is only here because for some reason the initial impl of this always serialized
        // `doc_values=false`, even though it cannot be set; and so we need to continue
        // serializing it forever because of mapper assertions in mixed clusters.
        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> false, false).addValidator(v -> {
            if (v) {
                throw new MapperParsingException("Cannot set [doc_values] on field of type [search_as_you_type]");
            }
        }).alwaysSerialize();

        private final Parameter<Integer> maxShingleSize = Parameter.intParam(
            "max_shingle_size",
            false,
            m -> builder(m).maxShingleSize.get(),
            Defaults.MAX_SHINGLE_SIZE
        ).addValidator(v -> {
            if (v < MAX_SHINGLE_SIZE_LOWER_BOUND || v > MAX_SHINGLE_SIZE_UPPER_BOUND) {
                throw new MapperParsingException(
                    "[max_shingle_size] must be at least ["
                        + MAX_SHINGLE_SIZE_LOWER_BOUND
                        + "] and at most "
                        + "["
                        + MAX_SHINGLE_SIZE_UPPER_BOUND
                        + "], got ["
                        + v
                        + "]"
                );
            }
        }).alwaysSerialize();

        final TextParams.Analyzers analyzers;
        final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> builder(m).similarity.get());

        final Parameter<String> indexOptions = TextParams.textIndexOptions(m -> builder(m).indexOptions.get());
        final Parameter<Boolean> norms = TextParams.norms(true, m -> builder(m).norms.get());
        final Parameter<String> termVectors = TextParams.termVectors(m -> builder(m).termVectors.get());

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final IndexVersion indexCreatedVersion;

        public Builder(String name, IndexVersion indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> builder(m).analyzers.getIndexAnalyzer(),
                m -> builder(m).analyzers.positionIncrementGap.getValue(),
                indexCreatedVersion
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                index,
                store,
                docValues,
                maxShingleSize,
                analyzers.indexAnalyzer,
                analyzers.searchAnalyzer,
                analyzers.searchQuoteAnalyzer,
                similarity,
                indexOptions,
                norms,
                termVectors,
                meta };
        }

        @Override
        public SearchAsYouTypeFieldMapper build(MapperBuilderContext context) {

            FieldType fieldType = new FieldType();
            fieldType.setIndexOptions(TextParams.toIndexOptions(index.getValue(), indexOptions.getValue()));
            fieldType.setOmitNorms(norms.getValue() == false);
            fieldType.setStored(store.getValue());
            TextParams.setTermVectorParams(termVectors.getValue(), fieldType);

            Map<String, NamedAnalyzer> indexAnalyzers = new HashMap<>();

            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();

            SearchAsYouTypeFieldType ft = new SearchAsYouTypeFieldType(
                context.buildFullName(name),
                fieldType,
                similarity.getValue(),
                analyzers.getSearchAnalyzer(),
                analyzers.getSearchQuoteAnalyzer(),
                meta.getValue()
            );

            indexAnalyzers.put(ft.name(), indexAnalyzer);

            // set up the prefix field
            FieldType prefixft = new FieldType();
            prefixft.setIndexOptions(fieldType.indexOptions());
            prefixft.setOmitNorms(true);
            prefixft.setStored(false);
            final String fullName = context.buildFullName(name);
            // wrap the root field's index analyzer with shingles and edge ngrams
            final Analyzer prefixIndexWrapper = SearchAsYouTypeAnalyzer.withShingleAndPrefix(
                indexAnalyzer.analyzer(),
                maxShingleSize.getValue()
            );
            // wrap the root field's search analyzer with only shingles
            final NamedAnalyzer prefixSearchWrapper = new NamedAnalyzer(
                searchAnalyzer.name(),
                searchAnalyzer.scope(),
                SearchAsYouTypeAnalyzer.withShingle(searchAnalyzer.analyzer(), maxShingleSize.getValue())
            );
            // don't wrap the root field's search quote analyzer as prefix field doesn't support phrase queries
            TextSearchInfo prefixSearchInfo = new TextSearchInfo(prefixft, similarity.getValue(), prefixSearchWrapper, searchAnalyzer);
            final PrefixFieldType prefixFieldType = new PrefixFieldType(fullName, prefixSearchInfo, Defaults.MIN_GRAM, Defaults.MAX_GRAM);
            final NamedAnalyzer prefixAnalyzer = new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, prefixIndexWrapper);
            final PrefixFieldMapper prefixFieldMapper = new PrefixFieldMapper(prefixft, prefixFieldType);
            indexAnalyzers.put(prefixFieldType.name(), prefixAnalyzer);

            // set up the shingle fields
            final ShingleFieldMapper[] shingleFieldMappers = new ShingleFieldMapper[maxShingleSize.getValue() - 1];
            final ShingleFieldType[] shingleFieldTypes = new ShingleFieldType[maxShingleSize.getValue() - 1];
            for (int i = 0; i < shingleFieldMappers.length; i++) {
                final int shingleSize = i + 2;
                FieldType shingleft = new FieldType(fieldType);
                shingleft.setStored(false);
                String fieldName = getShingleFieldName(context.buildFullName(name), shingleSize);
                // wrap the root field's index, search, and search quote analyzers with shingles
                final SearchAsYouTypeAnalyzer shingleIndexWrapper = SearchAsYouTypeAnalyzer.withShingle(
                    indexAnalyzer.analyzer(),
                    shingleSize
                );
                final NamedAnalyzer shingleSearchWrapper = new NamedAnalyzer(
                    searchAnalyzer.name(),
                    searchAnalyzer.scope(),
                    SearchAsYouTypeAnalyzer.withShingle(searchAnalyzer.analyzer(), shingleSize)
                );
                final NamedAnalyzer shingleSearchQuoteWrapper = new NamedAnalyzer(
                    searchAnalyzer.name(),
                    searchAnalyzer.scope(),
                    SearchAsYouTypeAnalyzer.withShingle(searchAnalyzer.analyzer(), shingleSize)
                );
                TextSearchInfo textSearchInfo = new TextSearchInfo(
                    shingleft,
                    similarity.getValue(),
                    shingleSearchWrapper,
                    shingleSearchQuoteWrapper
                );
                final ShingleFieldType shingleFieldType = new ShingleFieldType(fieldName, shingleSize, textSearchInfo);
                shingleFieldType.setPrefixFieldType(prefixFieldType);
                shingleFieldTypes[i] = shingleFieldType;
                NamedAnalyzer shingleAnalyzer = new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, shingleIndexWrapper);
                shingleFieldMappers[i] = new ShingleFieldMapper(shingleft, shingleFieldType);
                indexAnalyzers.put(shingleFieldType.name(), shingleAnalyzer);
            }
            ft.setPrefixField(prefixFieldType);
            ft.setShingleFields(shingleFieldTypes);
            return new SearchAsYouTypeFieldMapper(
                name,
                ft,
                copyTo.build(),
                indexAnalyzers,
                prefixFieldMapper,
                shingleFieldMappers,
                multiFieldsBuilder.build(this, context),
                this
            );
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

        final FieldType fieldType;
        PrefixFieldType prefixField;
        ShingleFieldType[] shingleFields = new ShingleFieldType[0];

        SearchAsYouTypeFieldType(
            String name,
            FieldType fieldType,
            SimilarityProvider similarity,
            NamedAnalyzer searchAnalyzer,
            NamedAnalyzer searchQuoteAnalyzer,
            Map<String, String> meta
        ) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                fieldType.stored(),
                false,
                new TextSearchInfo(fieldType, similarity, searchAnalyzer, searchQuoteAnalyzer),
                meta
            );
            this.fieldType = fieldType;
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
            if (prefixField == null || prefixField.termLengthWithinBounds(value.length()) == false) {
                return super.prefixQuery(value, method, caseInsensitive, context);
            } else {
                final Query query = prefixField.prefixQuery(value, method, caseInsensitive, context);
                if (method == null
                    || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                    || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE
                    || method == MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE) {
                    return new ConstantScoreQuery(query);
                } else {
                    return query;
                }
            }
        }

        private void checkForPositions() {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalStateException("field:[" + name() + "] was indexed without position data; cannot run PhraseQuery");
            }
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
            throws IOException {
            checkForPositions();
            int numPos = countPosition(stream);
            if (shingleFields.length == 0 || slop > 0 || hasGaps(stream) || numPos <= 1) {
                return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
            }
            final ShingleFieldType shingleField = shingleFieldForPositions(numPos);
            stream = new FixedShingleFilter(stream, shingleField.shingleSize);
            return shingleField.phraseQuery(stream, 0, true, context);
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
            throws IOException {
            checkForPositions();
            int numPos = countPosition(stream);
            if (shingleFields.length == 0 || slop > 0 || hasGaps(stream) || numPos <= 1) {
                return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
            }
            final ShingleFieldType shingleField = shingleFieldForPositions(numPos);
            stream = new FixedShingleFilter(stream, shingleField.shingleSize);
            return shingleField.multiPhraseQuery(stream, 0, true, context);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
            int numPos = countPosition(stream);
            if (numPos > 1) {
                checkForPositions();
            }
            if (shingleFields.length == 0 || slop > 0 || hasGaps(stream) || numPos <= 1) {
                return TextFieldMapper.createPhrasePrefixQuery(stream, name(), slop, maxExpansions, null, null);
            }
            final ShingleFieldType shingleField = shingleFieldForPositions(numPos);
            stream = new FixedShingleFilter(stream, shingleField.shingleSize);
            return shingleField.phrasePrefixQuery(stream, 0, maxExpansions, context);
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
            if (prefixField != null && prefixField.termLengthWithinBounds(value.length())) {
                return new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixField.name(), indexedValueForSearch(value))), name());
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
            super(parentField + PREFIX_FIELD_SUFFIX, true, false, false, textSearchInfo, Collections.emptyMap());
            this.minChars = minChars;
            this.maxChars = maxChars;
            this.parentField = parentField;
        }

        boolean termLengthWithinBounds(int length) {
            return length >= minChars - 1 && length <= maxChars;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return false;
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
            automata.add(Automata.makeString(value));
            for (int i = value.length(); i < minChars; i++) {
                automata.add(Automata.makeAnyChar());
            }
            Automaton automaton = Operations.concatenate(automata);
            AutomatonQuery query = method == null
                ? new AutomatonQuery(new Term(name(), value + "*"), automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, false)
                : new AutomatonQuery(new Term(name(), value + "*"), automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, false, method);
            return new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(parentField, value)), BooleanClause.Occur.SHOULD)
                .build();
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Because this internal field is modelled as a multi-field, SourceValueFetcher will look up its
            // parent field in _source. So we don't need to use the parent field name here.
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public String typeName() {
            return "prefix";
        }

        @Override
        public String toString() {
            return super.toString() + ",prefixChars=" + minChars + ":" + maxChars;
        }
    }

    static final class PrefixFieldMapper extends FieldMapper {

        final FieldType fieldType;

        PrefixFieldMapper(FieldType fieldType, PrefixFieldType mappedFieldType) {
            super(mappedFieldType.name(), mappedFieldType, MultiFields.empty(), CopyTo.empty());
            this.fieldType = fieldType;
        }

        @Override
        public PrefixFieldType fieldType() {
            return (PrefixFieldType) super.fieldType();
        }

        FieldType getLuceneFieldType() {
            return fieldType;
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder getMergeBuilder() {
            return null;
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

        private final FieldType fieldType;

        ShingleFieldMapper(FieldType fieldType, ShingleFieldType mappedFieldtype) {
            super(mappedFieldtype.name(), mappedFieldtype, MultiFields.empty(), CopyTo.empty());
            this.fieldType = fieldType;
        }

        FieldType getLuceneFieldType() {
            return fieldType;
        }

        @Override
        public ShingleFieldType fieldType() {
            return (ShingleFieldType) super.fieldType();
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder getMergeBuilder() {
            return null;
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
            super(name, true, false, false, textSearchInfo, Collections.emptyMap());
            this.shingleSize = shingleSize;
        }

        void setPrefixFieldType(PrefixFieldType prefixFieldType) {
            this.prefixFieldType = prefixFieldType;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return false;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Because this internal field is modelled as a multi-field, SourceValueFetcher will look up its
            // parent field in _source. So we don't need to use the parent field name here.
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query prefixQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            if (prefixFieldType == null || prefixFieldType.termLengthWithinBounds(value.length()) == false) {
                return super.prefixQuery(value, method, caseInsensitive, context);
            } else {
                final Query query = prefixFieldType.prefixQuery(value, method, caseInsensitive, context);
                if (method == null
                    || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                    || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE
                    || method == MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE) {
                    return new ConstantScoreQuery(query);
                } else {
                    return query;
                }
            }
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
            throws IOException {
            return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, SearchExecutionContext context)
            throws IOException {
            return TextFieldMapper.createPhraseQuery(stream, name(), slop, enablePositionIncrements);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, SearchExecutionContext context) throws IOException {
            final String prefixFieldName = slop > 0 ? null : prefixFieldType.name();
            return TextFieldMapper.createPhrasePrefixQuery(
                stream,
                name(),
                slop,
                maxExpansions,
                prefixFieldName,
                prefixFieldType::termLengthWithinBounds
            );
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, SearchExecutionContext context) {
            if (prefixFieldType != null && prefixFieldType.termLengthWithinBounds(value.length())) {
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
    }

    private final int maxShingleSize;
    private final PrefixFieldMapper prefixField;
    private final ShingleFieldMapper[] shingleFields;
    private final Builder builder;

    private final Map<String, NamedAnalyzer> indexAnalyzers;

    public SearchAsYouTypeFieldMapper(
        String simpleName,
        SearchAsYouTypeFieldType mappedFieldType,
        CopyTo copyTo,
        Map<String, NamedAnalyzer> indexAnalyzers,
        PrefixFieldMapper prefixField,
        ShingleFieldMapper[] shingleFields,
        MultiFields multiFields,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, false, null);
        this.prefixField = prefixField;
        this.shingleFields = shingleFields;
        this.maxShingleSize = builder.maxShingleSize.getValue();
        this.builder = builder;
        this.indexAnalyzers = Map.copyOf(indexAnalyzers);
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return indexAnalyzers;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        if (value == null) {
            return;
        }

        if (this.builder.index.get() == false && this.builder.store.get() == false) {
            return;
        }

        context.doc().add(new Field(fieldType().name(), value, fieldType().fieldType));
        if (this.builder.index.get()) {
            for (ShingleFieldMapper subFieldMapper : shingleFields) {
                context.doc().add(new Field(subFieldMapper.fieldType().name(), value, subFieldMapper.getLuceneFieldType()));
            }
            context.doc().add(new Field(prefixField.fieldType().name(), value, prefixField.getLuceneFieldType()));
        }
        if (fieldType().fieldType.omitNorms()) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), builder.indexCreatedVersion, builder.analyzers.indexAnalyzers).init(this);
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
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(prefixField);
        subIterators.addAll(Arrays.asList(shingleFields));
        @SuppressWarnings("unchecked")
        Iterator<Mapper> concat = Iterators.concat(super.iterator(), subIterators.iterator());
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

        private SearchAsYouTypeAnalyzer(Analyzer delegate, int shingleSize, boolean indexPrefixes) {

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
