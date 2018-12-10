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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.TypeParsers.nodeIndexOptionValue;

/**
 * This class is a container for two field mappers, {@link SearchAsYouTypeFieldMapper} which maps the root field of a search-as-you-type
 * field type, and {@link SuggesterizedFieldMapper} which maps the synthetic fields created by this type
 */
public final class SearchAsYouTypeFieldMappers {

    private static final Logger LOG = LogManager.getLogger(SearchAsYouTypeFieldMappers.class);

    public static final String CONTENT_TYPE = "search_as_you_type";

    public static class Defaults {

        public static final int MIN_GRAM = 1;
        public static final int MAX_GRAM = 20;
        public static final int MAX_SHINGLE_SIZE = 3;

        public static final MappedFieldType FIELD_TYPE = new SuggesterizedFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name,
                                          Map<String, Object> node,
                                          ParserContext parserContext) throws MapperParsingException {

            final SearchAsYouTypeFieldMappers.Builder builder = new SearchAsYouTypeFieldMappers.Builder(name);

            NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer();
            int maxShingleSize = Defaults.MAX_SHINGLE_SIZE;

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                final Map.Entry<String, Object> entry = iterator.next();
                final String fieldName = entry.getKey();
                final Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                } else if (fieldName.equals("index_options")) {
                    builder.indexOptions(nodeIndexOptionValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("analyzer")) {
                    final String analyzerName = fieldNode.toString();
                    analyzer = parserContext.getIndexAnalyzers().get(analyzerName);
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + analyzerName + "] not found for field  [" + name + "]");
                    }
                    iterator.remove();
                } else if (fieldName.equals("max_shingle_size")) {
                    maxShingleSize = XContentMapValues.nodeIntegerValue(fieldNode);
                    iterator.remove();
                }
            }

            builder.indexAnalyzer(analyzer);
            builder.searchAnalyzer(analyzer);
            builder.maxShingleSize(maxShingleSize);

            return builder;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, SearchAsYouTypeFieldMapper> {

        private int maxShingleSize;

        public Builder(String name) {

            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder maxShingleSize(int maxShingleSize) {
            if (maxShingleSize < 2) {
                throw new MapperParsingException("[max_shingle_size] must be at least 2, got [" + maxShingleSize + "]");
            }

            if (maxShingleSize > 5) {
                throw new MapperParsingException("[max_shingle_size] must be at most 5, got [" + maxShingleSize + "]");
            }

            this.maxShingleSize = maxShingleSize;

            return builder;
        }

        @Override
        public SuggesterizedFieldType fieldType() {
            return (SuggesterizedFieldType) this.fieldType;
        }

        @Override
        public SearchAsYouTypeFieldMapper build(Mapper.BuilderContext context) {
            setupFieldType(context);

            final NamedAnalyzer originalAnalyzer = fieldType().indexAnalyzer();
            if (originalAnalyzer.equals(fieldType().searchAnalyzer()) == false) {
                throw new MapperParsingException("Index and search analyzers must be the same");
            }

            final Set<SuggesterizedFieldType> suggesterizedFieldTypes = new HashSet<>();

            final SuggesterizedFieldType withEdgeNgrams = new SuggesterizedFieldType(name() + "._with_edge_ngrams");
            final SearchAsYouTypeAnalyzer wrappedWithEdgeNGrams = SearchAsYouTypeAnalyzer.withEdgeNGrams(originalAnalyzer);
            final SearchAsYouTypeAnalyzer unmodified = SearchAsYouTypeAnalyzer.withNeither(originalAnalyzer);
            withEdgeNgrams.setIndexAnalyzer(new NamedAnalyzer(originalAnalyzer.name(), AnalyzerScope.INDEX, wrappedWithEdgeNGrams));
            withEdgeNgrams.setSearchAnalyzer(new NamedAnalyzer(originalAnalyzer.name(), AnalyzerScope.INDEX, unmodified));
            suggesterizedFieldTypes.add(withEdgeNgrams);

            for (int i = 2; i <= maxShingleSize; i++) {
                final int numberOfShingles = i;
                final SuggesterizedFieldType withShingles = new SuggesterizedFieldType(name() + "._with_" + numberOfShingles + "_shingles");
                final SuggesterizedFieldType withShinglesAndEdgeNGrams = new SuggesterizedFieldType(name() + "._with_" + numberOfShingles +
                    "_shingles_and_edge_ngrams");

                final SearchAsYouTypeAnalyzer withShinglesAnalyzer = SearchAsYouTypeAnalyzer.withShingles(originalAnalyzer, numberOfShingles);
                final SearchAsYouTypeAnalyzer withShinglesAndEdgeNGramsAnalyzer =
                    SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(originalAnalyzer, numberOfShingles);

                withShingles.setIndexAnalyzer(new NamedAnalyzer(originalAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));
                withShingles.setSearchAnalyzer(new NamedAnalyzer(originalAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

                withShinglesAndEdgeNGrams.setIndexAnalyzer(
                    new NamedAnalyzer(originalAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAndEdgeNGramsAnalyzer));
                withShinglesAndEdgeNGrams.setSearchAnalyzer(
                    new NamedAnalyzer(originalAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

                suggesterizedFieldTypes.add(withShingles);
                suggesterizedFieldTypes.add(withShinglesAndEdgeNGrams);
            }

            final Set<SuggesterizedFieldMapper> suggesterizedFieldMappers = suggesterizedFieldTypes.stream()
                .map(suggesterizedFieldType -> new SuggesterizedFieldMapper(suggesterizedFieldType, context.indexSettings()))
                .collect(Collectors.toSet());
            return new SearchAsYouTypeFieldMapper(
                name(),
                fieldType(),
                suggesterizedFieldMappers,
                context.indexSettings(),
                copyTo
            );
        }
    }

    public static class SearchAsYouTypeAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final boolean withShingles;
        private final int shingleSize;
        private final boolean withEdgeNGrams;

        private SearchAsYouTypeAnalyzer(Analyzer delegate,
                                        boolean withShingles,
                                        int shingleSize,
                                        boolean withEdgeNGrams) {

            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.withShingles = withShingles;
            this.shingleSize = shingleSize;
            this.withEdgeNGrams = withEdgeNGrams;
        }

        public static SearchAsYouTypeAnalyzer withNeither(Analyzer delegate) {
            return new SearchAsYouTypeAnalyzer(delegate, false, -1, false);
        }

        public static SearchAsYouTypeAnalyzer withShingles(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, true, shingleSize, false);
        }

        public static SearchAsYouTypeAnalyzer withEdgeNGrams(Analyzer delegate) {
            return new SearchAsYouTypeAnalyzer(delegate, false, -1, true);
        }

        public static SearchAsYouTypeAnalyzer withShinglesAndEdgeNGrams(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, true, shingleSize, true);
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            // TODO we must find a way to add the last unigram term (michael jackson -> jackson)
            TokenStream tokenStream = components.getTokenStream();
            if (withShingles) {
                tokenStream = new FixedShingleFilter(tokenStream, shingleSize);
            }
            if (withEdgeNGrams) {
                tokenStream = new EdgeNGramTokenFilter(tokenStream, Defaults.MIN_GRAM, Defaults.MAX_GRAM, true);
            }
            return new TokenStreamComponents(components.getTokenizer(), tokenStream);
        }

        public boolean isWithEdgeNGrams() {
            return withEdgeNGrams;
        }

        public int getShingleSize() {
            return shingleSize;
        }

        public boolean isWithShingles() {
            return withShingles;
        }
    }

    public static class SuggesterizedFieldType extends StringFieldType {

        SuggesterizedFieldType() {
            setOmitNorms(true);
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            setTokenized(true);
        }

        SuggesterizedFieldType(String name) {
            setName(name);
            setOmitNorms(true);
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS); // todo do we need to always set omitNorms and INdexOptions
            setTokenized(true);
        }

        SuggesterizedFieldType(SuggesterizedFieldType reference) {
            super(reference);
        }

        @Override
        public SuggesterizedFieldType clone() {
            return new SuggesterizedFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    public static class SuggesterizedFieldMapper extends FieldMapper implements ArrayValueMapperParser { // todo better name

        protected SuggesterizedFieldMapper(String simpleName,
                                           MappedFieldType fieldType,
                                           Settings indexSettings,
                                           CopyTo copyTo) {
            super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, MultiFields.empty(), copyTo);
        }

        protected SuggesterizedFieldMapper(SuggesterizedFieldType fieldType, Settings indexSettings) {
            super(fieldType.name(), fieldType, fieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        public SuggesterizedFieldType fieldType() {
            return (SuggesterizedFieldType) super.fieldType();
        }

        void addField(String value, List<IndexableField> fields) {
            fields.add(new Field(fieldType().name(), value, fieldType));
        }

        @Override
        protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }
    }

    public static class SearchAsYouTypeFieldMapper extends SuggesterizedFieldMapper {

        private Set<SuggesterizedFieldMapper> suggesterizedFieldMappers;

        public SearchAsYouTypeFieldMapper(String simpleName,
                                          MappedFieldType fieldType,
                                          Set<SuggesterizedFieldMapper> suggesterizedFieldMappers,
                                          Settings indexSettings,
                                          CopyTo copyTo) {

            super(simpleName, fieldType, indexSettings, copyTo);
            this.suggesterizedFieldMappers = suggesterizedFieldMappers;
        }

        @Override
        protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
            final String value = context.externalValueSet()
                ? context.externalValue().toString()
                : context.parser().textOrNull();

            if (value == null) {
                return;
            }

            Field field = new Field(fieldType().name(), value, fieldType());
            fields.add(field);

            if (fieldType().omitNorms()) {
                createFieldNamesField(context, fields);
            }

            for (SuggesterizedFieldMapper fieldMapper : suggesterizedFieldMappers) {
                fieldMapper.addField(value, fields);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(simpleName());
            builder.field("type", CONTENT_TYPE);
            if (fieldType().indexAnalyzer().name().equals("default") == false) {
                builder.field("analyzer", fieldType().indexAnalyzer().name());
            }
            builder.endObject();
            return builder;
            // todo we should provide more info about the under the hood fields, or at least how they're analyzed
        }

        @SuppressWarnings("unchecked") // todo fix
        @Override
        public Iterator<Mapper> iterator() {
            final List<Mapper> mappers = new ArrayList<>(suggesterizedFieldMappers);
            return Iterators.concat(super.iterator(), mappers.iterator());
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }
    }
}
