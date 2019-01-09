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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
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

        private static SuggesterizedFieldMapper buildShinglesAndEdgeNGramsField(String rootFieldName,
                                                NamedAnalyzer rootFieldAnalyzer,
                                                int numberOfShingles,
                                                Mapper.BuilderContext context) {

            final SuggesterizedFieldType withShinglesAndEdgeNGramsType = new SuggesterizedFieldType(
                rootFieldName + "._with_" + numberOfShingles + "_shingles_and_edge_ngrams", true, numberOfShingles, true);

            final SearchAsYouTypeAnalyzer withShinglesAnalyzer =
                SearchAsYouTypeAnalyzer.withShingles(rootFieldAnalyzer.analyzer(), numberOfShingles);
            final SearchAsYouTypeAnalyzer withShinglesAndEdgeNGramsAnalyzer =
                SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(rootFieldAnalyzer.analyzer(), numberOfShingles);

            withShinglesAndEdgeNGramsType.setIndexAnalyzer(
                new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAndEdgeNGramsAnalyzer));
            withShinglesAndEdgeNGramsType.setSearchAnalyzer(
                new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

            final SuggesterizedFieldMapper withShinglesAndEdgeNGramsMapper =
                new SuggesterizedFieldMapper(withShinglesAndEdgeNGramsType, context.indexSettings());

            return withShinglesAndEdgeNGramsMapper;
        }

        private static SuggesterizedFieldMapper buildShinglesField(String rootFieldName,
                                                                   NamedAnalyzer rootFieldAnalyzer,
                                                                   int numberOfShingles,
                                                                   Mapper.BuilderContext context) {

            return buildShinglesField(rootFieldName, rootFieldAnalyzer, numberOfShingles, context, null);
        }


        private static SuggesterizedFieldMapper buildShinglesField(String rootFieldName,
                                                                   NamedAnalyzer rootFieldAnalyzer,
                                                                   int numberOfShingles,
                                                                   Mapper.BuilderContext context,
                                                                   SuggesterizedFieldMapper edgeNGramsMapper) {

            final String subfieldName = rootFieldName + "._with_" + numberOfShingles + "_shingles";
            final SuggesterizedFieldType shinglesFieldType;
            if (edgeNGramsMapper == null) {
                shinglesFieldType = new SuggesterizedFieldType(subfieldName, true, numberOfShingles, false);
            } else {
                shinglesFieldType = new SuggesterizedFieldType(subfieldName, true, numberOfShingles, false, edgeNGramsMapper.fieldType());
            }

            final SearchAsYouTypeAnalyzer withShinglesAnalyzer =
                SearchAsYouTypeAnalyzer.withShingles(rootFieldAnalyzer.analyzer(), numberOfShingles);

            shinglesFieldType.setIndexAnalyzer(new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));
            shinglesFieldType.setSearchAnalyzer(new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

            final SuggesterizedFieldMapper shinglesFieldMapper;
            if (edgeNGramsMapper == null) {
                shinglesFieldMapper = new SuggesterizedFieldMapper(shinglesFieldType, context.indexSettings());
            } else {
                shinglesFieldMapper = new SuggesterizedFieldMapper(shinglesFieldType, context.indexSettings(), edgeNGramsMapper);
            }

            return shinglesFieldMapper;

        }

        @Override
        public SearchAsYouTypeFieldMapper build(Mapper.BuilderContext context) {
            setupFieldType(context);

            final NamedAnalyzer originalAnalyzer = fieldType().indexAnalyzer();
            if (originalAnalyzer.equals(fieldType().searchAnalyzer()) == false) {
                throw new MapperParsingException("Index and search analyzers must be the same");
            }

            final Map<Integer, SuggesterizedFieldMapper> withShinglesMappers = new HashMap<>();

            // todo does it make sense to use ShingleFilter instead of FixedSHingleFilter and collapse all the shingle-without-edge-ngrams
            // fields into one field to reduce the number of fields further - we'd get the same tokens and would have exactly 3
            // fields (root, variable sized singles, max-fixed-sized-and-edge-ngrams)
            for (int numberOfShingles = 2; numberOfShingles < maxShingleSize; numberOfShingles++) {
                final SuggesterizedFieldMapper shingleField = buildShinglesField(name(), originalAnalyzer, numberOfShingles, context);
                withShinglesMappers.put(numberOfShingles, shingleField);
            }

            final SuggesterizedFieldMapper maxShinglesAndEdgeNGramsField =
                buildShinglesAndEdgeNGramsField(name(), originalAnalyzer, maxShingleSize, context);
            final SuggesterizedFieldMapper maxShinglesField =
                buildShinglesField(name(), originalAnalyzer, maxShingleSize, context, maxShinglesAndEdgeNGramsField);
            withShinglesMappers.put(maxShingleSize, maxShinglesField);

            final SuggesterizedFields suggesterizedFields = new SuggesterizedFields(withShinglesMappers, maxShinglesAndEdgeNGramsField);

            return new SearchAsYouTypeFieldMapper(
                name(),
                fieldType(),
                context.indexSettings(),
                copyTo,
                suggesterizedFields
            );
        }
    }

    public static final class SuggesterizedFields implements Iterable<SuggesterizedFieldMapper> {
        final Map<Integer, SuggesterizedFieldMapper> withShingles;
        final SuggesterizedFieldMapper withMaxShinglesAndEdgeNGrams;

        public SuggesterizedFields(Map<Integer, SuggesterizedFieldMapper> withShingles,
                                   SuggesterizedFieldMapper withMaxShinglesAndEdgeNGrams) {

            this.withShingles = unmodifiableMap(withShingles);
            this.withMaxShinglesAndEdgeNGrams = withMaxShinglesAndEdgeNGrams;
        }

        @Override
        public Iterator<SuggesterizedFieldMapper> iterator() {
            return Stream.concat(withShingles.values().stream(), Stream.of(withMaxShinglesAndEdgeNGrams)).iterator();
        }
    }

    public static class SearchAsYouTypeAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final boolean hasShingles;
        private final int shingleSize;
        private final boolean hasEdgeNGrams;

        private SearchAsYouTypeAnalyzer(Analyzer delegate,
                                        boolean hasShingles,
                                        int shingleSize,
                                        boolean hasEdgeNGrams) {

            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.hasShingles = hasShingles;
            this.shingleSize = shingleSize;
            this.hasEdgeNGrams = hasEdgeNGrams;
        }

        // todo this can probably be removed
        public static SearchAsYouTypeAnalyzer withNeither(Analyzer delegate) {
            return new SearchAsYouTypeAnalyzer(delegate, false, -1, false);
        }

        public static SearchAsYouTypeAnalyzer withShingles(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, true, shingleSize, false);
        }

        // todo this can probably be removed
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
            if (hasShingles) {
                tokenStream = new FixedShingleFilter(tokenStream, shingleSize);
            }
            if (hasEdgeNGrams) {
                tokenStream = new EdgeNGramTokenFilter(tokenStream, Defaults.MIN_GRAM, Defaults.MAX_GRAM, true);
            }
            return new TokenStreamComponents(components.getSource(), tokenStream);
        }

        public boolean hasEdgeNGrams() {
            return hasEdgeNGrams;
        }

        public int shingleSize() {
            return shingleSize;
        }

        public boolean hasShingles() {
            return hasShingles;
        }
    }

    public static class SuggesterizedFieldType extends StringFieldType {

        private final boolean hasShingles;
        private final int shingleSize;
        private final boolean hasEdgeNGrams;
        private final SuggesterizedFieldType withEdgeNGramsField;

        SuggesterizedFieldType() {
            this(null, false, -1, false);
        }

        SuggesterizedFieldType(String name, boolean hasShingles, int shingleSize, boolean hasEdgeNGrams) {
            this(name, hasShingles, shingleSize, hasEdgeNGrams, null);
        }

        SuggesterizedFieldType(String name,
                               boolean hasShingles,
                               int shingleSize,
                               boolean hasEdgeNGrams,
                               SuggesterizedFieldType withEdgeNGramsField) {

            setOmitNorms(true);
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            setTokenized(true);

            setName(name);
            this.hasShingles = hasShingles;
            this.shingleSize = shingleSize;
            this.hasEdgeNGrams = hasEdgeNGrams;
            this.withEdgeNGramsField = withEdgeNGramsField;
        }

        SuggesterizedFieldType(SuggesterizedFieldType reference) {
            super(reference);

            this.hasShingles = reference.hasShingles;
            this.shingleSize = reference.shingleSize;
            this.hasEdgeNGrams = reference.hasEdgeNGrams;
            if (reference.withEdgeNGramsField != null) {
                this.withEdgeNGramsField = reference.withEdgeNGramsField.clone();
            } else {
                this.withEdgeNGramsField = null;
            }
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

        public boolean hasShingles() {
            return hasShingles;
        }

        public int shingleSize() {
            return shingleSize;
        }

        public boolean hasEdgeNGrams() {
            return hasEdgeNGrams;
        }

        public SuggesterizedFieldType withEdgeNGramsField() {
            return withEdgeNGramsField;
        }

        @Override
        public boolean equals(Object otherObject) {
            if (super.equals(otherObject) == false) {
                return false;
            }
            final SuggesterizedFieldType other = (SuggesterizedFieldType) otherObject;
            return Objects.equals(this.hasShingles, other.hasShingles)
                && Objects.equals(this.shingleSize, other.shingleSize)
                && Objects.equals(this.hasEdgeNGrams, other.hasEdgeNGrams)
                && Objects.equals(this.withEdgeNGramsField, other.withEdgeNGramsField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), hasShingles, shingleSize, hasEdgeNGrams, withEdgeNGramsField);
        }
    }

    public static class SuggesterizedFieldMapper extends FieldMapper implements ArrayValueMapperParser { // todo better name

        private final SuggesterizedFieldMapper withEdgeNGramsField;

        protected SuggesterizedFieldMapper(SuggesterizedFieldType fieldType, Settings indexSettings) {

            this(fieldType, indexSettings, null);
        }

        protected SuggesterizedFieldMapper(SuggesterizedFieldType fieldType,
                                           Settings indexSettings,
                                           SuggesterizedFieldMapper withEdgeNGramsField) {

            this(fieldType.name(), fieldType, indexSettings, CopyTo.empty(), withEdgeNGramsField);
        }

        protected SuggesterizedFieldMapper(String simpleName, MappedFieldType fieldType, Settings indexSettings, CopyTo copyTo) {

            this(simpleName, fieldType, indexSettings, copyTo, null);
        }

        private SuggesterizedFieldMapper(String simpleName,
                                         MappedFieldType fieldType,
                                         Settings indexSettings,
                                         CopyTo copyTo,
                                         SuggesterizedFieldMapper withEdgeNGramsField) {

            super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, MultiFields.empty(), copyTo);

            this.withEdgeNGramsField = withEdgeNGramsField;
        }

        public SuggesterizedFieldMapper withEdgeNGramsField() {
            return withEdgeNGramsField;
        }

        @Override
        public SuggesterizedFieldType fieldType() {
            return (SuggesterizedFieldType) super.fieldType();
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

        private final SuggesterizedFields suggesterizedFields;

        public SearchAsYouTypeFieldMapper(String simpleName,
                                          MappedFieldType fieldType,
                                          Settings indexSettings,
                                          CopyTo copyTo,
                                          SuggesterizedFields suggesterizedFields) {

            super(simpleName, fieldType, indexSettings, copyTo);

            this.suggesterizedFields = suggesterizedFields;
        }

        SuggesterizedFieldMapper subfield(int shingleSize, boolean edgeNGrams) {
            final SuggesterizedFieldMapper subfieldMapper = suggesterizedFields.withShingles.get(shingleSize);
            if (subfieldMapper == null) {
                throw new IllegalArgumentException("No subfields with [" + shingleSize + "] shingles");
            }
            if (edgeNGrams) {
                if (subfieldMapper.withEdgeNGramsField == null) {
                    throw new IllegalArgumentException("No subfield with [" + shingleSize + "] shingles and edge ngrams");
                } else {
                    return subfieldMapper.withEdgeNGramsField;
                }
            } else {
                return subfieldMapper;
            }
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

            for (SuggesterizedFieldMapper fieldMapper : suggesterizedFields) {
                fields.add(new Field(fieldMapper.fieldType().name(), value, fieldMapper.fieldType()));
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
            return Iterators.concat(super.iterator(), suggesterizedFields.iterator());
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }
    }
}
