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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.index.mapper.TypeParsers.nodeIndexOptionValue;

public class SearchAsYouTypeFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "search_as_you_type";
    private static final int LOWEST_MAX_SHINGLE_SIZE = 2;
    private static final int HIGHEST_MAX_SHINGLE_SIZE = 4;

    public static class Defaults {

        public static final int MIN_GRAM = 1;
        public static final int MAX_GRAM = 20;
        public static final int MIN_SHINGLE_SIZE = 2;
        public static final int MAX_SHINGLE_SIZE = 3;

        public static final MappedFieldType FIELD_TYPE = new SearchAsYouTypeFieldType();

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

            final Builder builder = new Builder(name);

            NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer();

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                final Map.Entry<String, Object> entry = iterator.next();
                final String fieldName = entry.getKey();
                final Object fieldNode = entry.getValue();

                if (fieldName.equals("index_options")) {
                    builder.indexOptions(nodeIndexOptionValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("analyzer")) {
                    final String analyzerName = fieldNode.toString();
                    analyzer = parserContext.getIndexAnalyzers().get(analyzerName);
                    if (analyzer == null) {
                        throw new MapperParsingException("analyzer [" + analyzerName + "] not found for field [" + name + "]");
                    }
                    iterator.remove();
                } else if (fieldName.equals("max_shingle_size")) {
                    builder.maxShingleSize(nodeIntegerValue(fieldNode));
                    iterator.remove();
                }
            }

            builder.indexAnalyzer(analyzer);
            builder.searchAnalyzer(analyzer);
            builder.searchQuoteAnalyzer(analyzer); // todo should we be setting this?
            return builder;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, SearchAsYouTypeFieldMapper> {

        private int minShingleSize = Defaults.MIN_SHINGLE_SIZE;
        private int maxShingleSize = Defaults.MAX_SHINGLE_SIZE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder maxShingleSize(int maxShingleSize) {
            if (maxShingleSize < LOWEST_MAX_SHINGLE_SIZE || maxShingleSize > HIGHEST_MAX_SHINGLE_SIZE) {
                throw new MapperParsingException("[max_shingle_size] must be at least [" + LOWEST_MAX_SHINGLE_SIZE + "] and at most [" +
                    HIGHEST_MAX_SHINGLE_SIZE + "], got [" + maxShingleSize + "]");
            }
            this.maxShingleSize = maxShingleSize;
            return builder;
        }

        @Override
        public SearchAsYouTypeFieldType fieldType() {
            return (SearchAsYouTypeFieldType) this.fieldType;
        }

        private static SubFieldMapper buildShinglesAndEdgeNGramsField(String rootFieldName,
                                                                      NamedAnalyzer rootFieldAnalyzer,
                                                                      int shingleSize,
                                                                      Mapper.BuilderContext context) {

            final String name = rootFieldName + "._with_" + shingleSize + "_shingles_and_edge_ngrams";
            final SubFieldType withShinglesAndEdgeNGramsType = new SubFieldType(name, shingleSize, true);

            final SearchAsYouTypeAnalyzer withShinglesAndEdgeNGramsAnalyzer =
                SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(rootFieldAnalyzer.analyzer(), shingleSize);
            withShinglesAndEdgeNGramsType.setIndexAnalyzer(
                new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAndEdgeNGramsAnalyzer));

            final SearchAsYouTypeAnalyzer withShinglesAnalyzer =
                SearchAsYouTypeAnalyzer.withShingles(rootFieldAnalyzer.analyzer(), shingleSize);
            withShinglesAndEdgeNGramsType.setSearchAnalyzer(
                new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

            return new SubFieldMapper(withShinglesAndEdgeNGramsType, context.indexSettings());
        }

        private static SubFieldMapper buildShinglesField(String rootFieldName,
                                                         NamedAnalyzer rootFieldAnalyzer,
                                                         int shingleSize,
                                                         Mapper.BuilderContext context,
                                                         SubFieldMapper edgeNGramsMapper) {

            final String name = rootFieldName + "._with_" + shingleSize + "_shingles";
            final SubFieldType shinglesFieldType = new SubFieldType(name, shingleSize, false, edgeNGramsMapper.fieldType());

            final SearchAsYouTypeAnalyzer withShinglesAnalyzer =
                SearchAsYouTypeAnalyzer.withShingles(rootFieldAnalyzer.analyzer(), shingleSize);

            shinglesFieldType.setIndexAnalyzer(new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));
            shinglesFieldType.setSearchAnalyzer(new NamedAnalyzer(rootFieldAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

            return new SubFieldMapper(shinglesFieldType, context.indexSettings(), edgeNGramsMapper);
        }

        @Override
        public SearchAsYouTypeFieldMapper build(Mapper.BuilderContext context) {
            setupFieldType(context);

            final NamedAnalyzer originalAnalyzer = fieldType().indexAnalyzer();
            if (originalAnalyzer.equals(fieldType().searchAnalyzer()) == false) {
                throw new MapperParsingException("Index and search analyzers must be the same");
            }

            final SubFieldMapper maxShinglesAndEdgeNGramsField =
                buildShinglesAndEdgeNGramsField(name(), originalAnalyzer, maxShingleSize, context);

            final Map<Integer, SubFieldMapper> withShinglesMappers = new HashMap<>();
            for (int shingleSize = minShingleSize; shingleSize <= maxShingleSize; shingleSize++) {
                final SubFieldMapper shingleField = buildShinglesField(
                    name(),
                    originalAnalyzer,
                    shingleSize,
                    context,
                    maxShinglesAndEdgeNGramsField
                );
                withShinglesMappers.put(shingleSize, shingleField);
            }

            final Map<Integer, SubFieldType> withShinglesTypes = new HashMap<>();
            withShinglesMappers.forEach((shingleSize, mapper) -> withShinglesTypes.put(shingleSize, mapper.fieldType()));
            final SubFieldTypes subFieldTypes =
                new SubFieldTypes(minShingleSize, maxShingleSize, withShinglesTypes, maxShinglesAndEdgeNGramsField.fieldType());
            fieldType().setSubFieldTypes(subFieldTypes);

            final SubFieldMappers subFieldMappers =
                new SubFieldMappers(minShingleSize, maxShingleSize, withShinglesMappers, maxShinglesAndEdgeNGramsField);
            return new SearchAsYouTypeFieldMapper(name(), fieldType(), context.indexSettings(), copyTo, subFieldMappers);
        }
    }

    public static class SubFieldType extends StringFieldType {

        private final int shingleSize;
        private final boolean hasEdgeNGrams;
        private final SubFieldType edgeNGramsField;

        SubFieldType(String name, int shingleSize, boolean hasEdgeNGrams) {
            this(name, shingleSize, hasEdgeNGrams, null);
        }

        SubFieldType(String name, int shingleSize, boolean hasEdgeNGrams, @Nullable SubFieldType edgeNGramsField) {
            setTokenized(true);
            setName(name);
            this.shingleSize = shingleSize;
            this.hasEdgeNGrams = hasEdgeNGrams;
            this.edgeNGramsField = edgeNGramsField;
        }

        SubFieldType(SubFieldType other) {
            super(other);
            this.shingleSize = other.shingleSize;
            this.hasEdgeNGrams = other.hasEdgeNGrams;
            if (other.edgeNGramsField != null) {
                this.edgeNGramsField = other.edgeNGramsField.clone();
            } else {
                this.edgeNGramsField = null;
            }
        }

        @Override
        public SubFieldType clone() {
            return new SubFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        public int shingleSize() {
            return shingleSize;
        }

        public boolean hasEdgeNGrams() {
            return hasEdgeNGrams;
        }

        public SubFieldType edgeNGramsField() {
            return edgeNGramsField;
        }

        @Override
        public boolean equals(Object otherObject) {
            if (super.equals(otherObject) == false) {
                return false;
            }
            final SubFieldType other = (SubFieldType) otherObject;
            return Objects.equals(shingleSize, other.shingleSize)
                && Objects.equals(hasEdgeNGrams, other.hasEdgeNGrams)
                && Objects.equals(edgeNGramsField, other.edgeNGramsField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), shingleSize, hasEdgeNGrams, edgeNGramsField);
        }
    }

    public static class SubFieldMapper extends FieldMapper implements ArrayValueMapperParser {

        private final SubFieldMapper edgeNGramsFieldMapper;

        public SubFieldMapper(SubFieldType fieldType, Settings indexSettings) {
            this(fieldType, indexSettings, null);
        }

        public SubFieldMapper(SubFieldType fieldType, Settings indexSettings, @Nullable SubFieldMapper edgeNGramsFieldMapper) {
            super(fieldType.name(), fieldType, fieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
            this.edgeNGramsFieldMapper = edgeNGramsFieldMapper;
        }

        @Override
        public SubFieldType fieldType() {
            return (SubFieldType) super.fieldType();
        }

        @Override
        protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        public SubFieldMapper edgeNGramsFieldMapper() {
            return edgeNGramsFieldMapper;
        }
    }

    public static class SearchAsYouTypeFieldType extends StringFieldType {

        private SubFieldTypes subFieldTypes;

        public SearchAsYouTypeFieldType() {
            this((SubFieldTypes) null);
        }

        public SearchAsYouTypeFieldType(SubFieldTypes subFieldTypes) {
            super();
            setTokenized(true);
            this.subFieldTypes = subFieldTypes;
        }

        public SearchAsYouTypeFieldType(SearchAsYouTypeFieldType other) {
            super(other);
            if (other.subFieldTypes != null) {
                this.subFieldTypes = other.subFieldTypes.clone();
            } else {
                this.subFieldTypes = null;
            }
        }

        public SubFieldTypes getSubFieldTypes() {
            return subFieldTypes;
        }

        public void setSubFieldTypes(SubFieldTypes subFieldTypes) {
            this.subFieldTypes = subFieldTypes;
        }

        @Override
        public SearchAsYouTypeFieldType clone() {
            return new SearchAsYouTypeFieldType(this);
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

    public static class SearchAsYouTypeAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final int shingleSize;
        private final boolean hasEdgeNGrams;

        private SearchAsYouTypeAnalyzer(Analyzer delegate,
                                        int shingleSize,
                                        boolean hasEdgeNGrams) {

            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.shingleSize = shingleSize;
            this.hasEdgeNGrams = hasEdgeNGrams;
        }

        public static SearchAsYouTypeAnalyzer withShingles(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, shingleSize, false);
        }

        public static SearchAsYouTypeAnalyzer withShinglesAndEdgeNGrams(Analyzer delegate, int shingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, shingleSize, true);
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            TokenStream tokenStream = components.getTokenStream();
            tokenStream = new TrailingShingleTokenFilter(tokenStream, shingleSize - 1);
            tokenStream = new FixedShingleFilter(tokenStream, shingleSize, " ", "");
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

        private static class TrailingShingleTokenFilter extends TokenFilter {

            private final int numberOfExtraTrailingPositions;
            private final PositionIncrementAttribute positionIncrementAttribute;

            TrailingShingleTokenFilter(TokenStream input, int numberOfExtraTrailingPositions) {
                super(input);
                this.numberOfExtraTrailingPositions = numberOfExtraTrailingPositions;
                this.positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
            }

            @Override
            public boolean incrementToken() throws IOException {
                return input.incrementToken();
            }

            @Override
            public void end() {
                positionIncrementAttribute.setPositionIncrement(numberOfExtraTrailingPositions);
            }
        }
    }

    public static class SubFields<T> {

        final int minShingleSize;
        final int maxShingleSize;
        final Map<Integer, T> withShingles;
        final T withMaxShinglesAndEdgeNGrams;

        public SubFields(int minShingleSize,
                         int maxShingleSize,
                         Map<Integer, T> withShingles,
                         T withMaxShinglesAndEdgeNGrams) {

            this.minShingleSize = minShingleSize;
            this.maxShingleSize = maxShingleSize;
            this.withShingles = unmodifiableMap(withShingles);
            this.withMaxShinglesAndEdgeNGrams = withMaxShinglesAndEdgeNGrams;
        }

        public Iterable<T> allSubFields() {
            return () -> Stream.concat(withShingles.values().stream(), Stream.of(withMaxShinglesAndEdgeNGrams)).iterator();
        }

        public Iterable<T> shingleOnlySubfields() {
            return () -> withShingles.values().iterator();
        }

        @Override
        public boolean equals(Object otherObject) {
            if (this == otherObject) {
                return true;
            }

            if (otherObject == null || getClass() != otherObject.getClass()) {
                return false;
            }

            final SubFields<?> other = (SubFields<?>) otherObject;
            return Objects.equals(minShingleSize, other.minShingleSize)
                && Objects.equals(maxShingleSize, other.maxShingleSize)
                && Objects.equals(withShingles, other.withShingles)
                && Objects.equals(withMaxShinglesAndEdgeNGrams, other.withMaxShinglesAndEdgeNGrams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(minShingleSize, maxShingleSize, withShingles, withMaxShinglesAndEdgeNGrams);
        }
    }

    public static class SubFieldTypes extends SubFields<SubFieldType> {
        public SubFieldTypes(int minShingleSize,
                             int maxShingleSize,
                             Map<Integer, SubFieldType> withShingles,
                             SubFieldType withMaxShinglesAndEdgeNGrams) {

            super(minShingleSize, maxShingleSize, withShingles, withMaxShinglesAndEdgeNGrams);
        }

        public SubFieldTypes clone() {
            final Map<Integer, SubFieldType> clonedMap = withShingles.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, entry -> entry.getValue().clone()
            ));
            return new SubFieldTypes(minShingleSize, maxShingleSize, clonedMap, withMaxShinglesAndEdgeNGrams.clone());
        }
    }

    public static class SubFieldMappers extends SubFields<SubFieldMapper> {
        public SubFieldMappers(int minShingleSize,
                               int maxShingleSize,
                               Map<Integer, SubFieldMapper> withShingles,
                               SubFieldMapper withMaxShinglesAndEdgeNGrams) {

            super(minShingleSize, maxShingleSize, withShingles, withMaxShinglesAndEdgeNGrams);
        }
    }

    private final SubFieldMappers subFieldMappers;

    public SearchAsYouTypeFieldMapper(String simpleName,
                                      SearchAsYouTypeFieldType fieldType,
                                      Settings indexSettings,
                                      CopyTo copyTo,
                                      SubFieldMappers subFieldMappers) {

        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, MultiFields.empty(), copyTo);
        this.subFieldMappers = subFieldMappers;
    }

    public SubFieldMappers subFieldMappers() {
        return subFieldMappers;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        final String value = context.externalValueSet()
            ? context.externalValue().toString()
            : context.parser().textOrNull();

        if (value == null) {
            return;
        }

        fields.add(new Field(fieldType().name(), value, fieldType()));

        if (fieldType().omitNorms()) {
            createFieldNamesField(context, fields);
        }

        for (SubFieldMapper subFieldMapper : subFieldMappers.allSubFields()) {
            fields.add(new Field(subFieldMapper.fieldType().name(), value, subFieldMapper.fieldType()));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SearchAsYouTypeFieldType fieldType() {
        return (SearchAsYouTypeFieldType) super.fieldType();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", CONTENT_TYPE);
        if (fieldType().indexAnalyzer().name().equals("default") == false) {
            builder.field("analyzer", fieldType().indexAnalyzer().name());
        }
        builder.field("max_shingle_size", subFieldMappers.maxShingleSize);
        builder.endObject();
        return builder;
    }

    /**
     * We intentionally only expose the shingle-only subfields and hide the one with edge ngrams
     */
    @SuppressWarnings("unchecked") // todo what's the right thing to do here?
    @Override
    public Iterator<Mapper> iterator() {
        return Iterators.concat(super.iterator(), subFieldMappers.shingleOnlySubfields().iterator());
    }
}
