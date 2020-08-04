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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TypeParsers.checkNull;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 */
public final class KeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "keyword";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
        public static final boolean EAGER_GLOBAL_ORDINALS = false;
        public static final boolean SPLIT_QUERIES_ON_WHITESPACE = false;
    }

    public static class KeywordField extends Field {

        public KeywordField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
        }

        public KeywordField(String field, BytesRef term) {
            super(field, term, Defaults.FIELD_TYPE);
        }

    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        protected String nullValue = Defaults.NULL_VALUE;
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        private IndexAnalyzers indexAnalyzers;
        private String normalizerName = "default";
        private boolean eagerGlobalOrdinals = Defaults.EAGER_GLOBAL_ORDINALS;
        private boolean splitQueriesOnWhitespace = Defaults.SPLIT_QUERIES_ON_WHITESPACE;
        private SimilarityProvider similarity;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder omitNorms(boolean omitNorms) {
            fieldType.setOmitNorms(omitNorms);
            return builder;
        }

        public void similarity(SimilarityProvider similarity) {
            this.similarity = similarity;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException("The [keyword] field does not support positions, got [index_options]="
                        + indexOptionToString(indexOptions));
            }
            return super.indexOptions(indexOptions);
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return builder;
        }

        public Builder splitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            return builder;
        }

        public Builder normalizer(IndexAnalyzers indexAnalyzers, String name) {
            this.indexAnalyzers = indexAnalyzers;
            this.normalizerName = name;
            return builder;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return builder;
        }

        private KeywordFieldType buildFieldType(BuilderContext context) {
            NamedAnalyzer normalizer = Lucene.KEYWORD_ANALYZER;
            NamedAnalyzer searchAnalyzer = Lucene.KEYWORD_ANALYZER;
            if (normalizerName == null || "default".equals(normalizerName) == false) {
                normalizer = indexAnalyzers.getNormalizer(normalizerName);
                if (normalizer == null) {
                    throw new MapperParsingException("normalizer [" + normalizerName + "] not found for field [" + name + "]");
                }
                if (splitQueriesOnWhitespace) {
                    searchAnalyzer = indexAnalyzers.getWhitespaceNormalizer(normalizerName);
                } else {
                    searchAnalyzer = normalizer;
                }
            }
            else if (splitQueriesOnWhitespace) {
                // TODO should this be a Lucene global analyzer as well?
                searchAnalyzer = new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer());
            }
            return new KeywordFieldType(buildFullName(context), hasDocValues, fieldType,
                eagerGlobalOrdinals, normalizer, searchAnalyzer, similarity, meta, boost);
        }

        @Override
        public KeywordFieldMapper build(BuilderContext context) {
            return new KeywordFieldMapper(name,
                    fieldType, buildFieldType(context), ignoreAbove, splitQueriesOnWhitespace, nullValue,
                    multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                checkNull(propName, propNode);
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("norms")) {
                    TypeParsers.parseNorms(builder, name, propNode);
                    iterator.remove();
                } else if (propName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(propNode, "eager_global_ordinals"));
                    iterator.remove();
                } else if (propName.equals("normalizer")) {
                    if (propNode != null) {
                        builder.normalizer(parserContext.getIndexAnalyzers(), propNode.toString());
                    }
                    iterator.remove();
                } else if (propName.equals("split_queries_on_whitespace")) {
                    builder.splitQueriesOnWhitespace(XContentMapValues.nodeBooleanValue(propNode, "split_queries_on_whitespace"));
                    iterator.remove();
                } else if (propName.equals("similarity")) {
                    SimilarityProvider similarityProvider = TypeParsers.resolveSimilarity(parserContext, name, propNode.toString());
                    builder.similarity(similarityProvider);
                    iterator.remove();
                }
            }
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    public static final class KeywordFieldType extends StringFieldType {

        boolean hasNorms;

        public KeywordFieldType(String name, boolean hasDocValues, FieldType fieldType,
                                boolean eagerGlobalOrdinals, NamedAnalyzer normalizer, NamedAnalyzer searchAnalyzer,
                                SimilarityProvider similarity, Map<String, String> meta, float boost) {
            super(name, fieldType.indexOptions() != IndexOptions.NONE,
                hasDocValues, new TextSearchInfo(fieldType, similarity, searchAnalyzer, searchAnalyzer), meta);
            this.hasNorms = fieldType.omitNorms() == false;
            setEagerGlobalOrdinals(eagerGlobalOrdinals);
            setIndexAnalyzer(normalizer);
            setBoost(boost);
        }

        public KeywordFieldType(String name, boolean isSearchable, boolean hasDocValues, Map<String, String> meta) {
            super(name, isSearchable, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        public KeywordFieldType(String name) {
            this(name, true, Defaults.FIELD_TYPE, false,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, null, Collections.emptyMap(), 1f);
        }

        public KeywordFieldType(String name, NamedAnalyzer analyzer) {
            super(name, true, true, new TextSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer), Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        NamedAnalyzer normalizer() {
            return indexAnalyzer();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else if (hasNorms == false) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                // keyword analyzer with the default attribute source which encodes terms using UTF8
                // in that case we skip normalization, which may be slow if there many terms need to
                // parse (eg. large terms query) since Analyzer.normalize involves things like creating
                // attributes through reflection
                // This if statement will be used whenever a normalizer is NOT configured
                return super.indexedValueForSearch(value);
            }

            if (value == null) {
                return null;
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return getTextSearchInfo().getSearchAnalyzer().normalize(name(), value.toString());
        }
    }

    private int ignoreAbove;
    private boolean splitQueriesOnWhitespace;
    private String nullValue;

    protected KeywordFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                 int ignoreAbove, boolean splitQueriesOnWhitespace, String nullValue,
                                 MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.ignoreAbove = ignoreAbove;
        this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
        this.nullValue = nullValue;
    }

    /** Values that have more chars than the return value of this method will
     *  be skipped at parsing time. */
    public int ignoreAbove() {
        return ignoreAbove;
    }

    @Override
    protected KeywordFieldMapper clone() {
        return (KeywordFieldMapper) super.clone();
    }

    @Override
    public KeywordFieldType fieldType() {
        return (KeywordFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = nullValue;
            } else {
                value =  parser.textOrNull();
            }
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
            value = normalizeValue(normalizer, value);
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored())  {
            Field field = new KeywordField(fieldType().name(), binaryValue, fieldType);
            context.doc().add(field);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
        }

        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        }
    }

    private String normalizeValue(NamedAnalyzer normalizer, String value) throws IOException {
        try (TokenStream ts = normalizer.tokenStream(name(), value)) {
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            if (ts.incrementToken() == false) {
                throw new IllegalStateException("The normalization token stream is "
                    + "expected to produce exactly 1 token, but got 0 for analyzer "
                    + normalizer + " and input \"" + value + "\"");
            }
            final String newValue = termAtt.toString();
            if (ts.incrementToken()) {
                throw new IllegalStateException("The normalization token stream is "
                    + "expected to produce exactly 1 token, but got 2+ for analyzer "
                    + normalizer + " and input \"" + value + "\"");
            }
            ts.end();
            return newValue;
        }
    }

    @Override
    protected String parseSourceValue(Object value, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }

        String keywordValue = value.toString();
        if (keywordValue.length() > ignoreAbove) {
            return null;
        }

        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer == null) {
            return keywordValue;
        }

        try {
            return normalizeValue(normalizer, keywordValue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected String nullValue() {
        return nullValue;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        KeywordFieldMapper k = (KeywordFieldMapper) other;
        if (Objects.equals(fieldType().indexAnalyzer(), k.fieldType().indexAnalyzer()) == false) {
            conflicts.add("mapper [" + name() + "] has different [normalizer]");
        }
        if (Objects.equals(mappedFieldType.getTextSearchInfo().getSimilarity(),
            k.fieldType().getTextSearchInfo().getSimilarity()) == false) {
            conflicts.add("mapper [" + name() + "] has different [similarity]");
        }
        this.ignoreAbove = k.ignoreAbove;
        this.splitQueriesOnWhitespace = k.splitQueriesOnWhitespace;
        this.fieldType().setBoost(k.fieldType().boost());
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (fieldType.indexOptions() != IndexOptions.NONE
            && (includeDefaults || fieldType.indexOptions() != Defaults.FIELD_TYPE.indexOptions())) {
            builder.field("index_options", indexOptionToString(fieldType.indexOptions()));
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
        if (includeDefaults || fieldType.omitNorms() != Defaults.FIELD_TYPE.omitNorms()) {
            builder.field("norms", fieldType.omitNorms() == false);
        }
        if (includeDefaults || fieldType().eagerGlobalOrdinals() != false) {
            builder.field("eager_global_ordinals", fieldType().eagerGlobalOrdinals());
        }

        if (fieldType().normalizer() != null && fieldType().normalizer() != Lucene.KEYWORD_ANALYZER) {
            builder.field("normalizer", fieldType().normalizer().name());
        }
        else if (includeDefaults) {
            builder.field("normalizer", "default");
        }

        if (fieldType().getTextSearchInfo().getSimilarity() != null) {
            builder.field("similarity", fieldType().getTextSearchInfo().getSimilarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityService.DEFAULT_SIMILARITY);
        }

        if (includeDefaults || splitQueriesOnWhitespace) {
            builder.field("split_queries_on_whitespace", splitQueriesOnWhitespace);
        }
    }
}
