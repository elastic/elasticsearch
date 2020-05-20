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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 */
public final class KeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "keyword";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new KeywordFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        protected String nullValue = Defaults.NULL_VALUE;
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        private IndexAnalyzers indexAnalyzers;
        private String normalizerName;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public KeywordFieldType fieldType() {
            return (KeywordFieldType) super.fieldType();
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
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        public Builder splitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            fieldType().setSplitQueriesOnWhitespace(splitQueriesOnWhitespace);
            return builder;
        }

        public Builder normalizer(IndexAnalyzers indexAnalyzers, String name) {
            this.indexAnalyzers = indexAnalyzers;
            this.normalizerName = name;
            return builder;
        }

        @Override
        public KeywordFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            if (normalizerName != null) {
                NamedAnalyzer normalizer = indexAnalyzers.getNormalizer(normalizerName);
                if (normalizer == null) {
                    throw new MapperParsingException("normalizer [" + normalizerName + "] not found for field [" + name + "]");
                }
                fieldType().setNormalizer(normalizer);
                final NamedAnalyzer searchAnalyzer;
                if (fieldType().splitQueriesOnWhitespace) {
                    searchAnalyzer = indexAnalyzers.getWhitespaceNormalizer(normalizerName);
                } else {
                    searchAnalyzer = normalizer;
                }
                fieldType().setSearchAnalyzer(searchAnalyzer);
            } else if (fieldType().splitQueriesOnWhitespace) {
                fieldType().setSearchAnalyzer(new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer()));
            }
            return new KeywordFieldMapper(
                    name, fieldType, defaultFieldType, ignoreAbove,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
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
                }
            }
            return builder;
        }
    }

    @Override
    protected String parseSourceValue(Object value) {
        return value.toString();
    }

    public static final class KeywordFieldType extends StringFieldType {

        private NamedAnalyzer normalizer = null;
        private boolean splitQueriesOnWhitespace;

        public KeywordFieldType() {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected KeywordFieldType(KeywordFieldType ref) {
            super(ref);
            this.normalizer = ref.normalizer;
            this.splitQueriesOnWhitespace = ref.splitQueriesOnWhitespace;
        }

        @Override
        public KeywordFieldType clone() {
            return new KeywordFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            KeywordFieldType other = (KeywordFieldType) o;
            return Objects.equals(normalizer, other.normalizer) &&
                splitQueriesOnWhitespace == other.splitQueriesOnWhitespace;
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hash(normalizer, splitQueriesOnWhitespace);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        NamedAnalyzer normalizer() {
            return normalizer;
        }

        public void setNormalizer(NamedAnalyzer normalizer) {
            checkIfFrozen();
            this.normalizer = normalizer;
            setIndexAnalyzer(normalizer);
        }

        public boolean splitQueriesOnWhitespace() {
            return splitQueriesOnWhitespace;
        }

        public void setSplitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            checkIfFrozen();
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else if (omitNorms()) {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            } else {
                return new NormsFieldExistsQuery(name());
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.BYTES;
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
            if (searchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
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
            return searchAnalyzer().normalize(name(), value.toString());
        }
    }

    private int ignoreAbove;

    protected KeywordFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                 int ignoreAbove, Settings indexSettings,
                                 MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.ignoreAbove = ignoreAbove;
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
                value = fieldType().nullValueAsString();
            } else {
                value =  parser.textOrNull();
            }
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        final NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
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
                value = newValue;
            }
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored())  {
            Field field = new Field(fieldType().name(), binaryValue, fieldType());
            context.doc().add(field);

            if (fieldType().hasDocValues() == false && fieldType().omitNorms()) {
                createFieldNamesField(context);
            }
        }

        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        }
    }
    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        KeywordFieldMapper k = (KeywordFieldMapper) other;
        if (Objects.equals(fieldType().normalizer, k.fieldType().normalizer) == false) {
            conflicts.add("mapper [" + name() + "] has different [normalizer]");
        }
        this.ignoreAbove = k.ignoreAbove;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }

        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }

        if (fieldType().normalizer() != null) {
            builder.field("normalizer", fieldType().normalizer().name());
        } else if (includeDefaults) {
            builder.nullField("normalizer");
        }

        if (includeDefaults || fieldType().splitQueriesOnWhitespace) {
            builder.field("split_queries_on_whitespace", fieldType().splitQueriesOnWhitespace);
        }
    }
}
