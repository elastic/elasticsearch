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
package org.elasticsearch.index.mapper.core;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.suggest.analyzing.XFreeTextSuggester;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.context.ContextMapping;
import org.elasticsearch.search.suggest.freetext.FreeTextPostingsFormat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeByteValue;
import static org.elasticsearch.index.mapper.MapperBuilders.freetextField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

/**
 *
 */
public class FreetextFieldMapper extends AbstractFieldMapper<String> {

    public static final String CONTENT_TYPE = "freetext";

    @Override
    public String value(Object value) {
        return null;
    }

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final byte DEFAULT_SEPARATOR = XFreeTextSuggester.DEFAULT_SEPARATOR;
        public static final Gram DEFAULT_GRAM = Gram.BIGRAM;
    }

    public enum Gram {
        UNIGRAM(1), BIGRAM(2), TRIGAM(3);

        private final int gramLength;

        Gram(int gramLength) {
            this.gramLength = gramLength;
        }

        public int gramLength() {
            return gramLength;
        }

        public static Gram findByLength(int length) {
            for (Gram gram : Gram.values()) {
                if (gram.gramLength == length) {
                    return gram;
                }
            }

            return BIGRAM;
        }
    }

    public static class Fields {
        // Mapping field names
        public static final String ANALYZER = "analyzer";
        public static final ParseField INDEX_ANALYZER = new ParseField("index_analyzer");
        public static final ParseField SEARCH_ANALYZER = new ParseField("search_analyzer");
        public static final ParseField GRAM = new ParseField("gram");
        public static final ParseField SEPARATOR = new ParseField("separator");
        public static final String TYPE = "type";
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, FreetextFieldMapper> {

        private Gram gram = Defaults.DEFAULT_GRAM;
        private byte separator = Defaults.DEFAULT_SEPARATOR;

        public Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
        }

        public Builder gram(Gram gram) {
            this.gram = gram;
            return this;
        }

        public Builder separator(byte separator) {
            this.separator = separator;
            return this;
        }

        @Override
        public FreetextFieldMapper build(BuilderContext context) {
            return new FreetextFieldMapper(buildNames(context), indexAnalyzer, searchAnalyzer, postingsProvider, similarity, gram, separator, multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            FreetextFieldMapper.Builder builder = freetextField(name);
            boolean gramsConfigured = false;

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type") || fieldName.equals("postings_format")) {
                    continue;
                }
                if (fieldName.equals("analyzer")) {
                    NamedAnalyzer analyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    builder.indexAnalyzer(analyzer);
                    builder.searchAnalyzer(analyzer);
                } else if (Fields.INDEX_ANALYZER.match(fieldName)) {
                    builder.indexAnalyzer(getNamedAnalyzer(parserContext, fieldNode.toString()));
                } else if (Fields.SEARCH_ANALYZER.match(fieldName)) {
                    builder.searchAnalyzer(getNamedAnalyzer(parserContext, fieldNode.toString()));
                } else if (Fields.SEPARATOR.match(fieldName)) {
                    builder.separator(nodeByteValue(fieldNode));
                } else if (Fields.GRAM.match(fieldName)) {
                    builder.gram(Gram.valueOf(fieldNode.toString()));
                    gramsConfigured = true;
                } else if ("fields".equals(fieldName) || "path".equals(fieldName)) {
                    parseMultiField(builder, name, node, parserContext, fieldName, fieldNode);
                } else {
                    throw new MapperParsingException("Unknown field [" + fieldName + "]");
                }
            }

            if (builder.searchAnalyzer == null) {
                builder.searchAnalyzer(parserContext.analysisService().analyzer("simple"));
            }

            if (builder.indexAnalyzer == null) {
                builder.indexAnalyzer(parserContext.analysisService().analyzer("simple"));
            }

            // no explicit configuration, try to go the shingle configuration
            ShingleTokenFilterFactory.Factory shingleFilterFactory = SuggestUtils.getShingleFilterFactory(builder.indexAnalyzer);

            if (shingleFilterFactory != null) {
                // TODO this defaults to bigrams, if the max shingle size is not 1 or 3, good?
                if (!gramsConfigured) {
                    builder.gram(Gram.findByLength(shingleFilterFactory.getMaxShingleSize()));
                }
            }

            // use freetext postings format
            builder.postingsFormat(parserContext.postingFormatService().get("freetext"));
            return builder;
        }

        private NamedAnalyzer getNamedAnalyzer(ParserContext parserContext, String name) {
            NamedAnalyzer analyzer = parserContext.analysisService().analyzer(name);
            if (analyzer == null) {
                throw new ElasticsearchIllegalArgumentException("Can't find default or mapped analyzer with name [" + name + "]");
            }
            return analyzer;
        }
    }

    private final Gram gram;
    private final byte separator;

    public FreetextFieldMapper(Names names, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsProvider, SimilarityProvider similarity, Gram gram, byte separator, MultiFields multiFields, CopyTo copyTo) {
        super(names, 1.0f, Defaults.FIELD_TYPE, null, indexAnalyzer, searchAnalyzer, postingsProvider, null, similarity, null, null, null, multiFields, copyTo);
        this.gram = gram;
        this.separator = separator;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        String input = null;

        if (token == XContentParser.Token.VALUE_STRING) {
            input = parser.text();
            multiFields.parse(this, context);
        }

        boolean containsSeparator = input.indexOf(separator) >= 0;
        if (containsSeparator) {
            throw new ElasticsearchParseException("Field["+ names.name() +"] may not contain ngram separator ["+ separator +"]");
        }

        // TODO WHERE DOES POSTINGSFORMAT COME INTO PLAY???
        context.doc().add(new StringFieldMapper.StringField(names().fullName(), input, fieldType));
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {}

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public Gram getGram() {
        return gram;
    }

    public byte getSeparator() {
        return separator;
    }
}
