/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.suggest.completion.AnalyzingCompletionLookupProvider;
import org.elasticsearch.search.suggest.completion.CompletionPostingsFormatProvider;
import org.elasticsearch.search.suggest.completion.CompletionTokenStream;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CompletionFieldMapper extends AbstractFieldMapper<String> {

    public static final String CONTENT_TYPE = "completion";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final boolean DEFAULT_PRESERVE_SEPARATORS = true;
        public static final boolean DEFAULT_POSITION_INCREMENTS = true;
        public static final boolean DEFAULT_HAS_PAYLOADS = false;
    }

    public static class Fields {
        public static final String INDEX_ANALYZER = "index_analyzer";
        public static final String SEARCH_ANALYZER = "search_analyzer";
        public static final String PRESERVE_SEPARATORS = "preserve_separators";
        public static final String PRESERVE_POSITION_INCREMENTS = "preserve_position_increments";
        public static final String PAYLOADS = "payloads";
        public static final String TYPE = "type";
    }

    public static class Builder extends AbstractFieldMapper.OpenBuilder<Builder, CompletionFieldMapper>  {

        private NamedAnalyzer searchAnalyzer;
        private NamedAnalyzer indexAnalyzer;
        private boolean preserveSeparators = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean payloads = Defaults.DEFAULT_HAS_PAYLOADS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public Builder indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public Builder payloads(boolean payloads) {
            this.payloads = payloads;
            return this;
        }

        public Builder preserveSeparators(boolean preserveSeparators) {
            this.preserveSeparators = preserveSeparators;
            return this;
        }

        public Builder preservePositionIncrements(boolean preservePositionIncrements) {
            this.preservePositionIncrements = preservePositionIncrements;
            return this;
        }

        @Override
        public CompletionFieldMapper build(Mapper.BuilderContext context) {
            return new CompletionFieldMapper(buildNames(context), indexAnalyzer, searchAnalyzer, provider, similarity, payloads, preserveSeparators, preservePositionIncrements);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            CompletionFieldMapper.Builder builder = new CompletionFieldMapper.Builder(name);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                }
                if (fieldName.equals(Fields.INDEX_ANALYZER) || fieldName.equals("indexAnalyzer")) {
                    builder.indexAnalyzer(parserContext.analysisService().analyzer(fieldNode.toString()));
                } else if (fieldName.equals(Fields.SEARCH_ANALYZER) || fieldName.equals("searchAnalyzer")) {
                    builder.searchAnalyzer(parserContext.analysisService().analyzer(fieldNode.toString()));
                } else if (fieldName.equals(Fields.PAYLOADS)) {
                    builder.payloads(Boolean.parseBoolean(fieldNode.toString()));
                } else if (fieldName.equals(Fields.PRESERVE_SEPARATORS) || fieldName.equals("preserveSeparators")) {
                    builder.preserveSeparators(Boolean.parseBoolean(fieldNode.toString()));
                } else if (fieldName.equals(Fields.PRESERVE_POSITION_INCREMENTS) || fieldName.equals("preservePositionIncrements")) {
                    builder.preservePositionIncrements(Boolean.parseBoolean(fieldNode.toString()));
                }
            }

            if (builder.searchAnalyzer == null) {
                builder.searchAnalyzer(parserContext.analysisService().analyzer("simple"));
            }

            if (builder.indexAnalyzer == null) {
                builder.indexAnalyzer(parserContext.analysisService().analyzer("simple"));
            }
            // we are just using this as the default to be wrapped by the CompletionPostingsFormatProvider in the SuggesteFieldMapper ctor
            builder.postingsFormat(parserContext.postingFormatService().get("default"));
            return builder;
        }
    }

    private static final BytesRef EMPTY = new BytesRef();

    private final CompletionPostingsFormatProvider completionPostingsFormatProvider;
    private final AnalyzingCompletionLookupProvider analyzingSuggestLookupProvider;
    private final boolean payloads;
    private final boolean preservePositionIncrements;
    private final boolean preserveSeparators;

    public CompletionFieldMapper(Names names, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, PostingsFormatProvider provider, SimilarityProvider similarity, boolean payloads,
                                 boolean preserveSeparators, boolean preservePositionIncrements) {
        super(names, 1.0f, Defaults.FIELD_TYPE, indexAnalyzer, searchAnalyzer, provider, similarity, null);
        analyzingSuggestLookupProvider = new AnalyzingCompletionLookupProvider(preserveSeparators, false, preservePositionIncrements, payloads);
        this.completionPostingsFormatProvider = new CompletionPostingsFormatProvider("completion", provider, analyzingSuggestLookupProvider);
        this.preserveSeparators = preserveSeparators;
        this.payloads = payloads;
        this.preservePositionIncrements = preservePositionIncrements;
    }

   
    @Override
    public PostingsFormatProvider postingsFormatProvider() {
        return this.completionPostingsFormatProvider;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();

        String surfaceForm = null;
        BytesRef payload = null;
        long weight = -1;
        List<String> inputs = Lists.newArrayListWithExpectedSize(4);

        if (token == XContentParser.Token.VALUE_STRING) {
            inputs.add(parser.text());
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("payload".equals(currentFieldName)) {
                    if (!isStoringPayloads()) {
                        throw new MapperException("Payloads disabled in mapping");
                    }
                    if (token == XContentParser.Token.START_OBJECT) {
                        XContentBuilder payloadBuilder = XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                        payload = payloadBuilder.bytes().toBytesRef();
                        payloadBuilder.close();
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("output".equals(currentFieldName)) {
                        surfaceForm = parser.text();
                    }
                    if ("input".equals(currentFieldName)) {
                        inputs.add(parser.text());
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("weight".equals(currentFieldName)) {
                        weight = parser.longValue(); // always parse a long to make sure we don't get the overflow value
                        if (weight < 0 || weight > Integer.MAX_VALUE) {
                            throw new ElasticSearchIllegalArgumentException("Weight must be in the interval [0..2147483647] but was " + weight);
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("input".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            inputs.add(parser.text());
                        }
                    }
                }
            }
        }
        payload = payload == null ? EMPTY: payload;
        if (surfaceForm == null) { // no surface form use the input
            for (String input : inputs) {
                BytesRef suggestPayload = analyzingSuggestLookupProvider.buildPayload(new BytesRef(
                        input), weight, payload);
                context.doc().add(getCompletionField(input, suggestPayload));
            }
        } else {
            BytesRef suggestPayload = analyzingSuggestLookupProvider.buildPayload(new BytesRef(
                    surfaceForm), weight, payload);
            for (String input : inputs) {
                context.doc().add(getCompletionField(input, suggestPayload));
            }    
        }
    }
    
    public Field getCompletionField(String input, BytesRef payload) {
        return new SuggestField(names().fullName(), input, this.fieldType, payload, analyzingSuggestLookupProvider);
    }

    public BytesRef buildPayload(BytesRef surfaceForm, long weight, BytesRef payload) throws IOException {
        return analyzingSuggestLookupProvider.buildPayload(
                surfaceForm, weight, payload);
    }

    private static final class SuggestField extends Field {
        private final BytesRef payload;
        private final CompletionTokenStream.ToFiniteStrings toFiniteStrings;

        public SuggestField(String name, String value, FieldType type, BytesRef payload, CompletionTokenStream.ToFiniteStrings toFiniteStrings) {
            super(name, value, type);
            this.payload = payload;
            this.toFiniteStrings = toFiniteStrings;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
            TokenStream ts = super.tokenStream(analyzer);
            return new CompletionTokenStream(ts, payload, toFiniteStrings);
        }
    }

    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(name())
            .field(Fields.TYPE, CONTENT_TYPE)
            .field(Fields.INDEX_ANALYZER, indexAnalyzer.name())
            .field(Fields.SEARCH_ANALYZER, searchAnalyzer.name())
            .field(Fields.PAYLOADS, this.payloads)
            .field(Fields.PRESERVE_SEPARATORS, this.preserveSeparators)
            .field(Fields.PRESERVE_POSITION_INCREMENTS, this.preservePositionIncrements)
        .endObject();
    }

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        return null;
    }


    @Override
    protected String contentType() {
        return CONTENT_TYPE;
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
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    public boolean isStoringPayloads() {
        return payloads;
    }
}
