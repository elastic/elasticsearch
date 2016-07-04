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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.NumberType;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TermBasedFieldType;
import org.elasticsearch.search.suggest.completion2x.AnalyzingCompletionLookupProvider;
import org.elasticsearch.search.suggest.completion2x.Completion090PostingsFormat;
import org.elasticsearch.search.suggest.completion2x.CompletionTokenStream;
import org.elasticsearch.search.suggest.completion2x.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion2x.context.ContextMapping;
import org.elasticsearch.search.suggest.completion2x.context.ContextMapping.ContextConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

/**
 *
 */
public class CompletionFieldMapper2x extends FieldMapper {

    public static final String CONTENT_TYPE = "completion";

    public static class Defaults {
        public static final CompletionFieldType FIELD_TYPE = new CompletionFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final boolean DEFAULT_PRESERVE_SEPARATORS = true;
        public static final boolean DEFAULT_POSITION_INCREMENTS = true;
        public static final boolean DEFAULT_HAS_PAYLOADS = false;
        public static final int DEFAULT_MAX_INPUT_LENGTH = 50;
    }

    public static class Fields {
        // Mapping field names
        public static final String ANALYZER = "analyzer";
        public static final ParseField SEARCH_ANALYZER = new ParseField("search_analyzer");
        public static final ParseField PRESERVE_SEPARATORS = new ParseField("preserve_separators");
        public static final ParseField PRESERVE_POSITION_INCREMENTS = new ParseField("preserve_position_increments");
        public static final String PAYLOADS = "payloads";
        public static final String TYPE = "type";
        public static final ParseField MAX_INPUT_LENGTH = new ParseField("max_input_length", "max_input_len");
        // Content field names
        public static final String CONTENT_FIELD_NAME_INPUT = "input";
        public static final String CONTENT_FIELD_NAME_OUTPUT = "output";
        public static final String CONTENT_FIELD_NAME_PAYLOAD = "payload";
        public static final String CONTENT_FIELD_NAME_WEIGHT = "weight";
        public static final String CONTEXT = "context";
    }

    public static final Set<String> ALLOWED_CONTENT_FIELD_NAMES;
    static {
        ALLOWED_CONTENT_FIELD_NAMES = new HashSet<>();
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_INPUT);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_OUTPUT);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_PAYLOAD);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTENT_FIELD_NAME_WEIGHT);
        ALLOWED_CONTENT_FIELD_NAMES.add(Fields.CONTEXT);
    }

    public static class Builder extends FieldMapper.Builder<Builder, CompletionFieldMapper2x> {

        private boolean preserveSeparators = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean payloads = Defaults.DEFAULT_HAS_PAYLOADS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;
        private int maxInputLength = Defaults.DEFAULT_MAX_INPUT_LENGTH;
        private SortedMap<String, ContextMapping> contextMapping = ContextMapping.EMPTY_MAPPING;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
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

        public Builder maxInputLength(int maxInputLength) {
            if (maxInputLength <= 0) {
                throw new IllegalArgumentException(
                    Fields.MAX_INPUT_LENGTH.getPreferredName() + " must be > 0 but was [" + maxInputLength + "]");
            }
            this.maxInputLength = maxInputLength;
            return this;
        }

        public Builder contextMapping(SortedMap<String, ContextMapping> contextMapping) {
            this.contextMapping = contextMapping;
            return this;
        }

        @Override
        public CompletionFieldMapper2x build(Mapper.BuilderContext context) {
            setupFieldType(context);
            CompletionFieldType completionFieldType = (CompletionFieldType) fieldType;
            completionFieldType.setProvider(
                new AnalyzingCompletionLookupProvider(preserveSeparators, preservePositionIncrements, payloads));
            completionFieldType.setContextMapping(contextMapping);
            return new CompletionFieldMapper2x(name, fieldType, maxInputLength, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }

    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            CompletionFieldMapper2x.Builder builder = new Builder(name);
            NamedAnalyzer indexAnalyzer = null;
            NamedAnalyzer searchAnalyzer = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                }
                if (Fields.ANALYZER.equals(fieldName) || // index_analyzer is for backcompat, remove for v3.0
                    fieldName.equals("index_analyzer") && parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {

                    indexAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.SEARCH_ANALYZER)) {
                    searchAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (fieldName.equals(Fields.PAYLOADS)) {
                    builder.payloads(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.PRESERVE_SEPARATORS)) {
                    builder.preserveSeparators(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.PRESERVE_POSITION_INCREMENTS)) {
                    builder.preservePositionIncrements(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.MAX_INPUT_LENGTH)) {
                    builder.maxInputLength(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, fieldName, fieldNode)) {
                    iterator.remove();
                } else if (fieldName.equals(Fields.CONTEXT)) {
                    builder.contextMapping(ContextBuilder.loadMappings(fieldNode, parserContext.indexVersionCreated()));
                    iterator.remove();
                }
            }

            if (indexAnalyzer == null) {
                if (searchAnalyzer != null) {
                    throw new MapperParsingException(
                        "analyzer on completion field [" + name + "] must be set when search_analyzer is set");
                }
                indexAnalyzer = searchAnalyzer = parserContext.analysisService().analyzer("simple");
            } else if (searchAnalyzer == null) {
                searchAnalyzer = indexAnalyzer;
            }
            builder.indexAnalyzer(indexAnalyzer);
            builder.searchAnalyzer(searchAnalyzer);

            return builder;
        }

        private NamedAnalyzer getNamedAnalyzer(ParserContext parserContext, String name) {
            NamedAnalyzer analyzer = parserContext.analysisService().analyzer(name);
            if (analyzer == null) {
                throw new IllegalArgumentException("Can't find default or mapped analyzer with name [" + name + "]");
            }
            return analyzer;
        }
    }

    public static final class CompletionFieldType extends TermBasedFieldType {
        private PostingsFormat postingsFormat;
        private AnalyzingCompletionLookupProvider analyzingSuggestLookupProvider;
        private SortedMap<String, ContextMapping> contextMapping = ContextMapping.EMPTY_MAPPING;

        public CompletionFieldType() {
        }

        protected CompletionFieldType(CompletionFieldType ref) {
            super(ref);
            this.postingsFormat = ref.postingsFormat;
            this.analyzingSuggestLookupProvider = ref.analyzingSuggestLookupProvider;
            this.contextMapping = ref.contextMapping;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompletionFieldType)) return false;
            if (!super.equals(o)) return false;
            CompletionFieldType fieldType = (CompletionFieldType) o;
            return analyzingSuggestLookupProvider.getPreserveSep() == fieldType.analyzingSuggestLookupProvider.getPreserveSep()
                    && analyzingSuggestLookupProvider.getPreservePositionsIncrements() ==
                fieldType.analyzingSuggestLookupProvider.getPreservePositionsIncrements() &&
                analyzingSuggestLookupProvider.hasPayloads() == fieldType.analyzingSuggestLookupProvider.hasPayloads() &&
                Objects.equals(getContextMapping(), fieldType.getContextMapping());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(),
                analyzingSuggestLookupProvider.getPreserveSep(),
                analyzingSuggestLookupProvider.getPreservePositionsIncrements(),
                analyzingSuggestLookupProvider.hasPayloads(),
                getContextMapping());
        }

        @Override
        public CompletionFieldType clone() {
            return new CompletionFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType fieldType, List<String> conflicts, boolean strict) {
            super.checkCompatibility(fieldType, conflicts, strict);
            CompletionFieldType other = (CompletionFieldType) fieldType;
            if (analyzingSuggestLookupProvider.hasPayloads() != other.analyzingSuggestLookupProvider.hasPayloads()) {
                conflicts.add("mapper [" + name() + "] has different [payload] values");
            }
            if (analyzingSuggestLookupProvider.getPreservePositionsIncrements() !=
                other.analyzingSuggestLookupProvider.getPreservePositionsIncrements()) {
                conflicts.add("mapper [" + name() + "] has different [preserve_position_increments] values");
            }
            if (analyzingSuggestLookupProvider.getPreserveSep() != other.analyzingSuggestLookupProvider.getPreserveSep()) {
                conflicts.add("mapper [" + name() + "] has different [preserve_separators] values");
            }
            if (!ContextMapping.mappingsAreEqual(getContextMapping(), other.getContextMapping())) {
                conflicts.add("mapper [" + name() + "] has different [context_mapping] values");
            }
        }

        public void setProvider(AnalyzingCompletionLookupProvider provider) {
            checkIfFrozen();
            this.analyzingSuggestLookupProvider = provider;
        }

        public synchronized PostingsFormat postingsFormat(PostingsFormat in) {
            if (in instanceof Completion090PostingsFormat) {
                throw new IllegalStateException("Double wrapping of " + Completion090PostingsFormat.class);
            }
            if (postingsFormat == null) {
                postingsFormat = new Completion090PostingsFormat(in, analyzingSuggestLookupProvider);
            }
            return postingsFormat;
        }

        public void setContextMapping(SortedMap<String, ContextMapping> contextMapping) {
            checkIfFrozen();
            this.contextMapping = contextMapping;
        }

        /**
         * Get the context mapping associated with this completion field
         */
        public SortedMap<String, ContextMapping> getContextMapping() {
            return contextMapping;
        }

        /**
         * @return true if a context mapping has been defined
         */
        public boolean requiresContext() {
            return contextMapping.isEmpty() == false;
        }
    }

    private static final BytesRef EMPTY = new BytesRef();

    private int maxInputLength;

    public CompletionFieldMapper2x(String simpleName, MappedFieldType fieldType, int maxInputLength,
                                   Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, multiFields, copyTo);
        this.maxInputLength = maxInputLength;
    }

    @Override
    public CompletionFieldType fieldType() {
        return (CompletionFieldType) super.fieldType();
    }

    /**
     * Parses and indexes inputs
     * Parsing:
     * Acceptable format:
     * "STRING" - interpreted as field value (input)
     * "ARRAY" - each element can be one of "OBJECT" (see below)
     * "OBJECT" - { "input": STRING|ARRAY, "weight": STRING|INT, "contexts": ARRAY|OBJECT }
     */
    @Override
    public Mapper parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            throw new MapperParsingException("completion field [" + fieldType().name() + "] does not support null values");
        }

        String surfaceForm = null;
        BytesRef payload = null;
        long weight = -1;
        List<String> inputs = new ArrayList<>(4);

        SortedMap<String, ContextConfig> contextConfig = null;

        if (token == XContentParser.Token.VALUE_STRING) {
            inputs.add(parser.text());
            multiFields.parse(this, context);
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    if (!ALLOWED_CONTENT_FIELD_NAMES.contains(currentFieldName)) {
                        throw new IllegalArgumentException(
                            "Unknown field name[" + currentFieldName + "], must be one of " + ALLOWED_CONTENT_FIELD_NAMES);
                    }
                } else if (Fields.CONTEXT.equals(currentFieldName)) {
                    SortedMap<String, ContextConfig> configs = new TreeMap<>();
                    if (token == Token.START_OBJECT) {
                        while ((token = parser.nextToken()) != Token.END_OBJECT) {
                            String name = parser.currentName();
                            ContextMapping mapping = fieldType().getContextMapping().get(name);
                            if (mapping == null) {
                                throw new ElasticsearchParseException("context [{}] is not defined", name);
                            } else {
                                token = parser.nextToken();
                                configs.put(name, mapping.parseContext(context, parser));
                            }
                        }
                        contextConfig = new TreeMap<>();
                        for (ContextMapping mapping : fieldType().getContextMapping().values()) {
                            ContextConfig config = configs.get(mapping.name());
                            contextConfig.put(mapping.name(), config == null ? mapping.defaultConfig() : config);
                        }
                    } else {
                        throw new ElasticsearchParseException("context must be an object");
                    }
                } else if (Fields.CONTENT_FIELD_NAME_PAYLOAD.equals(currentFieldName)) {
                    if (!isStoringPayloads()) {
                        throw new MapperException("Payloads disabled in mapping");
                    }
                    if (token == XContentParser.Token.START_OBJECT) {
                        XContentBuilder payloadBuilder =
                            XContentFactory.contentBuilder(parser.contentType()).copyCurrentStructure(parser);
                        payload = payloadBuilder.bytes().toBytesRef();
                        payloadBuilder.close();
                    } else if (token.isValue()) {
                        payload = parser.utf8BytesOrNull();
                    } else {
                        throw new MapperException("payload doesn't support type " + token);
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (Fields.CONTENT_FIELD_NAME_OUTPUT.equals(currentFieldName)) {
                        surfaceForm = parser.text();
                    }
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        inputs.add(parser.text());
                    }
                    if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        Number weightValue;
                        try {
                            weightValue = Long.parseLong(parser.text());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException(
                                "Weight must be a string representing a numeric value, but was [" + parser.text() + "]");
                        }
                        weight = weightValue.longValue(); // always parse a long to make sure we don't get overflow
                        checkWeight(weight);
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        NumberType numberType = parser.numberType();
                        if (NumberType.LONG != numberType && NumberType.INT != numberType) {
                            throw new IllegalArgumentException(
                                "Weight must be an integer, but was [" + parser.numberValue() + "]");
                        }
                        weight = parser.longValue(); // always parse a long to make sure we don't get overflow
                        checkWeight(weight);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            inputs.add(parser.text());
                        }
                    }
                }
            }
        }

        if (contextConfig == null) {
            contextConfig = new TreeMap<>();
            for (ContextMapping mapping : fieldType().getContextMapping().values()) {
                contextConfig.put(mapping.name(), mapping.defaultConfig());
            }
        }

        final ContextMapping.Context ctx = new ContextMapping.Context(contextConfig, context.doc());

        payload = payload == null ? EMPTY : payload;
        if (surfaceForm == null) { // no surface form use the input
            for (String input : inputs) {
                if (input.length() == 0) {
                    continue;
                }
                BytesRef suggestPayload = fieldType().analyzingSuggestLookupProvider.buildPayload(new BytesRef(
                    input), weight, payload);
                context.doc().add(getCompletionField(ctx, input, suggestPayload));
            }
        } else {
            BytesRef suggestPayload = fieldType().analyzingSuggestLookupProvider.buildPayload(new BytesRef(
                surfaceForm), weight, payload);
            for (String input : inputs) {
                if (input.length() == 0) {
                    continue;
                }
                context.doc().add(getCompletionField(ctx, input, suggestPayload));
            }
        }
        return null;
    }

    private void checkWeight(long weight) {
        if (weight < 0 || weight > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Weight must be in the interval [0..2147483647], but was [" + weight + "]");
        }
    }

    public Field getCompletionField(ContextMapping.Context ctx, String input, BytesRef payload) {
        final String originalInput = input;
        if (input.length() > maxInputLength) {
            final int len = correctSubStringLen(input, Math.min(maxInputLength, input.length()));
            input = input.substring(0, len);
        }
        for (int i = 0; i < input.length(); i++) {
            if (isReservedChar(input.charAt(i))) {
                throw new IllegalArgumentException("Illegal input [" + originalInput + "] UTF-16 codepoint  [0x"
                    + Integer.toHexString(input.charAt(i)).toUpperCase(Locale.ROOT)
                    + "] at position " + i + " is a reserved character");
            }
        }
        return new SuggestField(
            fieldType().name(), ctx, input, fieldType(), payload, fieldType().analyzingSuggestLookupProvider);
    }

    public static int correctSubStringLen(String input, int len) {
        if (Character.isHighSurrogate(input.charAt(len - 1))) {
            assert input.length() >= len + 1 && Character.isLowSurrogate(input.charAt(len));
            return len + 1;
        }
        return len;
    }

    public BytesRef buildPayload(BytesRef surfaceForm, long weight, BytesRef payload) throws IOException {
        return fieldType().analyzingSuggestLookupProvider.buildPayload(surfaceForm, weight, payload);
    }

    private static final class SuggestField extends Field {
        private final BytesRef payload;
        private final CompletionTokenStream.ToFiniteStrings toFiniteStrings;
        private final ContextMapping.Context ctx;

        public SuggestField(String name, ContextMapping.Context ctx,
                            String value, MappedFieldType type, BytesRef payload,
                            CompletionTokenStream.ToFiniteStrings toFiniteStrings) {
            super(name, value, type);
            this.payload = payload;
            this.toFiniteStrings = toFiniteStrings;
            this.ctx = ctx;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) {
            TokenStream ts = ctx.wrapTokenStream(super.tokenStream(analyzer, previous));
            return new CompletionTokenStream(ts, payload, toFiniteStrings);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName())
            .field(Fields.TYPE, CONTENT_TYPE);

        builder.field(Fields.ANALYZER, fieldType().indexAnalyzer().name());
        if (fieldType().indexAnalyzer().name().equals(fieldType().searchAnalyzer().name()) == false) {
            builder.field(Fields.SEARCH_ANALYZER.getPreferredName(), fieldType().searchAnalyzer().name());
        }
        builder.field(Fields.PAYLOADS, fieldType().analyzingSuggestLookupProvider.hasPayloads());
        builder.field(Fields.PRESERVE_SEPARATORS.getPreferredName(),
            fieldType().analyzingSuggestLookupProvider.getPreserveSep());
        builder.field(Fields.PRESERVE_POSITION_INCREMENTS.getPreferredName(),
            fieldType().analyzingSuggestLookupProvider.getPreservePositionsIncrements());
        builder.field(Fields.MAX_INPUT_LENGTH.getPreferredName(), this.maxInputLength);
        multiFields.toXContent(builder, params);

        if (fieldType().requiresContext()) {
            builder.startObject(Fields.CONTEXT);
            for (ContextMapping mapping : fieldType().getContextMapping().values()) {
                builder.value(mapping);
            }
            builder.endObject();
        }

        return builder.endObject();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public boolean isStoringPayloads() {
        return fieldType().analyzingSuggestLookupProvider.hasPayloads();
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        CompletionFieldMapper2x fieldMergeWith = (CompletionFieldMapper2x) mergeWith;
        this.maxInputLength = fieldMergeWith.maxInputLength;
    }

    // this should be package private but our tests don't allow it.
    public static boolean isReservedChar(char character) {
        /*  we use 0x001F as a SEP_LABEL in the suggester but we can use the UTF-16 representation since they
         *  are equivalent. We also don't need to convert the input character to UTF-8 here to check for
         *  the 0x00 end label since all multi-byte  UTF-8 chars start with 0x10 binary so if the UTF-16 CP is == 0x00
         *  it's the single byte UTF-8 CP */
        assert XAnalyzingSuggester.PAYLOAD_SEP == XAnalyzingSuggester.SEP_LABEL; // ensure they are the same!
        switch (character) {
            case XAnalyzingSuggester.END_BYTE:
            case XAnalyzingSuggester.SEP_LABEL:
            case XAnalyzingSuggester.HOLE_CHARACTER:
            case ContextMapping.SEPARATOR:
                return true;
            default:
                return false;
        }
    }
}
