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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.suggest.document.Completion84PostingsFormat;
import org.apache.lucene.search.suggest.document.CompletionAnalyzer;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.search.suggest.document.RegexCompletionQuery;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.NumberType;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Mapper for completion field. The field values are indexed as a weighted FST for
 * fast auto-completion/search-as-you-type functionality.<br>
 *
 * Type properties:<br>
 * <ul>
 *  <li>"analyzer": "simple", (default)</li>
 *  <li>"search_analyzer": "simple", (default)</li>
 *  <li>"preserve_separators" : true, (default)</li>
 *  <li>"preserve_position_increments" : true (default)</li>
 *  <li>"min_input_length": 50 (default)</li>
 *  <li>"contexts" : CONTEXTS</li>
 * </ul>
 * see {@link ContextMappings#load(Object, Version)} for CONTEXTS<br>
 * see {@link #parse(ParseContext)} for acceptable inputs for indexing<br>
 * <p>
 *  This field type constructs completion queries that are run
 *  against the weighted FST index by the {@link CompletionSuggester}.
 *  This field can also be extended to add search criteria to suggestions
 *  for query-time filtering and boosting (see {@link ContextMappings}
 */
public class CompletionFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "completion";

    /**
     * Maximum allowed number of completion contexts in a mapping.
     */
    static final int COMPLETION_CONTEXTS_LIMIT = 10;

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), defaultAnalyzer, indexVersionCreated).init(this);
    }

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
        public static final boolean DEFAULT_PRESERVE_SEPARATORS = true;
        public static final boolean DEFAULT_POSITION_INCREMENTS = true;
        public static final int DEFAULT_MAX_INPUT_LENGTH = 50;
    }

    public static class Fields {
        // Content field names
        public static final String CONTENT_FIELD_NAME_INPUT = "input";
        public static final String CONTENT_FIELD_NAME_WEIGHT = "weight";
        public static final String CONTENT_FIELD_NAME_CONTEXTS = "contexts";
    }

    private static CompletionFieldMapper toType(FieldMapper in) {
        return (CompletionFieldMapper) in;
    }

    /**
     * Builder for {@link CompletionFieldMapper}
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<NamedAnalyzer> analyzer;
        private final Parameter<NamedAnalyzer> searchAnalyzer;
        private final Parameter<Boolean> preserveSeparators = Parameter.boolParam("preserve_separators", false,
            m -> toType(m).preserveSeparators, Defaults.DEFAULT_PRESERVE_SEPARATORS);
        private final Parameter<Boolean> preservePosInc = Parameter.boolParam("preserve_position_increments", false,
            m -> toType(m).preservePosInc, Defaults.DEFAULT_POSITION_INCREMENTS);
        private final Parameter<ContextMappings> contexts = new Parameter<>("contexts", false, () -> null,
            (n, c, o) -> ContextMappings.load(o, c.indexVersionCreated()), m -> toType(m).contexts)
            .setSerializer((b, n, c) -> {
                if (c == null) {
                    return;
                }
                b.startArray(n);
                c.toXContent(b, ToXContent.EMPTY_PARAMS);
                b.endArray();
            }, ContextMappings::toString);
        private final Parameter<Integer> maxInputLength = Parameter.intParam("max_input_length", true,
            m -> toType(m).maxInputLength, Defaults.DEFAULT_MAX_INPUT_LENGTH)
            .addDeprecatedName("max_input_len")
            .setValidator(Builder::validateInputLength);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final NamedAnalyzer defaultAnalyzer;
        private final Version indexVersionCreated;

        private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Builder.class);

        /**
         * @param name of the completion field to build
         */
        public Builder(String name, NamedAnalyzer defaultAnalyzer, Version indexVersionCreated) {
            super(name);
            this.defaultAnalyzer = defaultAnalyzer;
            this.indexVersionCreated = indexVersionCreated;
            this.analyzer = Parameter.analyzerParam("analyzer", false, m -> toType(m).analyzer, () -> defaultAnalyzer);
            this.searchAnalyzer
                = Parameter.analyzerParam("search_analyzer", true, m -> toType(m).searchAnalyzer, analyzer::getValue);
        }

        private static void validateInputLength(int maxInputLength) {
            if (maxInputLength <= 0) {
                throw new IllegalArgumentException("[max_input_length] must be > 0 but was [" + maxInputLength + "]");
            }
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(analyzer, searchAnalyzer, preserveSeparators, preservePosInc, contexts, maxInputLength, meta);
        }

        @Override
        protected void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            builder.field("analyzer", this.analyzer.getValue().name());
            if (Objects.equals(this.analyzer.getValue().name(), this.searchAnalyzer.getValue().name()) == false) {
                builder.field("search_analyzer", this.searchAnalyzer.getValue().name());
            }
            builder.field(this.preserveSeparators.name, this.preserveSeparators.getValue());
            builder.field(this.preservePosInc.name, this.preservePosInc.getValue());
            builder.field(this.maxInputLength.name, this.maxInputLength.getValue());
            if (this.contexts.getValue() != null) {
                builder.startArray(this.contexts.name);
                this.contexts.getValue().toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endArray();
            }
        }

        @Override
        public CompletionFieldMapper build(BuilderContext context) {
            checkCompletionContextsLimit(context);
            NamedAnalyzer completionAnalyzer = new NamedAnalyzer(this.searchAnalyzer.getValue().name(), AnalyzerScope.INDEX,
                new CompletionAnalyzer(this.searchAnalyzer.getValue(), preserveSeparators.getValue(), preservePosInc.getValue()));

            CompletionFieldType ft
                = new CompletionFieldType(buildFullName(context), completionAnalyzer, meta.getValue());
            ft.setContextMappings(contexts.getValue());
            ft.setPreservePositionIncrements(preservePosInc.getValue());
            ft.setPreserveSep(preserveSeparators.getValue());
            ft.setIndexAnalyzer(analyzer.getValue());
            return new CompletionFieldMapper(name, ft, defaultAnalyzer,
                multiFieldsBuilder.build(this, context), copyTo.build(), indexVersionCreated, this);
        }

        private void checkCompletionContextsLimit(BuilderContext context) {
            if (this.contexts.getValue() != null && this.contexts.getValue().size() > COMPLETION_CONTEXTS_LIMIT) {
                if (context.indexCreatedVersion().onOrAfter(Version.V_8_0_0)) {
                    throw new IllegalArgumentException(
                        "Limit of completion field contexts [" + COMPLETION_CONTEXTS_LIMIT + "] has been exceeded");
                } else {
                    deprecationLogger.deprecate("excessive_completion_contexts",
                        "You have defined more than [" + COMPLETION_CONTEXTS_LIMIT + "] completion contexts" +
                            " in the mapping for index [" + context.indexSettings().get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME) + "]. " +
                            "The maximum allowed number of completion contexts in a mapping will be limited to " +
                            "[" + COMPLETION_CONTEXTS_LIMIT + "] starting in version [8.0].");
                }
            }
        }

    }

    public static final Set<String> ALLOWED_CONTENT_FIELD_NAMES = Sets.newHashSet(Fields.CONTENT_FIELD_NAME_INPUT,
            Fields.CONTENT_FIELD_NAME_WEIGHT, Fields.CONTENT_FIELD_NAME_CONTEXTS);

    public static final TypeParser PARSER
        = new TypeParser((n, c) -> new Builder(n, c.getIndexAnalyzers().get("simple"), c.indexVersionCreated()));

    public static final class CompletionFieldType extends TermBasedFieldType {

        private static PostingsFormat postingsFormat;

        private boolean preserveSep = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;
        private ContextMappings contextMappings = null;

        public CompletionFieldType(String name, NamedAnalyzer searchAnalyzer, Map<String, String> meta) {
            super(name, true, false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, searchAnalyzer, searchAnalyzer), meta);
        }

        public void setPreserveSep(boolean preserveSep) {
            this.preserveSep = preserveSep;
        }

        public void setPreservePositionIncrements(boolean preservePositionIncrements) {
            this.preservePositionIncrements = preservePositionIncrements;
        }

        public void setContextMappings(ContextMappings contextMappings) {
            this.contextMappings = contextMappings;
        }

        @Override
        public NamedAnalyzer indexAnalyzer() {
            final NamedAnalyzer indexAnalyzer = super.indexAnalyzer();
            if (indexAnalyzer != null && !(indexAnalyzer.analyzer() instanceof CompletionAnalyzer)) {
                return new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX,
                        new CompletionAnalyzer(indexAnalyzer, preserveSep, preservePositionIncrements));

            }
            return indexAnalyzer;
        }

        /**
         * @return true if there are one or more context mappings defined
         * for this field type
         */
        public boolean hasContextMappings() {
            return contextMappings != null;
        }

        /**
         * @return associated context mappings for this field type
         */
        public ContextMappings getContextMappings() {
            return contextMappings;
        }

        /**
         * @return postings format to use for this field-type
         */
        public static synchronized PostingsFormat postingsFormat() {
            if (postingsFormat == null) {
                postingsFormat = new Completion84PostingsFormat();
            }
            return postingsFormat;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        /**
         * Completion prefix query
         */
        public CompletionQuery prefixQuery(Object value) {
            return new PrefixCompletionQuery(getTextSearchInfo().getSearchAnalyzer().analyzer(),
                new Term(name(), indexedValueForSearch(value)));
        }

        /**
         * Completion prefix regular expression query
         */
        public CompletionQuery regexpQuery(Object value, int flags, int maxDeterminizedStates) {
            return new RegexCompletionQuery(new Term(name(), indexedValueForSearch(value)), flags, maxDeterminizedStates);
        }

        /**
         * Completion prefix fuzzy query
         */
        public CompletionQuery fuzzyQuery(String value, Fuzziness fuzziness, int nonFuzzyPrefixLength,
                                          int minFuzzyPrefixLength, int maxExpansions, boolean transpositions,
                                          boolean unicodeAware) {
            return new FuzzyCompletionQuery(getTextSearchInfo().getSearchAnalyzer().analyzer(),
                new Term(name(), indexedValueForSearch(value)), null,
                fuzziness.asDistance(), transpositions, nonFuzzyPrefixLength, minFuzzyPrefixLength,
                unicodeAware, maxExpansions);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

    }

    private final int maxInputLength;
    private final boolean preserveSeparators;
    private final boolean preservePosInc;
    private final NamedAnalyzer defaultAnalyzer;
    private final NamedAnalyzer analyzer;
    private final NamedAnalyzer searchAnalyzer;
    private final ContextMappings contexts;
    private final Version indexVersionCreated;

    public CompletionFieldMapper(String simpleName, MappedFieldType mappedFieldType, NamedAnalyzer defaultAnalyzer,
                                 MultiFields multiFields, CopyTo copyTo, Version indexVersionCreated, Builder builder) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.defaultAnalyzer = defaultAnalyzer;
        this.maxInputLength = builder.maxInputLength.getValue();
        this.preserveSeparators = builder.preserveSeparators.getValue();
        this.preservePosInc = builder.preservePosInc.getValue();
        this.analyzer = builder.analyzer.getValue();
        this.searchAnalyzer = builder.searchAnalyzer.getValue();
        this.contexts = builder.contexts.getValue();
        this.indexVersionCreated = indexVersionCreated;
    }

    @Override
    public CompletionFieldType fieldType() {
        return (CompletionFieldType) super.fieldType();
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    /**
     * Parses and indexes inputs
     *
     * Parsing:
     *  Acceptable format:
     *   "STRING" - interpreted as field value (input)
     *   "ARRAY" - each element can be one of "OBJECT" (see below)
     *   "OBJECT" - { "input": STRING|ARRAY, "weight": STRING|INT, "contexts": ARRAY|OBJECT }
     *
     * Indexing:
     *  if context mappings are defined, delegates to {@link ContextMappings#addField(ParseContext.Document, String, String, int, Map)}
     *  else adds inputs as a {@link org.apache.lucene.search.suggest.document.SuggestField}
     */
    @Override
    public void parse(ParseContext context) throws IOException {
        // parse
        XContentParser parser = context.parser();
        Token token = parser.currentToken();
        Map<String, CompletionInputMetadata> inputMap = new HashMap<>(1);

        if (context.externalValueSet()) {
            inputMap = getInputMapFromExternalValue(context);
        } else if (token == Token.VALUE_NULL) { // ignore null values
            return;
        } else if (token == Token.START_ARRAY) {
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                parse(context, token, parser, inputMap);
            }
        } else {
            parse(context, token, parser, inputMap);
        }

        // index
        for (Map.Entry<String, CompletionInputMetadata> completionInput : inputMap.entrySet()) {
            String input = completionInput.getKey();
            if (input.trim().isEmpty()) {
                context.addIgnoredField(mappedFieldType.name());
                continue;
            }
            // truncate input
            if (input.length() > maxInputLength) {
                int len = Math.min(maxInputLength, input.length());
                if (Character.isHighSurrogate(input.charAt(len - 1))) {
                    assert input.length() >= len + 1 && Character.isLowSurrogate(input.charAt(len));
                    len += 1;
                }
                input = input.substring(0, len);
            }
            CompletionInputMetadata metadata = completionInput.getValue();
            if (fieldType().hasContextMappings()) {
                fieldType().getContextMappings().addField(context.doc(), fieldType().name(),
                        input, metadata.weight, metadata.contexts);
            } else {
                context.doc().add(new SuggestField(fieldType().name(), input, metadata.weight));
            }
        }

        createFieldNamesField(context);
        for (CompletionInputMetadata metadata: inputMap.values()) {
            ParseContext externalValueContext = context.createExternalValueContext(metadata);
            multiFields.parse(this, externalValueContext);
        }
    }

    private Map<String, CompletionInputMetadata> getInputMapFromExternalValue(ParseContext context) {
        Map<String, CompletionInputMetadata> inputMap;
        if (isExternalValueOfClass(context, CompletionInputMetadata.class)) {
            CompletionInputMetadata inputAndMeta = (CompletionInputMetadata) context.externalValue();
            inputMap = Collections.singletonMap(inputAndMeta.input, inputAndMeta);
        } else {
            String fieldName = context.externalValue().toString();
            inputMap = Collections.singletonMap(fieldName, new CompletionInputMetadata(fieldName, Collections.emptyMap(), 1));
        }
        return inputMap;
    }

    private boolean isExternalValueOfClass(ParseContext context, Class<?> clazz) {
        return context.externalValue().getClass().equals(clazz);
    }

    /**
     * Acceptable inputs:
     *  "STRING" - interpreted as the field value (input)
     *  "OBJECT" - { "input": STRING|ARRAY, "weight": STRING|INT, "contexts": ARRAY|OBJECT }
     */
    private void parse(ParseContext parseContext, Token token,
                       XContentParser parser, Map<String, CompletionInputMetadata> inputMap) throws IOException {
        String currentFieldName = null;
        if (token == Token.VALUE_STRING) {
            inputMap.put(parser.text(), new CompletionInputMetadata(parser.text(), Collections.<String, Set<String>>emptyMap(), 1));
        } else if (token == Token.START_OBJECT) {
            Set<String> inputs = new HashSet<>();
            int weight = 1;
            Map<String, Set<String>> contextsMap = new HashMap<>();
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    if (!ALLOWED_CONTENT_FIELD_NAMES.contains(currentFieldName)) {
                        throw new IllegalArgumentException("unknown field name [" + currentFieldName
                            + "], must be one of " + ALLOWED_CONTENT_FIELD_NAMES);
                    }
                } else if (currentFieldName != null) {
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        if (token == Token.VALUE_STRING) {
                            inputs.add(parser.text());
                        } else if (token == Token.START_ARRAY) {
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                if (token == Token.VALUE_STRING) {
                                    inputs.add(parser.text());
                                } else {
                                    throw new IllegalArgumentException("input array must have string values, but was ["
                                        + token.name() + "]");
                                }
                            }
                        } else {
                            throw new IllegalArgumentException("input must be a string or array, but was [" + token.name() + "]");
                        }
                    } else if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        final Number weightValue;
                        if (token == Token.VALUE_STRING) {
                            try {
                                weightValue = Long.parseLong(parser.text());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException("weight must be an integer, but was [" + parser.text() + "]");
                            }
                        } else if (token == Token.VALUE_NUMBER) {
                            NumberType numberType = parser.numberType();
                            if (NumberType.LONG != numberType && NumberType.INT != numberType) {
                                throw new IllegalArgumentException("weight must be an integer, but was [" + parser.numberValue() + "]");
                            }
                            weightValue = parser.numberValue();
                        } else {
                            throw new IllegalArgumentException("weight must be a number or string, but was [" + token.name() + "]");
                        }
                        // always parse a long to make sure we don't get overflow
                        if (weightValue.longValue() < 0 || weightValue.longValue() > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException("weight must be in the interval [0..2147483647], but was ["
                                + weightValue.longValue() + "]");
                        }
                        weight = weightValue.intValue();
                    } else if (Fields.CONTENT_FIELD_NAME_CONTEXTS.equals(currentFieldName)) {
                        if (fieldType().hasContextMappings() == false) {
                            throw new IllegalArgumentException("contexts field is not supported for field: [" + fieldType().name() + "]");
                        }
                        ContextMappings contextMappings = fieldType().getContextMappings();
                        XContentParser.Token currentToken = parser.currentToken();
                        if (currentToken == XContentParser.Token.START_OBJECT) {
                            ContextMapping contextMapping = null;
                            String fieldName = null;
                            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (currentToken == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                    contextMapping = contextMappings.get(fieldName);
                                } else {
                                    assert fieldName != null;
                                    assert !contextsMap.containsKey(fieldName);
                                    contextsMap.put(fieldName, contextMapping.parseContext(parseContext, parser));
                                }
                            }
                        } else {
                            throw new IllegalArgumentException("contexts must be an object or an array , but was [" + currentToken + "]");
                        }
                    }
                }
            }
            for (String input : inputs) {
                if (inputMap.containsKey(input) == false || inputMap.get(input).weight < weight) {
                    inputMap.put(input, new CompletionInputMetadata(input, contextsMap, weight));
                }
            }
        } else {
            throw new ParsingException(parser.getTokenLocation(), "failed to parse [" + parser.currentName()
                + "]: expected text or object, but got " + token.name());
        }
    }

    @Override
    protected List<?> parseSourceValue(Object value, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }

        if (value instanceof List) {
            return (List<?>) value;
        } else {
            return List.of(value);
        }
    }

    static class CompletionInputMetadata {
        public final String input;
        public final Map<String, Set<String>> contexts;
        public final int weight;

        CompletionInputMetadata(String input, Map<String, Set<String>> contexts, int weight) {
            this.input = input;
            this.contexts = contexts;
            this.weight = weight;
        }

        @Override
        public String toString() {
            return input;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // no-op
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void doValidate(MappingLookup mappers) {
        if (fieldType().hasContextMappings()) {
            for (ContextMapping<?> contextMapping : fieldType().getContextMappings()) {
                contextMapping.validateReferences(indexVersionCreated, s -> mappers.fieldTypes().get(s));
            }
        }
    }
}
