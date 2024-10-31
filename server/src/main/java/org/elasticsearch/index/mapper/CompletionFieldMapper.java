/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.suggest.document.CompletionAnalyzer;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.search.suggest.document.PrefixCompletionQuery;
import org.apache.lucene.search.suggest.document.RegexCompletionQuery;
import org.apache.lucene.search.suggest.document.SuggestField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.FilterXContentParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.NumberType;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.ArrayList;
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
 * see {@link ContextMappings#load(Object)} for CONTEXTS<br>
 * see {@link #parse(DocumentParserContext)} for acceptable inputs for indexing<br>
 * <p>
 *  This field type constructs completion queries that are run
 *  against the weighted FST index by the {@link CompletionSuggester}.
 *  This field can also be extended to add search criteria to suggestions
 *  for query-time filtering and boosting (see {@link ContextMappings}
 */
public class CompletionFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "completion";

    /**
     * Maximum allowed number of completion contexts in a mapping.
     */
    static final int COMPLETION_CONTEXTS_LIMIT = 10;

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), builder.defaultAnalyzer, builder.indexVersionCreated).init(this);
    }

    public static class Defaults {
        public static final FieldType FIELD_TYPE;
        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            ft.setOmitNorms(true);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
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

    private static Builder builder(FieldMapper in) {
        return ((CompletionFieldMapper) in).builder;
    }

    /**
     * Builder for {@link CompletionFieldMapper}
     */
    public static class Builder extends FieldMapper.Builder {

        private final Parameter<NamedAnalyzer> analyzer;
        private final Parameter<NamedAnalyzer> searchAnalyzer;
        private final Parameter<Boolean> preserveSeparators = Parameter.boolParam(
            "preserve_separators",
            false,
            m -> builder(m).preserveSeparators.get(),
            Defaults.DEFAULT_PRESERVE_SEPARATORS
        ).alwaysSerialize();
        private final Parameter<Boolean> preservePosInc = Parameter.boolParam(
            "preserve_position_increments",
            false,
            m -> builder(m).preservePosInc.get(),
            Defaults.DEFAULT_POSITION_INCREMENTS
        ).alwaysSerialize();
        private final Parameter<ContextMappings> contexts = new Parameter<>(
            "contexts",
            false,
            () -> null,
            (n, c, o) -> ContextMappings.load(o),
            m -> builder(m).contexts.get(),
            (b, n, c) -> {
                if (c == null) {
                    return;
                }
                b.startArray(n);
                c.toXContent(b, ToXContent.EMPTY_PARAMS);
                b.endArray();
            },
            Objects::toString
        );
        private final Parameter<Integer> maxInputLength = Parameter.intParam(
            "max_input_length",
            true,
            m -> builder(m).maxInputLength.get(),
            Defaults.DEFAULT_MAX_INPUT_LENGTH
        ).addDeprecatedName("max_input_len").addValidator(Builder::validateInputLength).alwaysSerialize();
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final NamedAnalyzer defaultAnalyzer;
        private final IndexVersion indexVersionCreated;

        private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Builder.class);

        /**
         * @param name of the completion field to build
         */
        public Builder(String name, NamedAnalyzer defaultAnalyzer, IndexVersion indexVersionCreated) {
            super(name);
            this.defaultAnalyzer = defaultAnalyzer;
            this.indexVersionCreated = indexVersionCreated;
            this.analyzer = Parameter.analyzerParam("analyzer", false, m -> builder(m).analyzer.get(), () -> defaultAnalyzer)
                .alwaysSerialize();
            this.searchAnalyzer = Parameter.analyzerParam(
                "search_analyzer",
                true,
                m -> builder(m).searchAnalyzer.get(),
                analyzer::getValue
            );
        }

        private static void validateInputLength(int maxInputLength) {
            if (maxInputLength <= 0) {
                throw new IllegalArgumentException("[max_input_length] must be > 0 but was [" + maxInputLength + "]");
            }
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { analyzer, searchAnalyzer, preserveSeparators, preservePosInc, maxInputLength, contexts, meta };
        }

        NamedAnalyzer buildAnalyzer() {
            return new NamedAnalyzer(
                analyzer.get().name(),
                AnalyzerScope.INDEX,
                new CompletionAnalyzer(analyzer.get(), preserveSeparators.get(), preservePosInc.get())
            );
        }

        @Override
        public CompletionFieldMapper build(MapperBuilderContext context) {
            checkCompletionContextsLimit();
            NamedAnalyzer completionAnalyzer = new NamedAnalyzer(
                this.searchAnalyzer.getValue().name(),
                AnalyzerScope.INDEX,
                new CompletionAnalyzer(this.searchAnalyzer.getValue(), preserveSeparators.getValue(), preservePosInc.getValue())
            );

            CompletionFieldType ft = new CompletionFieldType(context.buildFullName(leafName()), completionAnalyzer, meta.getValue());
            ft.setContextMappings(contexts.getValue());
            return new CompletionFieldMapper(leafName(), ft, builderParams(this, context), this);
        }

        private void checkCompletionContextsLimit() {
            if (this.contexts.getValue() != null && this.contexts.getValue().size() > COMPLETION_CONTEXTS_LIMIT) {
                throw new IllegalArgumentException(
                    "Limit of completion field contexts [" + COMPLETION_CONTEXTS_LIMIT + "] has been exceeded"
                );
            }
        }
    }

    public static final Set<String> ALLOWED_CONTENT_FIELD_NAMES = Set.of(
        Fields.CONTENT_FIELD_NAME_INPUT,
        Fields.CONTENT_FIELD_NAME_WEIGHT,
        Fields.CONTENT_FIELD_NAME_CONTEXTS
    );

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.getIndexAnalyzers().get("simple"), c.indexVersionCreated())
    );

    public static final class CompletionFieldType extends TermBasedFieldType {

        private ContextMappings contextMappings = null;

        public CompletionFieldType(String name, NamedAnalyzer searchAnalyzer, Map<String, String> meta) {
            super(name, true, false, false, new TextSearchInfo(Defaults.FIELD_TYPE, null, searchAnalyzer, searchAnalyzer), meta);
        }

        public void setContextMappings(ContextMappings contextMappings) {
            this.contextMappings = contextMappings;
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
         * Completion prefix query
         */
        public CompletionQuery prefixQuery(Object value) {
            return new PrefixCompletionQuery(
                getTextSearchInfo().searchAnalyzer().analyzer(),
                new Term(name(), indexedValueForSearch(value))
            );
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
        public CompletionQuery fuzzyQuery(
            String value,
            Fuzziness fuzziness,
            int nonFuzzyPrefixLength,
            int minFuzzyPrefixLength,
            int maxExpansions,
            boolean transpositions,
            boolean unicodeAware
        ) {
            return new FuzzyCompletionQuery(
                getTextSearchInfo().searchAnalyzer().analyzer(),
                new Term(name(), indexedValueForSearch(value)),
                null,
                fuzziness.asDistance(),
                transpositions,
                nonFuzzyPrefixLength,
                minFuzzyPrefixLength,
                unicodeAware,
                maxExpansions
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected List<?> parseSourceValue(Object value) {
                    if (value instanceof List) {
                        return (List<?>) value;
                    } else {
                        return List.of(value);
                    }
                }
            };
        }

    }

    private final int maxInputLength;
    private final Builder builder;

    private final NamedAnalyzer indexAnalyzer;

    public CompletionFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams builderParams, Builder builder) {
        super(simpleName, mappedFieldType, builderParams);
        this.builder = builder;
        this.maxInputLength = builder.maxInputLength.getValue();
        this.indexAnalyzer = builder.buildAnalyzer();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    public CompletionFieldType fieldType() {
        return (CompletionFieldType) super.fieldType();
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    int getMaxInputLength() {
        return builder.maxInputLength.get();
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
     *  if context mappings are defined, delegates to {@link ContextMappings#addField(LuceneDocument, String, String, int, Map)}
     *  else adds inputs as a {@link org.apache.lucene.search.suggest.document.SuggestField}
     */
    @Override
    public void parse(DocumentParserContext context) throws IOException {
        // parse
        XContentParser parser = context.parser();
        Token token = parser.currentToken();
        Map<String, CompletionInputMetadata> inputMap = Maps.newMapWithExpectedSize(1);

        if (token == Token.VALUE_NULL) { // ignore null values
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
                int len = maxInputLength;
                if (Character.isHighSurrogate(input.charAt(len - 1))) {
                    assert input.length() >= len + 1 && Character.isLowSurrogate(input.charAt(len));
                    len += 1;
                }
                input = input.substring(0, len);
            }
            CompletionInputMetadata metadata = completionInput.getValue();
            if (fieldType().hasContextMappings()) {
                fieldType().getContextMappings().addField(context.doc(), fieldType().name(), input, metadata.weight, metadata.contexts);
            } else {
                context.doc().add(new SuggestField(fieldType().name(), input, metadata.weight));
            }
        }

        context.addToFieldNames(fieldType().name());
        for (CompletionInputMetadata metadata : inputMap.values()) {
            multiFields().parse(
                this,
                context,
                () -> context.switchParser(new MultiFieldParser(metadata, fieldType().name(), context.parser().getTokenLocation()))
            );
        }
    }

    /**
     * Acceptable inputs:
     *  "STRING" - interpreted as the field value (input)
     *  "OBJECT" - { "input": STRING|ARRAY, "weight": STRING|INT, "contexts": ARRAY|OBJECT }
     */
    private void parse(
        DocumentParserContext documentParserContext,
        Token token,
        XContentParser parser,
        Map<String, CompletionInputMetadata> inputMap
    ) throws IOException {
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
                    if (ALLOWED_CONTENT_FIELD_NAMES.contains(currentFieldName) == false) {
                        throw new IllegalArgumentException(
                            "unknown field name [" + currentFieldName + "], must be one of " + ALLOWED_CONTENT_FIELD_NAMES
                        );
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
                                    throw new IllegalArgumentException(
                                        "input array must have string values, but was [" + token.name() + "]"
                                    );
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
                            throw new IllegalArgumentException(
                                "weight must be in the interval [0..2147483647], but was [" + weightValue.longValue() + "]"
                            );
                        }
                        weight = weightValue.intValue();
                    } else if (Fields.CONTENT_FIELD_NAME_CONTEXTS.equals(currentFieldName)) {
                        if (fieldType().hasContextMappings() == false) {
                            throw new IllegalArgumentException("contexts field is not supported for field: [" + fieldType().name() + "]");
                        }
                        ContextMappings contextMappings = fieldType().getContextMappings();
                        XContentParser.Token currentToken = parser.currentToken();
                        if (currentToken == XContentParser.Token.START_OBJECT) {
                            ContextMapping<?> contextMapping = null;
                            String fieldName = null;
                            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (currentToken == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                    contextMapping = contextMappings.get(fieldName);
                                } else {
                                    assert fieldName != null;
                                    assert contextsMap.containsKey(fieldName) == false;
                                    contextsMap.put(fieldName, contextMapping.parseContext(documentParserContext, parser));
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
            throw new ParsingException(
                parser.getTokenLocation(),
                "failed to parse [" + parser.currentName() + "]: expected text or object, but got " + token.name()
            );
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

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("input", input);
            map.put("weight", weight);
            if (contexts.isEmpty() == false) {
                Map<String, List<String>> contextsAsList = new HashMap<>();
                contexts.forEach((k, v) -> {
                    List<String> l = new ArrayList<>(v);
                    contextsAsList.put(k, l);
                });
                map.put("contexts", contextsAsList);
            }
            return map;
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
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
                contextMapping.validateReferences(builder.indexVersionCreated, s -> mappers.fieldTypesLookup().get(s));
            }
        }
    }

    /**
     * Parser that exposes the expected format depending on the type of multi-field that is consuming content.
     * Completion fields can hold multi-fields, which can either parse a simple string value or an object in case of another completion
     * field. This parser detects which of the two is parsing content and exposes the full object when needed (including input, weight
     * and context if available), otherwise the input value only.
     *
     * A few assumptions are made that make this work:
     * 1) only string values are supported for a completion field, hence only sub-fields that parse strings are supported
     * 2) sub-fields that parse simple values only ever call {@link #textOrNull()} to do so. They may call {@link #currentToken()} only to
     * check if there's a null value, which is irrelevant in the multi-fields scenario as null values are ignored in the parent field and
     * don't lead to any field creation.
     * 3) completion is the only sub-field type that may be parsing the object structure.
     *
     * The parser is set to expose by default simple value, unless {@link #nextToken()} is called which is what signals that the
     * consumer supports the object structure.
     */
    // This parser changes behaviour depending on which methods are called by consumers, which is extremely delicate. This kind of works for
    // our internal mappers, but what about mappers from plugins?
    static class MultiFieldParser extends FilterXContentParser {
        private final String textValue;
        private final String fieldName;
        private final XContentLocation locationOffset;
        private final XContentParser fullObjectParser;
        // we assume that the consumer is parsing values, we will switch to exposing the object format if nextToken is called
        private boolean parsingObject = false;

        MultiFieldParser(CompletionInputMetadata metadata, String fieldName, XContentLocation locationOffset) {
            this.fullObjectParser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                metadata.toMap(),
                XContentType.JSON
            );
            this.fieldName = fieldName;
            this.locationOffset = locationOffset;
            this.textValue = metadata.input;
        }

        @Override
        protected XContentParser delegate() {
            // if consumers are only reading values, they should never go through delegate and rather call the
            // overridden currentToken and textOrNull below that don't call super
            assert parsingObject;
            return fullObjectParser;
        }

        @Override
        public Token currentToken() {
            if (parsingObject == false) {
                // nextToken has not been called, it may or may not be called at a later time.
                // What we return does not really matter for mappers that support simple values, as they only check for VALUE_NULL.
                // For mappers that do support objects, START_OBJECT is a good choice.
                return Token.START_OBJECT;
            }
            return super.currentToken();
        }

        @Override
        public String textOrNull() throws IOException {
            if (parsingObject == false) {
                return textValue;
            }
            return super.textOrNull();
        }

        @Override
        public Token nextToken() throws IOException {
            if (parsingObject == false) {
                // a completion sub-field is parsing
                parsingObject = true;
                // move to START_OBJECT, currentToken has already returned START_OBJECT and we will advance one token further just below
                this.fullObjectParser.nextToken();
            }
            return super.nextToken();
        }

        @Override
        public String currentName() throws IOException {
            if (parsingObject == false) {
                return fieldName;
            }
            String currentName = super.currentName();
            if (currentName == null && currentToken() == Token.END_OBJECT) {
                return fieldName;
            }
            return currentName;
        }

        @Override
        public XContentLocation getTokenLocation() {
            // return fixed token location: it's not possible to match the token location while parsing through the object structure,
            // because completion metadata have been rewritten hence they won't match the incoming document
            return locationOffset;
        }
    }
}
