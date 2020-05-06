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
import org.apache.lucene.codecs.PostingsFormat;
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.mapper.TypeParsers.parseMultiField;

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
public class CompletionFieldMapper extends FieldMapper implements ArrayValueMapperParser {
    public static final String CONTENT_TYPE = "completion";

    /**
     * Maximum allowed number of completion contexts in a mapping.
     */
    static final int COMPLETION_CONTEXTS_LIMIT = 10;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new CompletionFieldType();
        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
        public static final boolean DEFAULT_PRESERVE_SEPARATORS = true;
        public static final boolean DEFAULT_POSITION_INCREMENTS = true;
        public static final int DEFAULT_MAX_INPUT_LENGTH = 50;
    }

    public static class Fields {
        // Mapping field names
        public static final ParseField ANALYZER = new ParseField("analyzer");
        public static final ParseField SEARCH_ANALYZER = new ParseField("search_analyzer");
        public static final ParseField PRESERVE_SEPARATORS = new ParseField("preserve_separators");
        public static final ParseField PRESERVE_POSITION_INCREMENTS = new ParseField("preserve_position_increments");
        public static final ParseField TYPE = new ParseField("type");
        public static final ParseField CONTEXTS = new ParseField("contexts");
        public static final ParseField MAX_INPUT_LENGTH = new ParseField("max_input_length", "max_input_len");
        // Content field names
        public static final String CONTENT_FIELD_NAME_INPUT = "input";
        public static final String CONTENT_FIELD_NAME_WEIGHT = "weight";
        public static final String CONTENT_FIELD_NAME_CONTEXTS = "contexts";
    }

    public static final Set<String> ALLOWED_CONTENT_FIELD_NAMES = Sets.newHashSet(Fields.CONTENT_FIELD_NAME_INPUT,
            Fields.CONTENT_FIELD_NAME_WEIGHT, Fields.CONTENT_FIELD_NAME_CONTEXTS);

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            CompletionFieldMapper.Builder builder = new CompletionFieldMapper.Builder(name);
            NamedAnalyzer indexAnalyzer = null;
            NamedAnalyzer searchAnalyzer = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                }
                if (Fields.ANALYZER.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    indexAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (Fields.SEARCH_ANALYZER.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    searchAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (Fields.PRESERVE_SEPARATORS.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    builder.preserveSeparators(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (Fields.PRESERVE_POSITION_INCREMENTS.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    builder.preservePositionIncrements(Boolean.parseBoolean(fieldNode.toString()));
                    iterator.remove();
                } else if (Fields.MAX_INPUT_LENGTH.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    builder.maxInputLength(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (Fields.CONTEXTS.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    builder.contextMappings(ContextMappings.load(fieldNode, parserContext.indexVersionCreated()));
                    iterator.remove();
                } else if (parseMultiField(builder, name, parserContext, fieldName, fieldNode)) {
                    iterator.remove();
                }
            }

            if (indexAnalyzer == null) {
                if (searchAnalyzer != null) {
                    throw new MapperParsingException("analyzer on completion field [" + name + "] must be set when search_analyzer is set");
                }
                indexAnalyzer = searchAnalyzer = parserContext.getIndexAnalyzers().get("simple");
            } else if (searchAnalyzer == null) {
                searchAnalyzer = indexAnalyzer;
            }

            builder.indexAnalyzer(indexAnalyzer);
            builder.searchAnalyzer(searchAnalyzer);
            return builder;
        }

        private NamedAnalyzer getNamedAnalyzer(ParserContext parserContext, String name) {
            NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().get(name);
            if (analyzer == null) {
                throw new IllegalArgumentException("Can't find default or mapped analyzer with name [" + name + "]");
            }
            return analyzer;
        }
    }

    public static final class CompletionFieldType extends TermBasedFieldType {

        private static PostingsFormat postingsFormat;

        private boolean preserveSep = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;
        private ContextMappings contextMappings = null;

        public CompletionFieldType() {
        }

        private CompletionFieldType(CompletionFieldType ref) {
            super(ref);
            this.contextMappings = ref.contextMappings;
            this.preserveSep = ref.preserveSep;
            this.preservePositionIncrements = ref.preservePositionIncrements;
        }

        public void setPreserveSep(boolean preserveSep) {
            checkIfFrozen();
            this.preserveSep = preserveSep;
        }

        public void setPreservePositionIncrements(boolean preservePositionIncrements) {
            checkIfFrozen();
            this.preservePositionIncrements = preservePositionIncrements;
        }

        public void setContextMappings(ContextMappings contextMappings) {
            checkIfFrozen();
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

        @Override
        public NamedAnalyzer searchAnalyzer() {
            final NamedAnalyzer searchAnalyzer = super.searchAnalyzer();
            if (searchAnalyzer != null && !(searchAnalyzer.analyzer() instanceof CompletionAnalyzer)) {
                return new NamedAnalyzer(searchAnalyzer.name(), AnalyzerScope.INDEX,
                        new CompletionAnalyzer(searchAnalyzer, preserveSep, preservePositionIncrements));
            }
            return searchAnalyzer;
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

        public boolean preserveSep() {
            return preserveSep;
        }

        public boolean preservePositionIncrements() {
            return preservePositionIncrements;
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
            return new PrefixCompletionQuery(searchAnalyzer().analyzer(), new Term(name(), indexedValueForSearch(value)));
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
            return new FuzzyCompletionQuery(searchAnalyzer().analyzer(), new Term(name(), indexedValueForSearch(value)), null,
                    fuzziness.asDistance(), transpositions, nonFuzzyPrefixLength, minFuzzyPrefixLength,
                    unicodeAware, maxExpansions);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            CompletionFieldType that = (CompletionFieldType) o;

            if (preserveSep != that.preserveSep) return false;
            if (preservePositionIncrements != that.preservePositionIncrements) return false;
            return !(contextMappings != null ? !contextMappings.equals(that.contextMappings) : that.contextMappings != null);

        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(),
                    preserveSep,
                    preservePositionIncrements,
                    contextMappings);
        }

        @Override
        public CompletionFieldType clone() {
            return new CompletionFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

    }

    /**
     * Builder for {@link CompletionFieldMapper}
     */
    public static class Builder extends FieldMapper.Builder<Builder> {

        private int maxInputLength = Defaults.DEFAULT_MAX_INPUT_LENGTH;
        private ContextMappings contextMappings = null;
        private boolean preserveSeparators = Defaults.DEFAULT_PRESERVE_SEPARATORS;
        private boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;

        private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(Builder.class));

        /**
         * @param name of the completion field to build
         */
        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        /**
         * @param maxInputLength maximum expected prefix length
         *                       NOTE: prefixes longer than this will
         *                       be truncated
         */
        public Builder maxInputLength(int maxInputLength) {
            if (maxInputLength <= 0) {
                throw new IllegalArgumentException(Fields.MAX_INPUT_LENGTH.getPreferredName()
                    + " must be > 0 but was [" + maxInputLength + "]");
            }
            this.maxInputLength = maxInputLength;
            return this;
        }

        /**
         * Add context mapping to this field
         * @param contextMappings see {@link ContextMappings#load(Object, Version)}
         */
        public Builder contextMappings(ContextMappings contextMappings) {
            this.contextMappings = contextMappings;
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
        public CompletionFieldMapper build(BuilderContext context) {
            checkCompletionContextsLimit(context);
            setupFieldType(context);
            CompletionFieldType completionFieldType = (CompletionFieldType) this.fieldType;
            completionFieldType.setContextMappings(contextMappings);
            completionFieldType.setPreservePositionIncrements(preservePositionIncrements);
            completionFieldType.setPreserveSep(preserveSeparators);
            return new CompletionFieldMapper(name, this.fieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo, maxInputLength);
        }

        private void checkCompletionContextsLimit(BuilderContext context) {
            if (this.contextMappings != null && this.contextMappings.size() > COMPLETION_CONTEXTS_LIMIT) {
                if (context.indexCreatedVersion().onOrAfter(Version.V_8_0_0)) {
                    throw new IllegalArgumentException(
                        "Limit of completion field contexts [" + COMPLETION_CONTEXTS_LIMIT + "] has been exceeded");
                } else {
                    deprecationLogger.deprecatedAndMaybeLog("excessive_completion_contexts",
                        "You have defined more than [" + COMPLETION_CONTEXTS_LIMIT + "] completion contexts" +
                        " in the mapping for index [" + context.indexSettings().get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME) + "]. " +
                        "The maximum allowed number of completion contexts in a mapping will be limited to " +
                        "[" + COMPLETION_CONTEXTS_LIMIT + "] starting in version [8.0].");
                }
            }
        }
    }

    private int maxInputLength;

    public CompletionFieldMapper(String simpleName, MappedFieldType fieldType, Settings indexSettings,
                                 MultiFields multiFields, CopyTo copyTo, int maxInputLength) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, multiFields, copyTo);
        this.maxInputLength = maxInputLength;
    }

    @Override
    public CompletionFieldType fieldType() {
        return (CompletionFieldType) super.fieldType();
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
                context.addIgnoredField(fieldType.name());
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
    protected List<?> parseSourceValue(Object value) {
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName())
                .field(Fields.TYPE.getPreferredName(), CONTENT_TYPE);
        builder.field(Fields.ANALYZER.getPreferredName(), fieldType().indexAnalyzer().name());
        if (fieldType().indexAnalyzer().name().equals(fieldType().searchAnalyzer().name()) == false) {
            builder.field(Fields.SEARCH_ANALYZER.getPreferredName(), fieldType().searchAnalyzer().name());
        }
        builder.field(Fields.PRESERVE_SEPARATORS.getPreferredName(), fieldType().preserveSep());
        builder.field(Fields.PRESERVE_POSITION_INCREMENTS.getPreferredName(), fieldType().preservePositionIncrements());
        builder.field(Fields.MAX_INPUT_LENGTH.getPreferredName(), this.maxInputLength);

        if (fieldType().hasContextMappings()) {
            builder.startArray(Fields.CONTEXTS.getPreferredName());
            fieldType().getContextMappings().toXContent(builder, params);
            builder.endArray();
        }

        multiFields.toXContent(builder, params);
        return builder.endObject();
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
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        CompletionFieldType c = (CompletionFieldType)other.fieldType();

        if (fieldType().preservePositionIncrements != c.preservePositionIncrements) {
            conflicts.add("mapper [" + name() + "] has different [preserve_position_increments] values");
        }
        if (fieldType().preserveSep != c.preserveSep) {
            conflicts.add("mapper [" + name() + "] has different [preserve_separators] values");
        }
        if (fieldType().hasContextMappings() != c.hasContextMappings()) {
            conflicts.add("mapper [" + name() + "] has different [context_mappings] values");
        } else if (fieldType().hasContextMappings() && fieldType().contextMappings.equals(c.contextMappings) == false) {
            conflicts.add("mapper [" + name() + "] has different [context_mappings] values");
        }

        this.maxInputLength = ((CompletionFieldMapper)other).maxInputLength;
    }

}
