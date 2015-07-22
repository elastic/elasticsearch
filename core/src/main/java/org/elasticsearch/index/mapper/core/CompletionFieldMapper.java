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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.suggest.xdocument.*;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.NumberType;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.object.ArrayValueMapperParser;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

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
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                return new OldCompletionFieldMapper.TypeParser().parse(name, node, parserContext);
            }
            CompletionFieldMapper.Builder builder = completionField(name);
            NamedAnalyzer indexAnalyzer = null;
            NamedAnalyzer searchAnalyzer = null;
            boolean preservePositionIncrements = Defaults.DEFAULT_POSITION_INCREMENTS;
            boolean preserveSeparators = Defaults.DEFAULT_PRESERVE_SEPARATORS;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                }
                if (parserContext.parseFieldMatcher().match(fieldName, Fields.ANALYZER)) {
                    indexAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.SEARCH_ANALYZER)) {
                    searchAnalyzer = getNamedAnalyzer(parserContext, fieldNode.toString());
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.PRESERVE_SEPARATORS)) {
                    preserveSeparators = Boolean.parseBoolean(fieldNode.toString());
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.PRESERVE_POSITION_INCREMENTS)) {
                    preservePositionIncrements = Boolean.parseBoolean(fieldNode.toString());
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.MAX_INPUT_LENGTH)) {
                    builder.maxInputLength(Integer.parseInt(fieldNode.toString()));
                    iterator.remove();
                } else if (parserContext.parseFieldMatcher().match(fieldName, Fields.CONTEXTS)) {
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
                indexAnalyzer = searchAnalyzer = parserContext.analysisService().analyzer("simple");
            } else if (searchAnalyzer == null) {
                searchAnalyzer = indexAnalyzer;
            }

            CompletionAnalyzer completionIndexAnalyzer = new CompletionAnalyzer(indexAnalyzer, preserveSeparators, preservePositionIncrements);
            CompletionAnalyzer completionSearchAnalyzer = new CompletionAnalyzer(searchAnalyzer, preserveSeparators, preservePositionIncrements);
            builder.indexAnalyzer(new NamedAnalyzer(indexAnalyzer.name(), indexAnalyzer.scope(), completionIndexAnalyzer));
            builder.searchAnalyzer(new NamedAnalyzer(searchAnalyzer.name(), searchAnalyzer.scope(), completionSearchAnalyzer));
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

    public static final class CompletionFieldType extends MappedFieldType {

        private static PostingsFormat postingsFormat;
        private ContextMappings contextMappings = null;

        public CompletionFieldType() {
            setFieldDataType(null);
        }

        private CompletionFieldType(CompletionFieldType ref) {
            super(ref);
            this.contextMappings = ref.contextMappings;
        }

        private void setContextMappings(ContextMappings contextMappings) {
            checkIfFrozen();
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
         * @return postings format to use for this field-type
         */
        public static synchronized PostingsFormat postingsFormat() {
            if (postingsFormat == null) {
                postingsFormat = new Completion50PostingsFormat();
            }
            return postingsFormat;
        }

        /**
         * Completion prefix query
         */
        public CompletionQuery prefixQuery(Object value) {
            return new PrefixCompletionQuery(searchAnalyzer().analyzer(), createTerm(value));
        }

        /**
         * Completion prefix regular expression query
         */
        public CompletionQuery regexpQuery(Object value, int flags, int maxDeterminizedStates) {
            return new RegexCompletionQuery(createTerm(value), flags, maxDeterminizedStates);
        }

        /**
         * Completion prefix fuzzy query
         */
        public CompletionQuery fuzzyQuery(String value, Fuzziness fuzziness, int nonFuzzyPrefixLength,
                                          int minFuzzyPrefixLength, int maxExpansions, boolean transpositions,
                                          boolean unicodeAware) {
            return new FuzzyCompletionQuery(searchAnalyzer().analyzer(), createTerm(value), null,
                    fuzziness.asDistance(), transpositions, nonFuzzyPrefixLength, minFuzzyPrefixLength,
                    unicodeAware, maxExpansions);
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
            CompletionFieldType other = (CompletionFieldType)fieldType;
            CompletionAnalyzer analyzer = (CompletionAnalyzer) indexAnalyzer().analyzer();
            CompletionAnalyzer otherAnalyzer = (CompletionAnalyzer) other.indexAnalyzer().analyzer();

            if (analyzer.preservePositionIncrements() != otherAnalyzer.preservePositionIncrements()) {
                conflicts.add("mapper [" + names().fullName() + "] has different 'preserve_position_increments' values");
            }
            if (analyzer.preserveSep() != otherAnalyzer.preserveSep()) {
                conflicts.add("mapper [" + names().fullName() + "] has different 'preserve_separators' values");
            }
            if (hasContextMappings() != other.hasContextMappings()) {
                conflicts.add("mapper [" + names().fullName() + "] has different context mapping");
            } else if (hasContextMappings() && contextMappings.equals(other.contextMappings) == false) {
                conflicts.add("mapper [" + names().fullName() + "] has different 'context_mappings' values");
            }
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }

        @Override
        public boolean isSortable() {
            return false;
        }

    }

    /**
     * Builder for {@link CompletionFieldMapper}
     */
    public static class Builder extends FieldMapper.Builder<Builder, CompletionFieldMapper> {

        private int maxInputLength = Defaults.DEFAULT_MAX_INPUT_LENGTH;
        private ContextMappings contextMappings = null;

        /**
         * @param name of the completion field to build
         */
        public Builder(String name) {
            super(name, new CompletionFieldType());
            builder = this;
        }

        /**
         * @param maxInputLength maximum expected prefix length
         *                       NOTE: prefixes longer than this will
         *                       be truncated
         */
        public Builder maxInputLength(int maxInputLength) {
            if (maxInputLength <= 0) {
                throw new IllegalArgumentException(Fields.MAX_INPUT_LENGTH.getPreferredName() + " must be > 0 but was [" + maxInputLength + "]");
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

        @Override
        public CompletionFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            ((CompletionFieldType) fieldType).setContextMappings(contextMappings);
            return new CompletionFieldMapper(name, fieldType, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo, maxInputLength);
        }
    }

    private int maxInputLength;

    public CompletionFieldMapper(String simpleName, MappedFieldType fieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo, int maxInputLength) {
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
     *   "ARRAY" - each element can be one of {@link #parse(ParseContext, Token, XContentParser, CompletionInputs)}
     *   "OBJECT" - see {@link #parse(ParseContext, Token, XContentParser, CompletionInputs)}
     *
     * Indexing:
     *  if context mappings are defined, delegates to {@link ContextMappings#addFields(ParseContext.Document, String, String, int, Map)}
     *  else adds inputs as a {@link org.apache.lucene.search.suggest.xdocument.SuggestField}
     */
    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // parse
        XContentParser parser = context.parser();
        Token token = parser.currentToken();
        CompletionInputs completionInputs = new CompletionInputs();
        if (token == Token.VALUE_NULL) {
            throw new MapperParsingException("completion field [" + fieldType().names().fullName() + "] does not support null values");
        } else if (token == Token.VALUE_STRING) {
            completionInputs.add(parser.text(), 1, Collections.<String, Set<CharSequence>>emptyMap());
        } else if (token == Token.START_ARRAY) {
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                parse(context, token, parser, completionInputs);
            }
        } else {
            parse(context, token, parser, completionInputs);
        }

        // index
        for (Map.Entry<String, CompletionInputs.CompletionInputMetaData> completionInput : completionInputs) {
            String input = completionInput.getKey();
            if (input.length() > maxInputLength) {
                final int len = correctSubStringLen(input, Math.min(maxInputLength, input.length()));
                input = input.substring(0, len);
            }
            CompletionInputs.CompletionInputMetaData metaData = completionInput.getValue();
            if (fieldType().hasContextMappings()) {
                fieldType().getContextMappings().addFields(context.doc(), fieldType().names().indexName(),
                        input, metaData.weight, metaData.contexts);
            } else {
                context.doc().add(new org.apache.lucene.search.suggest.xdocument.SuggestField(fieldType().names().indexName(), input, metaData.weight));
            }
        }
        multiFields.parse(this, context);
        return null;
    }

    /**
     * Acceptable inputs:
     *  "STRING" - interpreted as the field value (input)
     *  "OBJECT" - { "input": STRING|ARRAY, "weight": STRING|INT, "contexts": ARRAY|OBJECT }
     *
     * NOTE: for "contexts" parsing see {@link ContextMappings#parseContext(ParseContext, XContentParser)}
     */
    private void parse(ParseContext parseContext, Token token, XContentParser parser, CompletionInputs completionInputs) throws IOException {
        Set<String> inputs = new HashSet<>();
        Map<String, Set<CharSequence>> contextsMap = new HashMap<>();
        int weight = 0;
        String currentFieldName = null;
        ContextMappings contextMappings = fieldType().getContextMappings();
        if (token == Token.VALUE_STRING) {
            inputs.add(parser.text());
        } else if (token == Token.START_OBJECT) {
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    if (!ALLOWED_CONTENT_FIELD_NAMES.contains(currentFieldName)) {
                        throw new IllegalArgumentException("Unknown field name[" + currentFieldName + "], must be one of " + ALLOWED_CONTENT_FIELD_NAMES);
                    }
                } else if (token == Token.VALUE_STRING) {
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        inputs.add(parser.text());
                    } else if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        Number weightValue;
                        try {
                            weightValue = Long.parseLong(parser.text());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Weight must be a string representing a numeric value, but was [" + parser.text() + "]");
                        }
                        checkWeight(weightValue.longValue());// always parse a long to make sure we don't get overflow
                        weight = weightValue.intValue();
                    }
                } else if (token == Token.VALUE_NUMBER) {
                    if (Fields.CONTENT_FIELD_NAME_WEIGHT.equals(currentFieldName)) {
                        NumberType numberType = parser.numberType();
                        if (NumberType.LONG != numberType && NumberType.INT != numberType) {
                            throw new IllegalArgumentException("Weight must be an integer, but was [" + parser.numberValue() + "]");
                        }
                        checkWeight(parser.longValue()); // always parse a long to make sure we don't get overflow
                        weight = parser.intValue();
                    }
                } else if (token == Token.START_ARRAY) {
                    if (Fields.CONTENT_FIELD_NAME_INPUT.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            inputs.add(parser.text());
                        }
                    } else if (Fields.CONTENT_FIELD_NAME_CONTEXTS.equals(currentFieldName)) {
                        if (fieldType().hasContextMappings() == false) {
                            throw new IllegalArgumentException("Supplied context(s) to a non context enabled field: [" + fieldType().names().fullName() + "]");
                        }
                        addContexts(contextsMap, contextMappings.parseContext(parseContext, parser));
                    }
                } else if (token == Token.START_OBJECT) {
                    if (Fields.CONTENT_FIELD_NAME_CONTEXTS.equals(currentFieldName)) {
                        if (fieldType().hasContextMappings() == false) {
                            throw new IllegalArgumentException("Supplied context(s) to a non context enabled field: [" + fieldType().names().fullName() + "]");
                        }
                        addContexts(contextsMap, contextMappings.parseContext(parseContext, parser));
                    }
                }
            }
        } else {
            throw new ElasticsearchParseException("failed to parse expected text or object got" + token.name());
        }
        completionInputs.add(inputs, weight, contextsMap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName())
                .field(Fields.TYPE.getPreferredName(), CONTENT_TYPE);
        builder.field(Fields.ANALYZER.getPreferredName(), fieldType().indexAnalyzer().name());
        if (fieldType().indexAnalyzer().name().equals(fieldType().searchAnalyzer().name()) == false) {
            builder.field(Fields.SEARCH_ANALYZER.getPreferredName(), fieldType().searchAnalyzer().name());
        }
        CompletionAnalyzer analyzer = (CompletionAnalyzer) fieldType().indexAnalyzer().analyzer();
        builder.field(Fields.PRESERVE_SEPARATORS.getPreferredName(), analyzer.preserveSep());
        builder.field(Fields.PRESERVE_POSITION_INCREMENTS.getPreferredName(), analyzer.preservePositionIncrements());
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
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        // no-op
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        CompletionFieldMapper fieldMergeWith = (CompletionFieldMapper) mergeWith;
        if (!mergeResult.simulate()) {
            this.maxInputLength = fieldMergeWith.maxInputLength;
        }
    }

    private static void addContexts(Map<String, Set<CharSequence>> contextsMap, Map<String, Set<CharSequence>> partialContextsMap) {
        for (Map.Entry<String, Set<CharSequence>> context : partialContextsMap.entrySet()) {
            Set<CharSequence> contexts = contextsMap.get(context.getKey());
            if (contexts == null) {
                contexts = context.getValue();
            } else {
                contexts.addAll(context.getValue());
            }
            contextsMap.put(context.getKey(), contexts);
        }
    }

    private static class CompletionInputs implements Iterable<Map.Entry<String, CompletionInputs.CompletionInputMetaData>> {
        Map<String, CompletionInputMetaData> inputs = Maps.newHashMapWithExpectedSize(4);

        static class CompletionInputMetaData {
            public final Map<String, Set<CharSequence>> contexts;
            public final int weight;

            CompletionInputMetaData(Map<String, Set<CharSequence>> contexts, int weight) {
                this.contexts = contexts;
                this.weight = weight;
            }
        }

        void add(Set<String> inputs, int weight, Map<String, Set<CharSequence>> contexts) {
            for (String input : inputs) {
                add(input, weight, contexts);
            }
        }

        void add(String input, int weight, Map<String, Set<CharSequence>> contexts) {
            if (inputs.containsKey(input)) {
                if (inputs.get(input).weight < weight) {
                    inputs.put(input, new CompletionInputMetaData(contexts, weight));
                }
            } else {
                inputs.put(input, new CompletionInputMetaData(contexts, weight));
            }
        }

        @Override
        public Iterator<Map.Entry<String, CompletionInputMetaData>> iterator() {
            return inputs.entrySet().iterator();
        }
    }

    public static int correctSubStringLen(String input, int len) {
        if (Character.isHighSurrogate(input.charAt(len - 1))) {
            assert input.length() >= len + 1 && Character.isLowSurrogate(input.charAt(len));
            return len + 1;
        }
        return len;
    }

    private static void checkWeight(Number weight) {
        if (weight.longValue() < 0 || weight.longValue() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Weight must be in the interval [0..2147483647], but was [" + weight + "]");
        }
    }

}
