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
package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.elasticsearch.search.suggest.completion.context.QueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Defines a suggest command based on a prefix, typically to provide "auto-complete" functionality
 * for users as they type search terms. The implementation of the completion service uses FSTs that
 * are created at index-time and so must be defined in the mapping with the type "completion" before
 * indexing.
 */
public class CompletionSuggestionBuilder extends SuggestionBuilder<CompletionSuggestionBuilder> {

    public static final CompletionSuggestionBuilder PROTOTYPE = new CompletionSuggestionBuilder("_na_");
    static final String SUGGESTION_NAME = "completion";
    static final ParseField PAYLOAD_FIELD = new ParseField("payload");
    static final ParseField CONTEXTS_FIELD = new ParseField("contexts", "context");

    private static ObjectParser<CompletionSuggestionBuilder, Void> TLP_PARSER =
        new ObjectParser<>(CompletionSuggestionBuilder.SUGGESTION_NAME, null);
    static {
        TLP_PARSER.declareStringArray(CompletionSuggestionBuilder::payload, CompletionSuggestionBuilder.PAYLOAD_FIELD);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) -> {
                if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                    if (parser.booleanValue()) {
                        completionSuggestionContext.fuzzyOptions = new FuzzyOptions.Builder().build();
                    }
                } else {
                    completionSuggestionContext.fuzzyOptions = FuzzyOptions.parse(parser);
                }
            },
            FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) ->
            completionSuggestionContext.regexOptions = RegexOptions.parse(parser),
            RegexOptions.REGEX_OPTIONS, ObjectParser.ValueType.OBJECT);
        TLP_PARSER.declareString(CompletionSuggestionBuilder::field, SuggestUtils.Fields.FIELD);
        TLP_PARSER.declareString(CompletionSuggestionBuilder::analyzer, SuggestUtils.Fields.ANALYZER);
        TLP_PARSER.declareInt(CompletionSuggestionBuilder::size, SuggestUtils.Fields.SIZE);
        TLP_PARSER.declareInt(CompletionSuggestionBuilder::shardSize, SuggestUtils.Fields.SHARD_SIZE);
        TLP_PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.copyCurrentStructure(p);
            v.contextBytes = builder.bytes();
            p.skipChildren();
        }, CompletionSuggestionBuilder.CONTEXTS_FIELD, ObjectParser.ValueType.OBJECT); // context is deprecated
    }

    private FuzzyOptions fuzzyOptions;
    private RegexOptions regexOptions;
    private BytesReference contextBytes = null;
    private List<String> payloadFields = Collections.emptyList();

    public CompletionSuggestionBuilder(String fieldname) {
        super(fieldname);
    }

    /**
     * Sets the prefix to provide completions for.
     * The prefix gets analyzed by the suggest analyzer.
     */
    @Override
    public CompletionSuggestionBuilder prefix(String prefix) {
        super.prefix(prefix);
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with fuzziness of <code>fuzziness</code>
     */
    public CompletionSuggestionBuilder prefix(String prefix, Fuzziness fuzziness) {
        super.prefix(prefix);
        this.fuzzyOptions = new FuzzyOptions.Builder().setFuzziness(fuzziness).build();
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with full fuzzy options
     * see {@link FuzzyOptions.Builder}
     */
    public CompletionSuggestionBuilder prefix(String prefix, FuzzyOptions fuzzyOptions) {
        super.prefix(prefix);
        this.fuzzyOptions = fuzzyOptions;
        return this;
    }

    /**
     * Sets a regular expression pattern for prefixes to provide completions for.
     */
    @Override
    public CompletionSuggestionBuilder regex(String regex) {
        super.regex(regex);
        return this;
    }

    /**
     * Same as {@link #regex(String)} with full regular expression options
     * see {@link RegexOptions.Builder}
     */
    public CompletionSuggestionBuilder regex(String regex, RegexOptions regexOptions) {
        this.regex(regex);
        this.regexOptions = regexOptions;
        return this;
    }

    /**
     * Sets the fields to be returned as suggestion payload.
     * Note: Only doc values enabled fields are supported
     */
    public CompletionSuggestionBuilder payload(List<String> fields) {
        this.payloadFields = fields;
        return this;
    }

    /**
     * Sets query contexts for completion
     * @param queryContexts named query contexts
     *                      see {@link org.elasticsearch.search.suggest.completion.context.CategoryQueryContext}
     *                      and {@link org.elasticsearch.search.suggest.completion.context.GeoQueryContext}
     */
    public CompletionSuggestionBuilder contexts(Map<String, List<? extends QueryContext>> queryContexts) {
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject();
            for (Map.Entry<String, List<? extends QueryContext>> contextEntry : queryContexts.entrySet()) {
                contentBuilder.startArray(contextEntry.getKey());
                for (ToXContent queryContext : contextEntry.getValue()) {
                    queryContext.toXContent(contentBuilder, EMPTY_PARAMS);
                }
                contentBuilder.endArray();
            }
            contentBuilder.endObject();
            contextBytes = contentBuilder.bytes();
            return this;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (payloadFields.isEmpty() == false) {
            builder.startArray(PAYLOAD_FIELD.getPreferredName());
            for (String field : payloadFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (fuzzyOptions != null) {
            fuzzyOptions.toXContent(builder, params);
        }
        if (regexOptions != null) {
            regexOptions.toXContent(builder, params);
        }
        if (contextBytes != null) {
            XContentParser contextParser = XContentFactory.xContent(XContentType.JSON).createParser(contextBytes);
            builder.field(CONTEXTS_FIELD.getPreferredName());
            builder.copyCurrentStructure(contextParser);
        }
        return builder;
    }

    @Override
    protected CompletionSuggestionBuilder innerFromXContent(QueryParseContext parseContext) throws IOException {
        CompletionSuggestionBuilder builder = new CompletionSuggestionBuilder();
        TLP_PARSER.parse(parseContext.parser(), builder);
        return builder;
    }

    @Override
    protected SuggestionContext innerBuild(QueryShardContext context) throws IOException {
        CompletionSuggestionContext suggestionContext = new CompletionSuggestionContext(context);
        // copy over common settings to each suggestion builder
        final MapperService mapperService = context.getMapperService();
        populateCommonFields(mapperService, suggestionContext);
        suggestionContext.setPayloadFields(payloadFields);
        suggestionContext.setFuzzyOptions(fuzzyOptions);
        suggestionContext.setRegexOptions(regexOptions);
        MappedFieldType mappedFieldType = mapperService.fullName(suggestionContext.getField());
        if (mappedFieldType == null) {
            throw new ElasticsearchException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        } else if (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            CompletionFieldMapper.CompletionFieldType type = (CompletionFieldMapper.CompletionFieldType) mappedFieldType;
            if (type.hasContextMappings() && contextBytes != null) {
                XContentParser contextParser = XContentFactory.xContent(contextBytes).createParser(contextBytes);
                suggestionContext.setQueryContexts(parseQueryContexts(contextParser, type));
            } else if (contextBytes != null) {
                throw new IllegalArgumentException("suggester [" + type.name() + "] doesn't expect any context");
            }
        } else {
            throw new IllegalArgumentException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        return suggestionContext;
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        boolean payloadFieldExists = payloadFields.isEmpty() == false;
        out.writeBoolean(payloadFieldExists);
        if (payloadFieldExists) {
            out.writeVInt(payloadFields.size());
            for (String payloadField : payloadFields) {
                out.writeString(payloadField);
            }
        }
        out.writeBoolean(fuzzyOptions != null);
        if (fuzzyOptions != null) {
            fuzzyOptions.writeTo(out);
        }
        out.writeBoolean(regexOptions != null);
        if (regexOptions != null) {
            regexOptions.writeTo(out);
        }
        boolean queryContextsExists = contextBytes != null;
        out.writeBoolean(queryContextsExists);
        if (queryContextsExists) {
            out.writeBytesReference(contextBytes);
        }
    }

    @Override
    public CompletionSuggestionBuilder doReadFrom(StreamInput in, String fieldname) throws IOException {
        CompletionSuggestionBuilder completionSuggestionBuilder = new CompletionSuggestionBuilder(fieldname);
        if (in.readBoolean()) {
            int numPayloadField = in.readVInt();
            List<String> payloadFields = new ArrayList<>(numPayloadField);
            for (int i = 0; i < numPayloadField; i++) {
                payloadFields.add(in.readString());
            }
            completionSuggestionBuilder.payloadFields = payloadFields;
        }
        if (in.readBoolean()) {
            completionSuggestionBuilder.fuzzyOptions = FuzzyOptions.readFuzzyOptions(in);
        }
        if (in.readBoolean()) {
            completionSuggestionBuilder.regexOptions = RegexOptions.readRegexOptions(in);
        }
        if (in.readBoolean()) {
            completionSuggestionBuilder.contextBytes = in.readBytesReference();
        }
        return completionSuggestionBuilder;
    }

    @Override
    protected boolean doEquals(CompletionSuggestionBuilder other) {
         return Objects.equals(payloadFields, other.payloadFields) &&
            Objects.equals(fuzzyOptions, other.fuzzyOptions) &&
            Objects.equals(regexOptions, other.regexOptions) &&
            Objects.equals(contextBytes, other.contextBytes);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(payloadFields, fuzzyOptions, regexOptions, contextBytes);
    }

    static Map<String, List<ContextMapping.InternalQueryContext>> parseQueryContexts(
        XContentParser contextParser, CompletionFieldMapper.CompletionFieldType type) throws IOException {
        Map<String, List<ContextMapping.InternalQueryContext>> queryContexts = Collections.emptyMap();
        if (type.hasContextMappings() && contextParser != null) {
            ContextMappings contextMappings = type.getContextMappings();
            contextParser.nextToken();
            queryContexts = new HashMap<>(contextMappings.size());
            assert contextParser.currentToken() == XContentParser.Token.START_OBJECT;
            XContentParser.Token currentToken;
            String currentFieldName;
            while ((currentToken = contextParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = contextParser.currentName();
                    final ContextMapping mapping = contextMappings.get(currentFieldName);
                    queryContexts.put(currentFieldName, mapping.parseQueryContext(contextParser));
                }
            }
            contextParser.close();
        }
        return queryContexts;
    }
}
