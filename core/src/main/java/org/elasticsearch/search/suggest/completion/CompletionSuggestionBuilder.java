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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
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
import org.elasticsearch.index.mapper.core.CompletionFieldMapper2x;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.elasticsearch.search.suggest.completion2x.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion2x.context.ContextMapping.ContextQuery;
import org.elasticsearch.search.suggest.completion2x.context.GeolocationContextMapping;

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
    static final String SUGGESTION_NAME = "completion";
    static final ParseField PAYLOAD_FIELD = new ParseField("payload");
    static final ParseField CONTEXTS_FIELD = new ParseField("contexts", "context");

    /**
     * {
     *     "field" : STRING
     *     "size" : INT
     *     "fuzzy" : BOOLEAN | FUZZY_OBJECT
     *     "contexts" : QUERY_CONTEXTS
     *     "regex" : REGEX_OBJECT
     *     "payload" : STRING_ARRAY
     * }
     */
    private static ObjectParser<CompletionSuggestionBuilder.InnerBuilder, ParseFieldMatcherSupplier> TLP_PARSER =
        new ObjectParser<>(SUGGESTION_NAME, null);
    static {
        TLP_PARSER.declareStringArray(CompletionSuggestionBuilder.InnerBuilder::payload, PAYLOAD_FIELD);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) -> {
                if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                    if (parser.booleanValue()) {
                        completionSuggestionContext.fuzzyOptions = new FuzzyOptions.Builder().build();
                    }
                } else {
                    completionSuggestionContext.fuzzyOptions = FuzzyOptions.parse(parser, context);
                }
            },
            FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) ->
            completionSuggestionContext.regexOptions = RegexOptions.parse(parser, context),
            RegexOptions.REGEX_OPTIONS, ObjectParser.ValueType.OBJECT);
        TLP_PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::field, SuggestUtils.Fields.FIELD);
        TLP_PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::analyzer, SuggestUtils.Fields.ANALYZER);
        TLP_PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::size, SuggestUtils.Fields.SIZE);
        TLP_PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::shardSize, SuggestUtils.Fields.SHARD_SIZE);
        TLP_PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.copyCurrentStructure(p);
            v.contextBytes = builder.bytes();
            p.skipChildren();
        }, CONTEXTS_FIELD, ObjectParser.ValueType.OBJECT); // context is deprecated
    }

    protected FuzzyOptions fuzzyOptions;
    protected RegexOptions regexOptions;
    protected BytesReference contextBytes = null;
    protected List<String> payloadFields = Collections.emptyList();

    public CompletionSuggestionBuilder(String field) {
        super(field);
    }

    /**
     * internal copy constructor that copies over all class fields except for the field which is
     * set to the one provided in the first argument
     */
    private CompletionSuggestionBuilder(String fieldname, CompletionSuggestionBuilder in) {
        super(fieldname, in);
        fuzzyOptions = in.fuzzyOptions;
        regexOptions = in.regexOptions;
        contextBytes = in.contextBytes;
        payloadFields = in.payloadFields;
    }

    /**
     * Read from a stream.
     */
    public CompletionSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        payloadFields = new ArrayList<>();
        Collections.addAll(payloadFields, in.readStringArray());
        fuzzyOptions = in.readOptionalWriteable(FuzzyOptions::new);
        regexOptions = in.readOptionalWriteable(RegexOptions::new);
        contextBytes = in.readOptionalBytesReference();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringArray(payloadFields.toArray(new String[payloadFields.size()]));
        out.writeOptionalWriteable(fuzzyOptions);
        out.writeOptionalWriteable(regexOptions);
        out.writeOptionalBytesReference(contextBytes);
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
        Objects.requireNonNull(fields, "payload must not be null");
        this.payloadFields = fields;
        return this;
    }

    /**
     * Sets query contexts for completion
     * @param queryContexts named query contexts
     *                      see {@link org.elasticsearch.search.suggest.completion.context.CategoryQueryContext}
     *                      and {@link org.elasticsearch.search.suggest.completion.context.GeoQueryContext}
     */
    public CompletionSuggestionBuilder contexts(Map<String, List<? extends ToXContent>> queryContexts) {
        Objects.requireNonNull(queryContexts, "contexts must not be null");
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject();
            for (Map.Entry<String, List<? extends ToXContent>> contextEntry : queryContexts.entrySet()) {
                contentBuilder.startArray(contextEntry.getKey());
                for (ToXContent queryContext : contextEntry.getValue()) {
                    queryContext.toXContent(contentBuilder, EMPTY_PARAMS);
                }
                contentBuilder.endArray();
            }
            contentBuilder.endObject();
            return contexts(contentBuilder);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private CompletionSuggestionBuilder contexts(XContentBuilder contextBuilder) {
        contextBytes = contextBuilder.bytes();
        return this;
    }

    public CompletionSuggestionBuilder contexts(Contexts2x contexts2x) {
        Objects.requireNonNull(contexts2x, "contexts must not be null");
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject();
            for (ContextQuery contextQuery : contexts2x.contextQueries) {
                contextQuery.toXContent(contentBuilder, EMPTY_PARAMS);
            }
            contentBuilder.endObject();
            return contexts(contentBuilder);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // for 2.x context support
    public static class Contexts2x {
        private List<ContextQuery> contextQueries = new ArrayList<>();

        @SuppressWarnings("unchecked")
        private Contexts2x addContextQuery(ContextQuery ctx) {
            this.contextQueries.add(ctx);
            return this;
        }

        /**
         * Setup a Geolocation for suggestions. See {@link GeolocationContextMapping}.
         * @param lat Latitude of the location
         * @param lon Longitude of the Location
         * @return this
         */
        @Deprecated
        public Contexts2x addGeoLocation(String name, double lat, double lon, int ... precisions) {
            return addContextQuery(GeolocationContextMapping.query(name, lat, lon, precisions));
        }

        /**
         * Setup a Geolocation for suggestions. See {@link GeolocationContextMapping}.
         * @param lat Latitude of the location
         * @param lon Longitude of the Location
         * @param precisions precisions as string var-args
         * @return this
         */
        @Deprecated
        public Contexts2x addGeoLocationWithPrecision(String name, double lat, double lon, String ... precisions) {
            return addContextQuery(GeolocationContextMapping.query(name, lat, lon, precisions));
        }

        /**
         * Setup a Geolocation for suggestions. See {@link GeolocationContextMapping}.
         * @param geohash Geohash of the location
         * @return this
         */
        @Deprecated
        public Contexts2x addGeoLocation(String name, String geohash) {
            return addContextQuery(GeolocationContextMapping.query(name, geohash));
        }

        /**
         * Setup a Category for suggestions. See {@link CategoryContextMapping}.
         * @param categories name of the category
         * @return this
         */
        @Deprecated
        public Contexts2x addCategory(String name, CharSequence...categories) {
            return addContextQuery(CategoryContextMapping.query(name, categories));
        }

        /**
         * Setup a Category for suggestions. See {@link CategoryContextMapping}.
         * @param categories name of the category
         * @return this
         */
        @Deprecated
        public Contexts2x addCategory(String name, Iterable<? extends CharSequence> categories) {
            return addContextQuery(CategoryContextMapping.query(name, categories));
        }

        /**
         * Setup a Context Field for suggestions. See {@link CategoryContextMapping}.
         * @param fieldvalues name of the category
         * @return this
         */
        @Deprecated
        public Contexts2x addContextField(String name, CharSequence...fieldvalues) {
            return addContextQuery(CategoryContextMapping.query(name, fieldvalues));
        }

        /**
         * Setup a Context Field for suggestions. See {@link CategoryContextMapping}.
         * @param fieldvalues name of the category
         * @return this
         */
        @Deprecated
        public Contexts2x addContextField(String name, Iterable<? extends CharSequence> fieldvalues) {
            return addContextQuery(CategoryContextMapping.query(name, fieldvalues));
        }
    }

    private static class InnerBuilder extends CompletionSuggestionBuilder {
        private String field;

        public InnerBuilder() {
            super("_na_");
        }

        private InnerBuilder field(String field) {
            this.field = field;
            return this;
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
            try (XContentParser contextParser = XContentFactory.xContent(XContentType.JSON).createParser(contextBytes)) {
                builder.field(CONTEXTS_FIELD.getPreferredName());
                builder.copyCurrentStructure(contextParser);
            }
        }
        return builder;
    }

    static CompletionSuggestionBuilder innerFromXContent(QueryParseContext parseContext) throws IOException {
        CompletionSuggestionBuilder.InnerBuilder builder = new CompletionSuggestionBuilder.InnerBuilder();
        TLP_PARSER.parse(parseContext.parser(), builder, parseContext);
        String field = builder.field;
        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (field == null) {
            throw new ElasticsearchParseException(
                "the required field option [" + SuggestUtils.Fields.FIELD.getPreferredName() + "] is missing");
        }
        return new CompletionSuggestionBuilder(field, builder);
    }

    @Override
    public SuggestionContext build(QueryShardContext context) throws IOException {
        CompletionSuggestionContext suggestionContext = new CompletionSuggestionContext(context);
        // copy over common settings to each suggestion builder
        final MapperService mapperService = context.getMapperService();
        populateCommonFields(mapperService, suggestionContext);
        suggestionContext.setPayloadFields(payloadFields);
        suggestionContext.setFuzzyOptions(fuzzyOptions);
        suggestionContext.setRegexOptions(regexOptions);
        MappedFieldType mappedFieldType = mapperService.fullName(suggestionContext.getField());
        if (mappedFieldType == null ||
            (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType == false
                && mappedFieldType instanceof CompletionFieldMapper2x.CompletionFieldType == false)) {
            throw new IllegalArgumentException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        if (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            CompletionFieldMapper.CompletionFieldType type = (CompletionFieldMapper.CompletionFieldType) mappedFieldType;
            suggestionContext.setFieldType(type);
            if (type.hasContextMappings() && contextBytes != null) {
                try (XContentParser contextParser = XContentFactory.xContent(contextBytes).createParser(contextBytes)) {
                    if (type.hasContextMappings() && contextParser != null) {
                        ContextMappings contextMappings = type.getContextMappings();
                        contextParser.nextToken();
                        Map<String, List<ContextMapping.InternalQueryContext>> queryContexts = new HashMap<>(contextMappings.size());
                        assert contextParser.currentToken() == XContentParser.Token.START_OBJECT;
                        XContentParser.Token currentToken;
                        String currentFieldName;
                        while ((currentToken = contextParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (currentToken == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = contextParser.currentName();
                                final ContextMapping mapping = contextMappings.get(currentFieldName);
                                queryContexts.put(currentFieldName, mapping.parseQueryContext(context.newParseContext(contextParser)));
                            }
                        }
                        suggestionContext.setQueryContexts(queryContexts);
                    }
                }
            } else if (contextBytes != null) {
                throw new IllegalArgumentException("suggester [" + type.name() + "] doesn't expect any context");
            }
        } else if (mappedFieldType instanceof CompletionFieldMapper2x.CompletionFieldType) {
            CompletionFieldMapper2x.CompletionFieldType type = ((CompletionFieldMapper2x.CompletionFieldType) mappedFieldType);
            suggestionContext.setFieldType2x(type);
            if (type.requiresContext()) {
                if (contextBytes != null) {
                    try (XContentParser contextParser = XContentFactory.xContent(contextBytes).createParser(contextBytes)) {
                        contextParser.nextToken();
                        suggestionContext.setContextQueries(ContextQuery.parseQueries(type.getContextMapping(), contextParser));
                    }
                } else {
                    throw new IllegalArgumentException("suggester [completion] requires context to be setup");
                }
            } else if (contextBytes != null) {
                throw new IllegalArgumentException("suggester [completion] doesn't expect any context");
            }
        }
        assert suggestionContext.getFieldType() != null || suggestionContext.getFieldType2x() != null : "no completion field type set";
        return suggestionContext;
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
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
}
