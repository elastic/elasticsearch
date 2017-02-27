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
package org.elasticsearch.search.suggest.filteredsuggest;

import org.elasticsearch.ElasticsearchParseException;
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
import org.elasticsearch.index.mapper.FilteredSuggestFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.FuzzyOptions;
import org.elasticsearch.search.suggest.completion.RegexOptions;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilteredSuggestFilterMapping;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Defines a suggest command based on a prefix, typically to provide "auto-complete" functionality for users as they
 * type search terms. The implementation of the completion service uses FSTs that are created at index-time and so must
 * be defined in the mapping with the type "filteredsuggest" before indexing.
 */
public class FilteredSuggestSuggestionBuilder extends SuggestionBuilder<FilteredSuggestSuggestionBuilder> {
    static final String SUGGESTION_NAME = "filteredsuggest";
    static final ParseField FILTERS_FIELD = new ParseField("filters", "filter");

    /** Character used to mock the empty filter */
    public static final String EMPTY_FILTER_FILLER = new String(new char['\u001B']);

    private static ObjectParser<FilteredSuggestSuggestionBuilder.InnerBuilder, Void> PARSER = new ObjectParser<>(SUGGESTION_NAME, null);
    static {
        PARSER.declareField((parser, completionSuggestionContext, context) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (parser.booleanValue()) {
                    completionSuggestionContext.fuzzyOptions = new FuzzyOptions.Builder().build();
                }
            } else {
                completionSuggestionContext.fuzzyOptions = FuzzyOptions.parse(parser);
            }
        }, FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        PARSER.declareField(
                (parser, completionSuggestionContext, context) -> completionSuggestionContext.regexOptions = RegexOptions.parse(parser),
                RegexOptions.REGEX_OPTIONS, ObjectParser.ValueType.OBJECT);
        PARSER.declareString(FilteredSuggestSuggestionBuilder.InnerBuilder::field, FIELDNAME_FIELD);
        PARSER.declareString(FilteredSuggestSuggestionBuilder.InnerBuilder::analyzer, ANALYZER_FIELD);
        PARSER.declareInt(FilteredSuggestSuggestionBuilder.InnerBuilder::size, SIZE_FIELD);
        PARSER.declareInt(FilteredSuggestSuggestionBuilder.InnerBuilder::shardSize, SHARDSIZE_FIELD);
        PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.copyCurrentStructure(p);
            v.contextBytes = builder.bytes();
            p.skipChildren();
        }, FILTERS_FIELD, ObjectParser.ValueType.OBJECT);
    }

    protected FuzzyOptions fuzzyOptions;
    protected RegexOptions regexOptions;
    protected BytesReference contextBytes = null;

    public FilteredSuggestSuggestionBuilder(String field) {
        super(field);
    }

    /**
     * internal copy constructor that copies over all class fields except for the field which is set to the one provided
     * in the first argument
     */
    private FilteredSuggestSuggestionBuilder(String fieldname, FilteredSuggestSuggestionBuilder in) {
        super(fieldname, in);
        fuzzyOptions = in.fuzzyOptions;
        regexOptions = in.regexOptions;
        contextBytes = in.contextBytes;
    }

    /**
     * Read from a stream.
     */
    public FilteredSuggestSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        fuzzyOptions = in.readOptionalWriteable(FuzzyOptions::new);
        regexOptions = in.readOptionalWriteable(RegexOptions::new);
        contextBytes = in.readOptionalBytesReference();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(fuzzyOptions);
        out.writeOptionalWriteable(regexOptions);
        out.writeOptionalBytesReference(contextBytes);
    }

    /**
     * Sets the prefix to provide completions for. The prefix gets analyzed by the suggest analyzer.
     */
    @Override
    public FilteredSuggestSuggestionBuilder prefix(String prefix) {
        super.prefix(prefix);
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with fuzziness of <code>fuzziness</code>
     */
    public FilteredSuggestSuggestionBuilder prefix(String prefix, Fuzziness fuzziness) {
        super.prefix(prefix);
        this.fuzzyOptions = new FuzzyOptions.Builder().setFuzziness(fuzziness).build();
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with full fuzzy options see {@link FuzzyOptions.Builder}
     */
    public FilteredSuggestSuggestionBuilder prefix(String prefix, FuzzyOptions fuzzyOptions) {
        super.prefix(prefix);
        this.fuzzyOptions = fuzzyOptions;
        return this;
    }

    /**
     * Sets a regular expression pattern for prefixes to provide completions for.
     */
    @Override
    public FilteredSuggestSuggestionBuilder regex(String regex) {
        super.regex(regex);
        return this;
    }

    /**
     * Same as {@link #regex(String)} with full regular expression options see {@link RegexOptions.Builder}
     */
    public FilteredSuggestSuggestionBuilder regex(String regex, RegexOptions regexOptions) {
        this.regex(regex);
        this.regexOptions = regexOptions;
        return this;
    }

    public FilteredSuggestSuggestionBuilder contexts(Map<String, List<? extends ToXContent>> queryFilters) {
        Objects.requireNonNull(queryFilters, "filters must not be null");
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject();
            for (Map.Entry<String, List<? extends ToXContent>> contextEntry : queryFilters.entrySet()) {
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

    private FilteredSuggestSuggestionBuilder contexts(XContentBuilder contextBuilder) {
        contextBytes = contextBuilder.bytes();
        return this;
    }

    private static class InnerBuilder extends FilteredSuggestSuggestionBuilder {
        private String field;

        InnerBuilder() {
            super("_na_");
        }

        private InnerBuilder field(String field) {
            this.field = field;
            return this;
        }
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (fuzzyOptions != null) {
            fuzzyOptions.toXContent(builder, params);
        }
        if (regexOptions != null) {
            regexOptions.toXContent(builder, params);
        }
        if (contextBytes != null) {
            builder.rawField(FILTERS_FIELD.getPreferredName(), contextBytes);
        }
        return builder;
    }

    public static FilteredSuggestSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
        FilteredSuggestSuggestionBuilder.InnerBuilder builder = new FilteredSuggestSuggestionBuilder.InnerBuilder();
        PARSER.parse(parser, builder, null);
        String field = builder.field;
        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (field == null) {
            throw new ElasticsearchParseException("the required field option [" + FIELDNAME_FIELD.getPreferredName() + "] is missing");
        }
        return new FilteredSuggestSuggestionBuilder(field, builder);
    }

    @Override
    public SuggestionContext build(QueryShardContext context) throws IOException {
        FilteredSuggestSuggestionContext suggestionContext = new FilteredSuggestSuggestionContext(context);
        // copy over common settings to each suggestion builder
        final MapperService mapperService = context.getMapperService();
        populateCommonFields(mapperService, suggestionContext);
        suggestionContext.setFuzzyOptions(fuzzyOptions);
        suggestionContext.setRegexOptions(regexOptions);
        MappedFieldType mappedFieldType = mapperService.fullName(suggestionContext.getField());
        if (mappedFieldType == null || mappedFieldType instanceof FilteredSuggestFieldMapper.CompletionFieldType == false) {
            throw new IllegalArgumentException("Field [" + suggestionContext.getField() + "] is not a filtered suggest field");
        }

        if (mappedFieldType instanceof FilteredSuggestFieldMapper.CompletionFieldType) {
            FilteredSuggestFieldMapper.CompletionFieldType type = ((FilteredSuggestFieldMapper.CompletionFieldType) mappedFieldType);
            suggestionContext.setFieldType(type);
            if (type.hasFilterMappings()) {
                if (contextBytes != null) {
                    try (XContentParser contextParser = XContentFactory.xContent(contextBytes).createParser(context.getXContentRegistry(),
                            contextBytes)) {
                        if (type.hasFilterMappings() && contextParser != null) {
                            contextParser.nextToken();
                            assert contextParser.currentToken() == XContentParser.Token.START_OBJECT;

                            suggestionContext.setQueryFilters(
                                    FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(), contextParser));
                        }
                    }
                } else {
                    suggestionContext.setQueryFilters(FilteredSuggestFilterMapping.parseQueries(context, type.getFilterMappings(), null));
                }
            }
        }

        assert suggestionContext.getFieldType() != null : "no nested suggest field type set";
        return suggestionContext;
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    protected boolean doEquals(FilteredSuggestSuggestionBuilder other) {
        return Objects.equals(fuzzyOptions, other.fuzzyOptions) && Objects.equals(regexOptions, other.regexOptions)
                && Objects.equals(contextBytes, other.contextBytes);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fuzzyOptions, regexOptions, contextBytes);
    }
}
