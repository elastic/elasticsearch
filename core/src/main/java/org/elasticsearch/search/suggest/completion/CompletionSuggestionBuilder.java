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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;
import org.elasticsearch.search.suggest.completion.context.QueryContext;

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
 * Defines a suggest command based on a prefix, typically to provide "auto-complete" functionality
 * for users as they type search terms. The implementation of the completion service uses FSTs that
 * are created at index-time and so must be defined in the mapping with the type "completion" before
 * indexing.
 */
public class CompletionSuggestionBuilder extends SuggestionBuilder<CompletionSuggestionBuilder> {

    public static final CompletionSuggestionBuilder PROTOTYPE = new CompletionSuggestionBuilder("_na_"); // name doesn't matter
    static final String SUGGESTION_NAME = "completion";
    static final ParseField PAYLOAD_FIELD = new ParseField("payload");
    static final ParseField CONTEXTS_FIELD = new ParseField("contexts", "context");

    private FuzzyOptions fuzzyOptions;
    private RegexOptions regexOptions;
    private final Map<String, List<QueryContext>> queryContexts = new HashMap<>();
    private final Set<String> payloadFields = new HashSet<>();

    public CompletionSuggestionBuilder(String name) {
        super(name);
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
        this.payloadFields.addAll(fields);
        return this;
    }

    /**
     * Sets query contexts for a category context
     * @param name of the category context to execute on
     * @param queryContexts a list of {@link CategoryQueryContext}
     */
    public CompletionSuggestionBuilder categoryContexts(String name, CategoryQueryContext... queryContexts) {
        return contexts(name, queryContexts);
    }

    /**
     * Sets query contexts for a geo context
     * @param name of the geo context to execute on
     * @param queryContexts a list of {@link GeoQueryContext}
     */
    public CompletionSuggestionBuilder geoContexts(String name, GeoQueryContext... queryContexts) {
        return contexts(name, queryContexts);
    }

    private CompletionSuggestionBuilder contexts(String name, QueryContext... queryContexts) {
        List<QueryContext> contexts = this.queryContexts.get(name);
        if (contexts == null) {
            contexts = new ArrayList<>(2);
            this.queryContexts.put(name, contexts);
        }
        Collections.addAll(contexts, queryContexts);
        return this;
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
        if (queryContexts.isEmpty() == false) {
            builder.startObject(CONTEXTS_FIELD.getPreferredName());
            for (Map.Entry<String, List<QueryContext>> entry : this.queryContexts.entrySet()) {
                builder.startArray(entry.getKey());
                for (ToXContent queryContext : entry.getValue()) {
                    queryContext.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    protected CompletionSuggestionBuilder innerFromXContent(QueryParseContext parseContext, String name) throws IOException {
        // NORELEASE implement parsing logic
        throw new UnsupportedOperationException();
    }

    @Override
    protected SuggestionContext innerBuild(QueryShardContext context) throws IOException {
        CompletionSuggestionContext suggestionContext = new CompletionSuggestionContext(context);
        // copy over common settings to each suggestion builder
        populateCommonFields(context.getMapperService(), suggestionContext);
        // NORELEASE
        // still need to populate CompletionSuggestionContext's specific settings
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
        boolean queryContextsExists = queryContexts.isEmpty() == false;
        out.writeBoolean(queryContextsExists);
        if (queryContextsExists) {
            out.writeVInt(queryContexts.size());
            for (Map.Entry<String, List<QueryContext>> namedQueryContexts : queryContexts.entrySet()) {
                out.writeString(namedQueryContexts.getKey());
                List<QueryContext> queryContexts = namedQueryContexts.getValue();
                out.writeVInt(queryContexts.size());
                for (QueryContext queryContext : queryContexts) {
                    out.writeCompletionSuggestionQueryContext(queryContext);
                }
            }
        }
    }

    @Override
    public CompletionSuggestionBuilder doReadFrom(StreamInput in, String name) throws IOException {
        CompletionSuggestionBuilder completionSuggestionBuilder = new CompletionSuggestionBuilder(name);
        if (in.readBoolean()) {
            int numPayloadField = in.readVInt();
            for (int i = 0; i < numPayloadField; i++) {
                completionSuggestionBuilder.payloadFields.add(in.readString());
            }
        }
        if (in.readBoolean()) {
            completionSuggestionBuilder.fuzzyOptions = FuzzyOptions.readFuzzyOptions(in);
        }
        if (in.readBoolean()) {
            completionSuggestionBuilder.regexOptions = RegexOptions.readRegexOptions(in);
        }
        if (in.readBoolean()) {
            int numNamedQueryContexts = in.readVInt();
            for (int i = 0; i < numNamedQueryContexts; i++) {
                String queryContextName = in.readString();
                int numQueryContexts = in.readVInt();
                List<QueryContext> queryContexts = new ArrayList<>(numQueryContexts);
                for (int j = 0; j < numQueryContexts; j++) {
                    queryContexts.add(in.readCompletionSuggestionQueryContext());
                }
                completionSuggestionBuilder.queryContexts.put(queryContextName, queryContexts);
            }
        }
        return completionSuggestionBuilder;
    }

    @Override
    protected boolean doEquals(CompletionSuggestionBuilder other) {
        return Objects.equals(payloadFields, other.payloadFields) &&
            Objects.equals(fuzzyOptions, other.fuzzyOptions) &&
            Objects.equals(regexOptions, other.regexOptions) &&
            Objects.equals(queryContexts, other.queryContexts);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(payloadFields, fuzzyOptions, regexOptions, queryContexts);
    }
}
