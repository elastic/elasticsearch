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

import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    private FuzzyOptionsBuilder fuzzyOptionsBuilder;
    private RegexOptionsBuilder regexOptionsBuilder;
    private final Map<String, List<ToXContent>> queryContexts = new HashMap<>();
    private final Set<String> payloadFields = new HashSet<>();

    public CompletionSuggestionBuilder(String name) {
        super(name);
    }

    /**
     * Options for fuzzy queries
     */
    public static class FuzzyOptionsBuilder implements ToXContent {
        static final ParseField FUZZY_OPTIONS = new ParseField("fuzzy");
        static final ParseField TRANSPOSITION_FIELD = new ParseField("transpositions");
        static final ParseField MIN_LENGTH_FIELD = new ParseField("min_length");
        static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
        static final ParseField UNICODE_AWARE_FIELD = new ParseField("unicode_aware");
        static final ParseField MAX_DETERMINIZED_STATES_FIELD = new ParseField("max_determinized_states");

        private int editDistance = FuzzyCompletionQuery.DEFAULT_MAX_EDITS;
        private boolean transpositions = FuzzyCompletionQuery.DEFAULT_TRANSPOSITIONS;
        private int fuzzyMinLength = FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH;
        private int fuzzyPrefixLength = FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX;
        private boolean unicodeAware = FuzzyCompletionQuery.DEFAULT_UNICODE_AWARE;
        private int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;

        public FuzzyOptionsBuilder() {
        }

        /**
         * Sets the level of fuzziness used to create suggestions using a {@link Fuzziness} instance.
         * The default value is {@link Fuzziness#ONE} which allows for an "edit distance" of one.
         */
        public FuzzyOptionsBuilder setFuzziness(int editDistance) {
            this.editDistance = editDistance;
            return this;
        }

        /**
         * Sets the level of fuzziness used to create suggestions using a {@link Fuzziness} instance.
         * The default value is {@link Fuzziness#ONE} which allows for an "edit distance" of one.
         */
        public FuzzyOptionsBuilder setFuzziness(Fuzziness fuzziness) {
            this.editDistance = fuzziness.asDistance();
            return this;
        }

        /**
         * Sets if transpositions (swapping one character for another) counts as one character
         * change or two.
         * Defaults to true, meaning it uses the fuzzier option of counting transpositions as
         * a single change.
         */
        public FuzzyOptionsBuilder setTranspositions(boolean transpositions) {
            this.transpositions = transpositions;
            return this;
        }

        /**
         * Sets the minimum length of input string before fuzzy suggestions are returned, defaulting
         * to 3.
         */
        public FuzzyOptionsBuilder setFuzzyMinLength(int fuzzyMinLength) {
            this.fuzzyMinLength = fuzzyMinLength;
            return this;
        }

        /**
         * Sets the minimum length of the input, which is not checked for fuzzy alternatives, defaults to 1
         */
        public FuzzyOptionsBuilder setFuzzyPrefixLength(int fuzzyPrefixLength) {
            this.fuzzyPrefixLength = fuzzyPrefixLength;
            return this;
        }

        /**
         * Sets the maximum automaton states allowed for the fuzzy expansion
         */
        public FuzzyOptionsBuilder setMaxDeterminizedStates(int maxDeterminizedStates) {
            this.maxDeterminizedStates = maxDeterminizedStates;
            return this;
        }

        /**
         * Set to true if all measurements (like edit distance, transpositions and lengths) are in unicode
         * code points (actual letters) instead of bytes. Default is false.
         */
        public FuzzyOptionsBuilder setUnicodeAware(boolean unicodeAware) {
            this.unicodeAware = unicodeAware;
            return this;
        }

        /**
         * Returns the maximum number of edits
         */
        int getEditDistance() {
            return editDistance;
        }

        /**
         * Returns if transpositions option is set
         *
         * if transpositions is set, then swapping one character for another counts as one edit instead of two.
         */
        boolean isTranspositions() {
            return transpositions;
        }


        /**
         * Returns the length of input prefix after which edits are applied
         */
        int getFuzzyMinLength() {
            return fuzzyMinLength;
        }

        /**
         * Returns the minimum length of the input prefix required to apply any edits
         */
        int getFuzzyPrefixLength() {
            return fuzzyPrefixLength;
        }

        /**
         * Returns if all measurements (like edit distance, transpositions and lengths) are in unicode code
         * points (actual letters) instead of bytes.
         */
        boolean isUnicodeAware() {
            return unicodeAware;
        }

        /**
         * Returns the maximum automaton states allowed for fuzzy expansion
         */
        int getMaxDeterminizedStates() {
            return maxDeterminizedStates;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(FUZZY_OPTIONS.getPreferredName());
            builder.field(Fuzziness.FIELD.getPreferredName(), editDistance);
            builder.field(TRANSPOSITION_FIELD.getPreferredName(), transpositions);
            builder.field(MIN_LENGTH_FIELD.getPreferredName(), fuzzyMinLength);
            builder.field(PREFIX_LENGTH_FIELD.getPreferredName(), fuzzyPrefixLength);
            builder.field(UNICODE_AWARE_FIELD.getPreferredName(), unicodeAware);
            builder.field(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), maxDeterminizedStates);
            builder.endObject();
            return builder;
        }
    }

    /**
     * Options for regular expression queries
     */
    public static class RegexOptionsBuilder implements ToXContent {
        static final ParseField REGEX_OPTIONS = new ParseField("regex");
        static final ParseField FLAGS_VALUE = new ParseField("flags", "flags_value");
        static final ParseField MAX_DETERMINIZED_STATES = new ParseField("max_determinized_states");
        private int flagsValue = RegExp.ALL;
        private int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;

        public RegexOptionsBuilder() {
        }

        /**
         * Sets the regular expression syntax flags
         * see {@link RegexpFlag}
         */
        public RegexOptionsBuilder setFlags(String flags) {
            this.flagsValue = RegexpFlag.resolveValue(flags);
            return this;
        }

        /**
         * Sets the maximum automaton states allowed for the regular expression expansion
         */
        public RegexOptionsBuilder setMaxDeterminizedStates(int maxDeterminizedStates) {
            this.maxDeterminizedStates = maxDeterminizedStates;
            return this;
        }

        int getFlagsValue() {
            return flagsValue;
        }

        int getMaxDeterminizedStates() {
            return maxDeterminizedStates;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(REGEX_OPTIONS.getPreferredName());
            builder.field(FLAGS_VALUE.getPreferredName(), flagsValue);
            builder.field(MAX_DETERMINIZED_STATES.getPreferredName(), maxDeterminizedStates);
            builder.endObject();
            return builder;
        }
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
        this.fuzzyOptionsBuilder = new FuzzyOptionsBuilder().setFuzziness(fuzziness);
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with full fuzzy options
     * see {@link FuzzyOptionsBuilder}
     */
    public CompletionSuggestionBuilder prefix(String prefix, FuzzyOptionsBuilder fuzzyOptionsBuilder) {
        super.prefix(prefix);
        this.fuzzyOptionsBuilder = fuzzyOptionsBuilder;
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
     * see {@link RegexOptionsBuilder}
     */
    public CompletionSuggestionBuilder regex(String regex, RegexOptionsBuilder regexOptionsBuilder) {
        this.regex(regex);
        this.regexOptionsBuilder = regexOptionsBuilder;
        return this;
    }

    /**
     * Sets the fields to be returned as suggestion payload.
     * Note: Only doc values enabled fields are supported
     */
    public CompletionSuggestionBuilder payload(String... fields) {
        Collections.addAll(this.payloadFields, fields);
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

    private CompletionSuggestionBuilder contexts(String name, ToXContent... queryContexts) {
        List<ToXContent> contexts = this.queryContexts.get(name);
        if (contexts == null) {
            contexts = new ArrayList<>(2);
            this.queryContexts.put(name, contexts);
        }
        Collections.addAll(contexts, queryContexts);
        return this;
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (payloadFields != null) {
            builder.startArray(PAYLOAD_FIELD.getPreferredName());
            for (String field : payloadFields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (fuzzyOptionsBuilder != null) {
            fuzzyOptionsBuilder.toXContent(builder, params);
        }
        if (regexOptionsBuilder != null) {
            regexOptionsBuilder.toXContent(builder, params);
        }
        if (queryContexts.isEmpty() == false) {
            builder.startObject(CONTEXTS_FIELD.getPreferredName());
            for (Map.Entry<String, List<ToXContent>> entry : this.queryContexts.entrySet()) {
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
        // NORELEASE
        return new CompletionSuggestionBuilder(name);
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
        // NORELEASE
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionSuggestionBuilder doReadFrom(StreamInput in, String name) throws IOException {
        // NORELEASE
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doEquals(CompletionSuggestionBuilder other) {
        // NORELEASE
        return false;
    }

    @Override
    protected int doHashCode() {
        // NORELEASE
        return 0;
    }
}
