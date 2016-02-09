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

package org.elasticsearch.search.suggest.term;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.DirectSpellcheckerSettings;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_ACCURACY;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MAX_EDITS;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MAX_INSPECTIONS;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MAX_TERM_FREQ;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MIN_DOC_FREQ;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_MIN_WORD_LENGTH;
import static org.elasticsearch.search.suggest.DirectSpellcheckerSettings.DEFAULT_PREFIX_LENGTH;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.ACCURACY;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.MAX_EDITS;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.MAX_INSPECTIONS;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.MAX_TERM_FREQ;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.MIN_DOC_FREQ;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.MIN_WORD_LENGTH;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.PREFIX_LENGTH;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.SORT;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.STRING_DISTANCE;
import static org.elasticsearch.search.suggest.SuggestUtils.Fields.SUGGEST_MODE;

/**
 * Defines the actual suggest command. Each command uses the global options
 * unless defined in the suggestion itself. All options are the same as the
 * global options, but are only applicable for this suggestion.
 */
public class TermSuggestionBuilder extends SuggestionBuilder<TermSuggestionBuilder> {

    public static final TermSuggestionBuilder PROTOTYPE = new TermSuggestionBuilder("_na_"); // name doesn't matter
    private static final String SUGGESTION_NAME = "term";

    private SuggestMode suggestMode = SuggestMode.MISSING;
    private Float accuracy = DEFAULT_ACCURACY;
    private SortBy sort = SortBy.SCORE;
    private StringDistanceImpl stringDistance = StringDistanceImpl.INTERNAL;
    private Integer maxEdits = DEFAULT_MAX_EDITS;
    private Integer maxInspections = DEFAULT_MAX_INSPECTIONS;
    private Float maxTermFreq = DEFAULT_MAX_TERM_FREQ;
    private Integer prefixLength = DEFAULT_PREFIX_LENGTH;
    private Integer minWordLength = DEFAULT_MIN_WORD_LENGTH;
    private Float minDocFreq = DEFAULT_MIN_DOC_FREQ;

    /**
     * @param name
     *            The name of this suggestion. This is a required parameter.
     */
    public TermSuggestionBuilder(String name) {
        super(name);
    }

    /**
     * The global suggest mode controls what suggested terms are included or
     * controls for what suggest text tokens, terms should be suggested for.
     * Three possible values can be specified:
     * <ol>
     * <li><code>missing</code> - Only suggest terms in the suggest text that
     * aren't in the index. This is the default.
     * <li><code>popular</code> - Only suggest terms that occur in more docs
     * then the original suggest text term.
     * <li><code>always</code> - Suggest any matching suggest terms based on
     * tokens in the suggest text.
     * </ol>
     */
    public TermSuggestionBuilder suggestMode(SuggestMode suggestMode) {
        Objects.requireNonNull(suggestMode, "suggestMode must not be null");
        this.suggestMode = suggestMode;
        return this;
    }

    /**
     * Get the suggest mode setting.
     */
    public SuggestMode suggestMode() {
        return suggestMode;
    }

    /**
     * s how similar the suggested terms at least need to be compared to the
     * original suggest text tokens. A value between 0 and 1 can be specified.
     * This value will be compared to the string distance result of each
     * candidate spelling correction.
     * <p>
     * Default is <tt>0.5</tt>
     */
    public TermSuggestionBuilder accuracy(float accuracy) {
        if (accuracy < 0.0f || accuracy > 1.0f) {
            throw new IllegalArgumentException("accuracy must be between 0 and 1");
        }
        this.accuracy = accuracy;
        return this;
    }

    /**
     * Get the accuracy setting.
     */
    public Float accuracy() {
        return accuracy;
    }

    /**
     * Sets how to sort the suggest terms per suggest text token. Two possible
     * values:
     * <ol>
     * <li><code>score</code> - Sort should first be based on score, then
     * document frequency and then the term itself.
     * <li><code>frequency</code> - Sort should first be based on document
     * frequency, then score and then the term itself.
     * </ol>
     * <p>
     * What the score is depends on the suggester being used.
     */
    public TermSuggestionBuilder sort(SortBy sort) {
        Objects.requireNonNull(sort, "sort must not be null");
        this.sort = sort;
        return this;
    }

    /**
     * Get the sort setting.
     */
    public SortBy sort() {
        return sort;
    }

    /**
     * Sets what string distance implementation to use for comparing how similar
     * suggested terms are. Five possible values can be specified:
     * <ol>
     * <li><code>internal</code> - This is the default and is based on
     * <code>damerau_levenshtein</code>, but highly optimized for comparing
     * string distance for terms inside the index.
     * <li><code>damerau_levenshtein</code> - String distance algorithm based on
     * Damerau-Levenshtein algorithm.
     * <li><code>levenstein</code> - String distance algorithm based on
     * Levenstein edit distance algorithm.
     * <li><code>jarowinkler</code> - String distance algorithm based on
     * Jaro-Winkler algorithm.
     * <li><code>ngram</code> - String distance algorithm based on character
     * n-grams.
     * </ol>
     */
    public TermSuggestionBuilder stringDistance(StringDistanceImpl stringDistance) {
        Objects.requireNonNull(stringDistance, "stringDistance must not be null");
        this.stringDistance = stringDistance;
        return this;
    }

    /**
     * Get the string distance implementation setting.
     */
    public StringDistanceImpl stringDistance() {
        return stringDistance;
    }

    /**
     * Sets the maximum edit distance candidate suggestions can have in order to
     * be considered as a suggestion. Can only be a value between 1 and 2. Any
     * other value result in an bad request error being thrown. Defaults to
     * <tt>2</tt>.
     */
    public TermSuggestionBuilder maxEdits(int maxEdits) {
        if (maxEdits < 1 || maxEdits > 2) {
            throw new IllegalArgumentException("maxEdits must be between 1 and 2");
        }
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * Get the maximum edit distance setting.
     */
    public Integer maxEdits() {
        return maxEdits;
    }

    /**
     * A factor that is used to multiply with the size in order to inspect more
     * candidate suggestions. Can improve accuracy at the cost of performance.
     * Defaults to <tt>5</tt>.
     */
    public TermSuggestionBuilder maxInspections(int maxInspections) {
        if (maxInspections < 0) {
            throw new IllegalArgumentException("maxInspections must be positive");
        }
        this.maxInspections = maxInspections;
        return this;
    }

    /**
     * Get the factor for inspecting more candidate suggestions setting.
     */
    public Integer maxInspections() {
        return maxInspections;
    }

    /**
     * Sets a maximum threshold in number of documents a suggest text token can
     * exist in order to be corrected. Can be a relative percentage number (e.g
     * 0.4) or an absolute number to represent document frequencies. If an value
     * higher than 1 is specified then fractional can not be specified. Defaults
     * to <tt>0.01</tt>.
     * <p>
     * This can be used to exclude high frequency terms from being suggested.
     * High frequency terms are usually spelled correctly on top of this this
     * also improves the suggest performance.
     */
    public TermSuggestionBuilder maxTermFreq(float maxTermFreq) {
        if (maxTermFreq < 0.0f) {
            throw new IllegalArgumentException("maxTermFreq must be positive");
        }
        if (maxTermFreq > 1.0f && maxTermFreq != Math.floor(maxTermFreq)) {
            throw new IllegalArgumentException("if maxTermFreq is greater than 1, it must not be a fraction");
        }
        this.maxTermFreq = maxTermFreq;
        return this;
    }

    /**
     * Get the maximum term frequency threshold setting.
     */
    public Float maxTermFreq() {
        return maxTermFreq;
    }

    /**
     * Sets the number of minimal prefix characters that must match in order be
     * a candidate suggestion. Defaults to 1. Increasing this number improves
     * suggest performance. Usually misspellings don't occur in the beginning of
     * terms.
     */
    public TermSuggestionBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("prefixLength must be positive");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Get the minimum prefix length that must match setting.
     */
    public Integer prefixLength() {
        return prefixLength;
    }

    /**
     * The minimum length a suggest text term must have in order to be
     * corrected. Defaults to <tt>4</tt>.
     */
    public TermSuggestionBuilder minWordLength(int minWordLength) {
        if (minWordLength < 1) {
            throw new IllegalArgumentException("minWordLength must be greater or equal to 1");
        }
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Get the minimum length of a text term to be corrected setting.
     */
    public Integer minWordLength() {
        return minWordLength;
    }

    /**
     * Sets a minimal threshold in number of documents a suggested term should
     * appear in. This can be specified as an absolute number or as a relative
     * percentage of number of documents. This can improve quality by only
     * suggesting high frequency terms. Defaults to 0f and is not enabled. If a
     * value higher than 1 is specified then the number cannot be fractional.
     */
    public TermSuggestionBuilder minDocFreq(float minDocFreq) {
        if (minDocFreq < 0.0f) {
            throw new IllegalArgumentException("minDocFreq must be positive");
        }
        if (minDocFreq > 1.0f && minDocFreq != Math.floor(minDocFreq)) {
            throw new IllegalArgumentException("if minDocFreq is greater than 1, it must not be a fraction");
        }
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Get the minimal threshold for the frequency of a term appearing in the
     * document set setting.
     */
    public Float minDocFreq() {
        return minDocFreq;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SUGGEST_MODE.getPreferredName(), suggestMode);
        builder.field(ACCURACY.getPreferredName(), accuracy);
        builder.field(SORT.getPreferredName(), sort);
        builder.field(STRING_DISTANCE.getPreferredName(), stringDistance);
        builder.field(MAX_EDITS.getPreferredName(), maxEdits);
        builder.field(MAX_INSPECTIONS.getPreferredName(), maxInspections);
        builder.field(MAX_TERM_FREQ.getPreferredName(), maxTermFreq);
        builder.field(PREFIX_LENGTH.getPreferredName(), prefixLength);
        builder.field(MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        builder.field(MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        return builder;
    }

    @Override
    protected TermSuggestionBuilder innerFromXContent(QueryParseContext parseContext, String name) throws IOException {
        XContentParser parser = parseContext.parser();
        TermSuggestionBuilder suggestion = new TermSuggestionBuilder(name);
        ParseFieldMatcher parseFieldMatcher = parseContext.parseFieldMatcher();
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseFieldMatcher.match(fieldName, SuggestionBuilder.ANALYZER_FIELD)) {
                    suggestion.analyzer(parser.text());
                } else if (parseFieldMatcher.match(fieldName, SuggestionBuilder.FIELDNAME_FIELD)) {
                    suggestion.field(parser.text());
                } else if (parseFieldMatcher.match(fieldName, SuggestionBuilder.SIZE_FIELD)) {
                    suggestion.size(parser.intValue());
                } else if (parseFieldMatcher.match(fieldName, SuggestionBuilder.SHARDSIZE_FIELD)) {
                    suggestion.shardSize(parser.intValue());
                } else if (parseFieldMatcher.match(fieldName, SUGGEST_MODE)) {
                    suggestion.suggestMode(SuggestMode.resolve(parser.text()));
                } else if (parseFieldMatcher.match(fieldName, ACCURACY)) {
                    suggestion.accuracy(parser.floatValue());
                } else if (parseFieldMatcher.match(fieldName, SORT)) {
                    suggestion.sort(SortBy.resolve(parser.text()));
                } else if (parseFieldMatcher.match(fieldName, STRING_DISTANCE)) {
                    suggestion.stringDistance(StringDistanceImpl.resolve(parser.text()));
                } else if (parseFieldMatcher.match(fieldName, MAX_EDITS)) {
                    suggestion.maxEdits(parser.intValue());
                } else if (parseFieldMatcher.match(fieldName, MAX_INSPECTIONS)) {
                    suggestion.maxInspections(parser.intValue());
                } else if (parseFieldMatcher.match(fieldName, MAX_TERM_FREQ)) {
                    suggestion.maxTermFreq(parser.floatValue());
                } else if (parseFieldMatcher.match(fieldName, PREFIX_LENGTH)) {
                    suggestion.prefixLength(parser.intValue());
                } else if (parseFieldMatcher.match(fieldName, MIN_WORD_LENGTH)) {
                    suggestion.minWordLength(parser.intValue());
                } else if (parseFieldMatcher.match(fieldName, MIN_DOC_FREQ)) {
                    suggestion.minDocFreq(parser.floatValue());
                }
            } else {
                throw new IllegalArgumentException("suggester[term] doesn't support field [" + fieldName + "]");
            }
        }
        return suggestion;
    }

    @Override
    protected SuggestionContext innerBuild(QueryShardContext context) throws IOException {
        TermSuggestionContext suggestionContext = new TermSuggestionContext(TermSuggester.PROTOTYPE);
        return fillSuggestionContext(suggestionContext);
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        suggestMode.writeTo(out);
        out.writeFloat(accuracy);
        sort.writeTo(out);
        stringDistance.writeTo(out);
        out.writeVInt(maxEdits);
        out.writeVInt(maxInspections);
        out.writeFloat(maxTermFreq);
        out.writeVInt(prefixLength);
        out.writeVInt(minWordLength);
        out.writeFloat(minDocFreq);
    }

    @Override
    public TermSuggestionBuilder doReadFrom(StreamInput in, String name) throws IOException {
        TermSuggestionBuilder builder = new TermSuggestionBuilder(name);
        builder.suggestMode = SuggestMode.PROTOTYPE.readFrom(in);
        builder.accuracy = in.readFloat();
        builder.sort = SortBy.PROTOTYPE.readFrom(in);
        builder.stringDistance = StringDistanceImpl.PROTOTYPE.readFrom(in);
        builder.maxEdits = in.readVInt();
        builder.maxInspections = in.readVInt();
        builder.maxTermFreq = in.readFloat();
        builder.prefixLength = in.readVInt();
        builder.minWordLength = in.readVInt();
        builder.minDocFreq = in.readFloat();
        return builder;
    }

    @Override
    protected boolean doEquals(TermSuggestionBuilder other) {
        return Objects.equals(suggestMode, other.suggestMode) &&
               Objects.equals(accuracy, other.accuracy) &&
               Objects.equals(sort, other.sort) &&
               Objects.equals(stringDistance, other.stringDistance) &&
               Objects.equals(maxEdits, other.maxEdits) &&
               Objects.equals(maxInspections, other.maxInspections) &&
               Objects.equals(maxTermFreq, other.maxTermFreq) &&
               Objects.equals(prefixLength, other.prefixLength) &&
               Objects.equals(minWordLength, other.minWordLength) &&
               Objects.equals(minDocFreq, other.minDocFreq);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(suggestMode, accuracy, sort, stringDistance, maxEdits, maxInspections,
                            maxTermFreq, prefixLength, minWordLength, minDocFreq);
    }

    // Transfers the builder settings to the target TermSuggestionContext
    private TermSuggestionContext fillSuggestionContext(TermSuggestionContext context) {
        DirectSpellcheckerSettings settings = context.getDirectSpellCheckerSettings();
        settings.accuracy(accuracy);
        settings.maxEdits(maxEdits);
        settings.maxInspections(maxInspections);
        settings.maxTermFreq(maxTermFreq);
        settings.minDocFreq(minDocFreq);
        settings.minWordLength(minWordLength);
        settings.prefixLength(prefixLength);
        settings.sort(sort);
        settings.stringDistance(SuggestUtils.resolveStringDistance(stringDistance));
        settings.suggestMode(SuggestUtils.resolveSuggestMode(suggestMode));
        return context;
    }


    /** An enum representing the valid suggest modes. */
    public enum SuggestMode implements Writeable<SuggestMode> {
        /** Only suggest terms in the suggest text that aren't in the index. This is the default. */
        MISSING,
        /** Only suggest terms that occur in more docs then the original suggest text term. */
        POPULAR,
        /** Suggest any matching suggest terms based on tokens in the suggest text. */
        ALWAYS;

        protected static SuggestMode PROTOTYPE = MISSING;

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public SuggestMode readFrom(final StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown SuggestMode ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        public static SuggestMode resolve(final String str) {
            Objects.requireNonNull(str, "Input string is null");
            return valueOf(str.toUpperCase(Locale.ROOT));
        }
    }

    /** An enum representing the valid sorting options */
    public enum SortBy implements Writeable<SortBy> {
        /** Sort should first be based on score, then document frequency and then the term itself. */
        SCORE((byte) 0x0),
        /** Sort should first be based on document frequency, then score and then the term itself. */
        FREQUENCY((byte) 0x1);

        protected static SortBy PROTOTYPE = SCORE;

        private byte id;

        SortBy(byte id) {
            this.id = id;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public SortBy readFrom(final StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown SortBy ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        public static SortBy resolve(final String str) {
            Objects.requireNonNull(str, "Input string is null");
            return valueOf(str.toUpperCase(Locale.ROOT));
        }

        public byte id() {
            return id;
        }

        public static SortBy fromId(byte id) {
            if (id == 0) {
                return SCORE;
            } else if (id == 1) {
                return FREQUENCY;
            } else {
                throw new ElasticsearchException("Illegal suggest sort " + id);
            }
        }
    }

    /** An enum representing the valid string edit distance algorithms for determining suggestions. */
    public enum StringDistanceImpl implements Writeable<StringDistanceImpl> {
        /** This is the default and is based on <code>damerau_levenshtein</code>, but highly optimized
         * for comparing string distance for terms inside the index. */
        INTERNAL,
        /** String distance algorithm based on Damerau-Levenshtein algorithm. */
        DAMERAU_LEVENSHTEIN,
        /** String distance algorithm based on Levenstein edit distance algorithm. */
        LEVENSTEIN,
        /** String distance algorithm based on Jaro-Winkler algorithm. */
        JAROWINKLER,
        /** String distance algorithm based on character n-grams. */
        NGRAM;

        protected static StringDistanceImpl PROTOTYPE = INTERNAL;

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        @Override
        public StringDistanceImpl readFrom(final StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown StringDistanceImpl ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        public static StringDistanceImpl resolve(final String str) {
            Objects.requireNonNull(str, "Input string is null");
            final String distanceVal = str.toLowerCase(Locale.US);
            switch (distanceVal) {
                case "internal":
                    return INTERNAL;
                case "damerau_levenshtein":
                case "damerauLevenshtein":
                    return DAMERAU_LEVENSHTEIN;
                case "levenstein":
                    return LEVENSTEIN;
                case "ngram":
                    return NGRAM;
                case "jarowinkler":
                    return JAROWINKLER;
                default: throw new IllegalArgumentException("Illegal distance option " + str);
            }
        }
    }

}
