/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.term;

import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.LuceneLevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.suggest.DirectSpellcheckerSettings;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.ACCURACY_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.MAX_EDITS_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.MAX_INSPECTIONS_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.MAX_TERM_FREQ_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.MIN_DOC_FREQ_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.MIN_WORD_LENGTH_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.PREFIX_LENGTH_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.SORT_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.STRING_DISTANCE_FIELD;
import static org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder.SUGGESTMODE_FIELD;

/**
 * Defines the actual suggest command. Each command uses the global options
 * unless defined in the suggestion itself. All options are the same as the
 * global options, but are only applicable for this suggestion.
 */
public class TermSuggestionBuilder extends SuggestionBuilder<TermSuggestionBuilder> {

    public static final String SUGGESTION_NAME = "term";

    private SuggestMode suggestMode = SuggestMode.MISSING;
    private float accuracy = DEFAULT_ACCURACY;
    private SortBy sort = SortBy.SCORE;
    private StringDistanceImpl stringDistance = StringDistanceImpl.INTERNAL;
    private int maxEdits = DEFAULT_MAX_EDITS;
    private int maxInspections = DEFAULT_MAX_INSPECTIONS;
    private float maxTermFreq = DEFAULT_MAX_TERM_FREQ;
    private int prefixLength = DEFAULT_PREFIX_LENGTH;
    private int minWordLength = DEFAULT_MIN_WORD_LENGTH;
    private float minDocFreq = DEFAULT_MIN_DOC_FREQ;

    public TermSuggestionBuilder(String field) {
        super(field);
    }

    /**
     * internal copy constructor that copies over all class field except field.
     */
    private TermSuggestionBuilder(String field, TermSuggestionBuilder in) {
        super(field, in);
        suggestMode = in.suggestMode;
        accuracy = in.accuracy;
        sort = in.sort;
        stringDistance = in.stringDistance;
        maxEdits = in.maxEdits;
        maxInspections = in.maxInspections;
        maxTermFreq = in.maxTermFreq;
        prefixLength = in.prefixLength;
        minWordLength = in.minWordLength;
        minDocFreq = in.minDocFreq;
    }

    /**
     * Read from a stream.
     */
    public TermSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        suggestMode = SuggestMode.readFromStream(in);
        accuracy = in.readFloat();
        sort = SortBy.readFromStream(in);
        stringDistance = StringDistanceImpl.readFromStream(in);
        maxEdits = in.readVInt();
        maxInspections = in.readVInt();
        maxTermFreq = in.readFloat();
        prefixLength = in.readVInt();
        minWordLength = in.readVInt();
        minDocFreq = in.readFloat();
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
     * Default is {@code 0.5}
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
    public float accuracy() {
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
     * <li><code>levenshtein</code> - String distance algorithm based on
     * Levenshtein edit distance algorithm.
     * <li><code>jaro_winkler</code> - String distance algorithm based on
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
     * {@code 2}.
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
    public int maxEdits() {
        return maxEdits;
    }

    /**
     * A factor that is used to multiply with the size in order to inspect more
     * candidate suggestions. Can improve accuracy at the cost of performance.
     * Defaults to {@code 5}.
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
    public int maxInspections() {
        return maxInspections;
    }

    /**
     * Sets a maximum threshold in number of documents a suggest text token can
     * exist in order to be corrected. Can be a relative percentage number (e.g
     * 0.4) or an absolute number to represent document frequencies. If an value
     * higher than 1 is specified then fractional can not be specified. Defaults
     * to {@code 0.01}.
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
    public float maxTermFreq() {
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
    public int prefixLength() {
        return prefixLength;
    }

    /**
     * The minimum length a suggest text term must have in order to be
     * corrected. Defaults to {@code 4}.
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
    public int minWordLength() {
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
    public float minDocFreq() {
        return minDocFreq;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SUGGESTMODE_FIELD.getPreferredName(), suggestMode);
        builder.field(ACCURACY_FIELD.getPreferredName(), accuracy);
        builder.field(SORT_FIELD.getPreferredName(), sort);
        builder.field(STRING_DISTANCE_FIELD.getPreferredName(), stringDistance);
        builder.field(MAX_EDITS_FIELD.getPreferredName(), maxEdits);
        builder.field(MAX_INSPECTIONS_FIELD.getPreferredName(), maxInspections);
        builder.field(MAX_TERM_FREQ_FIELD.getPreferredName(), maxTermFreq);
        builder.field(PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
        builder.field(MIN_WORD_LENGTH_FIELD.getPreferredName(), minWordLength);
        builder.field(MIN_DOC_FREQ_FIELD.getPreferredName(), minDocFreq);
        return builder;
    }

    public static TermSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
        TermSuggestionBuilder tmpSuggestion = new TermSuggestionBuilder("_na_");
        XContentParser.Token token;
        String currentFieldName = null;
        String fieldname = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SuggestionBuilder.ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.analyzer(parser.text());
                } else if (SuggestionBuilder.FIELDNAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fieldname = parser.text();
                } else if (SuggestionBuilder.SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.size(parser.intValue());
                } else if (SuggestionBuilder.SHARDSIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.shardSize(parser.intValue());
                } else if (SUGGESTMODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.suggestMode(SuggestMode.resolve(parser.text()));
                } else if (ACCURACY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.accuracy(parser.floatValue());
                } else if (SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.sort(SortBy.resolve(parser.text()));
                } else if (STRING_DISTANCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.stringDistance(StringDistanceImpl.resolve(parser.text()));
                } else if (MAX_EDITS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.maxEdits(parser.intValue());
                } else if (MAX_INSPECTIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.maxInspections(parser.intValue());
                } else if (MAX_TERM_FREQ_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.maxTermFreq(parser.floatValue());
                } else if (PREFIX_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.prefixLength(parser.intValue());
                } else if (MIN_WORD_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.minWordLength(parser.intValue());
                } else if (MIN_DOC_FREQ_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tmpSuggestion.minDocFreq(parser.floatValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "suggester[term] doesn't support field [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "suggester[term] parsing failed on [" + currentFieldName + "]");
            }
        }

        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (fieldname == null) {
            throw new ElasticsearchParseException("the required field option [" + FIELDNAME_FIELD.getPreferredName() + "] is missing");
        }
        return new TermSuggestionBuilder(fieldname, tmpSuggestion);
    }

    @Override
    public SuggestionContext build(SearchExecutionContext context) throws IOException {
        TermSuggestionContext suggestionContext = new TermSuggestionContext(context);
        // copy over common settings to each suggestion builder
        populateCommonFields(context, suggestionContext);
        // Transfers the builder settings to the target TermSuggestionContext
        DirectSpellcheckerSettings settings = suggestionContext.getDirectSpellCheckerSettings();
        settings.accuracy(accuracy);
        settings.maxEdits(maxEdits);
        settings.maxInspections(maxInspections);
        settings.maxTermFreq(maxTermFreq);
        settings.minDocFreq(minDocFreq);
        settings.minWordLength(minWordLength);
        settings.prefixLength(prefixLength);
        settings.sort(sort);
        settings.stringDistance(stringDistance.toLucene());
        settings.suggestMode(suggestMode.toLucene());
        return suggestionContext;
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }

    @Override
    protected boolean doEquals(TermSuggestionBuilder other) {
        return Objects.equals(suggestMode, other.suggestMode)
            && Objects.equals(accuracy, other.accuracy)
            && Objects.equals(sort, other.sort)
            && Objects.equals(stringDistance, other.stringDistance)
            && Objects.equals(maxEdits, other.maxEdits)
            && Objects.equals(maxInspections, other.maxInspections)
            && Objects.equals(maxTermFreq, other.maxTermFreq)
            && Objects.equals(prefixLength, other.prefixLength)
            && Objects.equals(minWordLength, other.minWordLength)
            && Objects.equals(minDocFreq, other.minDocFreq);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            suggestMode,
            accuracy,
            sort,
            stringDistance,
            maxEdits,
            maxInspections,
            maxTermFreq,
            prefixLength,
            minWordLength,
            minDocFreq
        );
    }

    /** An enum representing the valid suggest modes. */
    public enum SuggestMode implements Writeable {
        /** Only suggest terms in the suggest text that aren't in the index. This is the default. */
        MISSING {
            @Override
            public org.apache.lucene.search.spell.SuggestMode toLucene() {
                return org.apache.lucene.search.spell.SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
            }
        },
        /** Only suggest terms that occur in more docs then the original suggest text term. */
        POPULAR {
            @Override
            public org.apache.lucene.search.spell.SuggestMode toLucene() {
                return org.apache.lucene.search.spell.SuggestMode.SUGGEST_MORE_POPULAR;
            }
        },
        /** Suggest any matching suggest terms based on tokens in the suggest text. */
        ALWAYS {
            @Override
            public org.apache.lucene.search.spell.SuggestMode toLucene() {
                return org.apache.lucene.search.spell.SuggestMode.SUGGEST_ALWAYS;
            }
        };

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static SuggestMode readFromStream(final StreamInput in) throws IOException {
            return in.readEnum(SuggestMode.class);
        }

        public static SuggestMode resolve(final String str) {
            Objects.requireNonNull(str, "Input string is null");
            return valueOf(str.toUpperCase(Locale.ROOT));
        }

        public abstract org.apache.lucene.search.spell.SuggestMode toLucene();
    }

    /** An enum representing the valid string edit distance algorithms for determining suggestions. */
    public enum StringDistanceImpl implements Writeable {
        /** This is the default and is based on <code>damerau_levenshtein</code>, but highly optimized
         * for comparing string distance for terms inside the index. */
        INTERNAL {
            @Override
            public StringDistance toLucene() {
                return DirectSpellChecker.INTERNAL_LEVENSHTEIN;
            }
        },
        /** String distance algorithm based on Damerau-Levenshtein algorithm. */
        DAMERAU_LEVENSHTEIN {
            @Override
            public StringDistance toLucene() {
                return new LuceneLevenshteinDistance();
            }
        },
        /** String distance algorithm based on Levenshtein edit distance algorithm. */
        LEVENSHTEIN {
            @Override
            public StringDistance toLucene() {
                return new LevenshteinDistance();
            }
        },
        /** String distance algorithm based on Jaro-Winkler algorithm. */
        JARO_WINKLER {
            @Override
            public StringDistance toLucene() {
                return new JaroWinklerDistance();
            }
        },
        /** String distance algorithm based on character n-grams. */
        NGRAM {
            @Override
            public StringDistance toLucene() {
                return new NGramDistance();
            }
        };

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static StringDistanceImpl readFromStream(final StreamInput in) throws IOException {
            return in.readEnum(StringDistanceImpl.class);
        }

        public static StringDistanceImpl resolve(final String str) {
            Objects.requireNonNull(str, "Input string is null");
            final String distanceVal = str.toLowerCase(Locale.ROOT);
            return switch (distanceVal) {
                case "internal" -> INTERNAL;
                case "damerau_levenshtein" -> DAMERAU_LEVENSHTEIN;
                case "levenshtein" -> LEVENSHTEIN;
                case "ngram" -> NGRAM;
                case "jaro_winkler" -> JARO_WINKLER;
                default -> throw new IllegalArgumentException("Illegal distance option " + str);
            };
        }

        public abstract StringDistance toLucene();
    }

}
