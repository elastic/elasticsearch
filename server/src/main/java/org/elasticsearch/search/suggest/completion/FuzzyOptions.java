/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Fuzzy options for completion suggester
 */
public class FuzzyOptions implements ToXContentFragment, Writeable {
    static final ParseField FUZZY_OPTIONS = new ParseField("fuzzy");
    private static final ParseField TRANSPOSITION_FIELD = new ParseField("transpositions");
    private static final ParseField MIN_LENGTH_FIELD = new ParseField("min_length");
    private static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    private static final ParseField UNICODE_AWARE_FIELD = new ParseField("unicode_aware");
    private static final ParseField MAX_DETERMINIZED_STATES_FIELD = new ParseField("max_determinized_states");

    /**
     * fuzzy : {
     *     "edit_distance" : STRING | INT
     *     "transpositions" : BOOLEAN
     *     "min_length" : INT
     *     "prefix_length" : INT
     *     "unicode_aware" : BOOLEAN
     *     "max_determinized_states" : INT
     * }
     */
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(FUZZY_OPTIONS.getPreferredName(), Builder::new);
    static {
        PARSER.declareInt(Builder::setFuzzyMinLength, MIN_LENGTH_FIELD);
        PARSER.declareInt(Builder::setMaxDeterminizedStates, MAX_DETERMINIZED_STATES_FIELD);
        PARSER.declareBoolean(Builder::setUnicodeAware, UNICODE_AWARE_FIELD);
        PARSER.declareInt(Builder::setFuzzyPrefixLength, PREFIX_LENGTH_FIELD);
        PARSER.declareBoolean(Builder::setTranspositions, TRANSPOSITION_FIELD);
        PARSER.declareField(Builder::setFuzziness, Fuzziness::parse, Fuzziness.FIELD, ObjectParser.ValueType.VALUE);
    }

    static FuzzyOptions parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private int editDistance;
    private boolean transpositions;
    private int fuzzyMinLength;
    private int fuzzyPrefixLength;
    private boolean unicodeAware;
    private int maxDeterminizedStates;

    private FuzzyOptions(
        int editDistance,
        boolean transpositions,
        int fuzzyMinLength,
        int fuzzyPrefixLength,
        boolean unicodeAware,
        int maxDeterminizedStates
    ) {
        this.editDistance = editDistance;
        this.transpositions = transpositions;
        this.fuzzyMinLength = fuzzyMinLength;
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        this.unicodeAware = unicodeAware;
        this.maxDeterminizedStates = maxDeterminizedStates;
    }

    /**
     * Read from a stream.
     */
    FuzzyOptions(StreamInput in) throws IOException {
        transpositions = in.readBoolean();
        unicodeAware = in.readBoolean();
        editDistance = in.readVInt();
        fuzzyMinLength = in.readVInt();
        fuzzyPrefixLength = in.readVInt();
        maxDeterminizedStates = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(transpositions);
        out.writeBoolean(unicodeAware);
        out.writeVInt(editDistance);
        out.writeVInt(fuzzyMinLength);
        out.writeVInt(fuzzyPrefixLength);
        out.writeVInt(maxDeterminizedStates);
    }

    /**
     * Returns the maximum number of edits
     */
    public int getEditDistance() {
        return editDistance;
    }

    /**
     * Returns if transpositions option is set
     *
     * if transpositions is set, then swapping one character for another counts as one edit instead of two.
     */
    public boolean isTranspositions() {
        return transpositions;
    }

    /**
     * Returns the length of input prefix after which edits are applied
     */
    public int getFuzzyMinLength() {
        return fuzzyMinLength;
    }

    /**
     * Returns the minimum length of the input prefix required to apply any edits
     */
    public int getFuzzyPrefixLength() {
        return fuzzyPrefixLength;
    }

    /**
     * Returns if all measurements (like edit distance, transpositions and lengths) are in unicode code
     * points (actual letters) instead of bytes.
     */
    public boolean isUnicodeAware() {
        return unicodeAware;
    }

    /**
     * Returns the maximum automaton states allowed for fuzzy expansion
     */
    public int getMaxDeterminizedStates() {
        return maxDeterminizedStates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FuzzyOptions that = (FuzzyOptions) o;

        if (editDistance != that.editDistance) return false;
        if (transpositions != that.transpositions) return false;
        if (fuzzyMinLength != that.fuzzyMinLength) return false;
        if (fuzzyPrefixLength != that.fuzzyPrefixLength) return false;
        if (unicodeAware != that.unicodeAware) return false;
        return maxDeterminizedStates == that.maxDeterminizedStates;

    }

    @Override
    public int hashCode() {
        int result = editDistance;
        result = 31 * result + (transpositions ? 1 : 0);
        result = 31 * result + fuzzyMinLength;
        result = 31 * result + fuzzyPrefixLength;
        result = 31 * result + (unicodeAware ? 1 : 0);
        result = 31 * result + maxDeterminizedStates;
        return result;
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

    /**
     * Options for fuzzy queries
     */
    public static class Builder {

        private int editDistance = FuzzyCompletionQuery.DEFAULT_MAX_EDITS;
        private boolean transpositions = FuzzyCompletionQuery.DEFAULT_TRANSPOSITIONS;
        private int fuzzyMinLength = FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH;
        private int fuzzyPrefixLength = FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX;
        private boolean unicodeAware = FuzzyCompletionQuery.DEFAULT_UNICODE_AWARE;
        private int maxDeterminizedStates = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

        public Builder() {}

        /**
         * Sets the level of fuzziness used to create suggestions using a {@link Fuzziness} instance.
         * The default value is {@link Fuzziness#ONE} which allows for an "edit distance" of one.
         */
        public Builder setFuzziness(int editDistance) {
            if (editDistance < 0 || editDistance > 2) {
                throw new IllegalArgumentException("fuzziness must be between 0 and 2");
            }
            this.editDistance = editDistance;
            return this;
        }

        /**
         * Sets the level of fuzziness used to create suggestions using a {@link Fuzziness} instance.
         * The default value is {@link Fuzziness#ONE} which allows for an "edit distance" of one.
         */
        public Builder setFuzziness(Fuzziness fuzziness) {
            Objects.requireNonNull(fuzziness, "fuzziness must not be null");
            return setFuzziness(fuzziness.asDistance());
        }

        /**
         * Sets if transpositions (swapping one character for another) counts as one character
         * change or two.
         * Defaults to true, meaning it uses the fuzzier option of counting transpositions as
         * a single change.
         */
        public Builder setTranspositions(boolean transpositions) {
            this.transpositions = transpositions;
            return this;
        }

        /**
         * Sets the minimum length of input string before fuzzy suggestions are returned, defaulting
         * to 3.
         */
        public Builder setFuzzyMinLength(int fuzzyMinLength) {
            if (fuzzyMinLength < 0) {
                throw new IllegalArgumentException("fuzzyMinLength must not be negative");
            }
            this.fuzzyMinLength = fuzzyMinLength;
            return this;
        }

        /**
         * Sets the minimum length of the input, which is not checked for fuzzy alternatives, defaults to 1
         */
        public Builder setFuzzyPrefixLength(int fuzzyPrefixLength) {
            if (fuzzyPrefixLength < 0) {
                throw new IllegalArgumentException("fuzzyPrefixLength must not be negative");
            }
            this.fuzzyPrefixLength = fuzzyPrefixLength;
            return this;
        }

        /**
         * Sets the maximum automaton states allowed for the fuzzy expansion
         */
        public Builder setMaxDeterminizedStates(int maxDeterminizedStates) {
            if (maxDeterminizedStates < 0) {
                throw new IllegalArgumentException("maxDeterminizedStates must not be negative");
            }
            this.maxDeterminizedStates = maxDeterminizedStates;
            return this;
        }

        /**
         * Set to true if all measurements (like edit distance, transpositions and lengths) are in unicode
         * code points (actual letters) instead of bytes. Default is false.
         */
        public Builder setUnicodeAware(boolean unicodeAware) {
            this.unicodeAware = unicodeAware;
            return this;
        }

        public FuzzyOptions build() {
            return new FuzzyOptions(editDistance, transpositions, fuzzyMinLength, fuzzyPrefixLength, unicodeAware, maxDeterminizedStates);
        }
    }
}
