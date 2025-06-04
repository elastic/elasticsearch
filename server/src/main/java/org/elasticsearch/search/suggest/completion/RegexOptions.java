/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Regular expression options for completion suggester
 */
public class RegexOptions implements ToXContentFragment, Writeable {
    static final ParseField REGEX_OPTIONS = new ParseField("regex");
    private static final ParseField FLAGS_VALUE = new ParseField("flags", "flags_value");
    private static final ParseField MAX_DETERMINIZED_STATES = new ParseField("max_determinized_states");

    /**
     * regex: {
     *     "flags" : STRING | INT
     *     "max_determinized_states" : INT
     * }
     */
    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(REGEX_OPTIONS.getPreferredName(), Builder::new);
    static {
        PARSER.declareInt(Builder::setMaxDeterminizedStates, MAX_DETERMINIZED_STATES);
        PARSER.declareField((parser, builder, aVoid) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                builder.setFlags(parser.text());
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                builder.setFlagsValue(parser.intValue());
            } else {
                throw new ElasticsearchParseException(
                    REGEX_OPTIONS.getPreferredName() + " " + FLAGS_VALUE.getPreferredName() + " supports string or number"
                );
            }
        }, FLAGS_VALUE, ObjectParser.ValueType.VALUE);
    }

    public static Builder builder() {
        return new Builder();
    }

    static RegexOptions parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    private final int flagsValue;
    private final int maxDeterminizedStates;

    private RegexOptions(int flagsValue, int maxDeterminizedStates) {
        this.flagsValue = flagsValue;
        this.maxDeterminizedStates = maxDeterminizedStates;
    }

    /**
     * Read from a stream.
     */
    RegexOptions(StreamInput in) throws IOException {
        this.flagsValue = in.readVInt();
        this.maxDeterminizedStates = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(flagsValue);
        out.writeVInt(maxDeterminizedStates);
    }

    /**
     * Returns internal regular expression syntax flag value
     * see {@link RegexpFlag#value()}
     */
    public int getFlagsValue() {
        return flagsValue;
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

        RegexOptions that = (RegexOptions) o;

        if (flagsValue != that.flagsValue) return false;
        return maxDeterminizedStates == that.maxDeterminizedStates;

    }

    @Override
    public int hashCode() {
        int result = flagsValue;
        result = 31 * result + maxDeterminizedStates;
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(REGEX_OPTIONS.getPreferredName());
        builder.field(FLAGS_VALUE.getPreferredName(), flagsValue);
        builder.field(MAX_DETERMINIZED_STATES.getPreferredName(), maxDeterminizedStates);
        builder.endObject();
        return builder;
    }

    /**
     * Options for regular expression queries
     */
    public static class Builder {
        private int flagsValue = RegExp.ALL;
        private int maxDeterminizedStates = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

        public Builder() {}

        /**
         * Sets the regular expression syntax flags
         * see {@link RegexpFlag}
         */
        public Builder setFlags(String flags) {
            this.flagsValue = RegexpFlag.resolveValue(flags);
            return this;
        }

        private Builder setFlagsValue(int flagsValue) {
            this.flagsValue = flagsValue;
            return this;
        }

        /**
         * Sets the maximum automaton states allowed for the regular expression expansion
         */
        public Builder setMaxDeterminizedStates(int maxDeterminizedStates) {
            if (maxDeterminizedStates < 0) {
                throw new IllegalArgumentException("maxDeterminizedStates must not be negative");
            }
            this.maxDeterminizedStates = maxDeterminizedStates;
            return this;
        }

        public RegexOptions build() {
            return new RegexOptions(flagsValue, maxDeterminizedStates);
        }
    }
}
