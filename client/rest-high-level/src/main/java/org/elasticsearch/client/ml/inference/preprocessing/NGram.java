/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.IntStream;


/**
 * PreProcessor for n-gram encoding a string
 */
public class NGram implements PreProcessor {

    public static final String NAME = "n_gram_encoding";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_PREFIX = new ParseField("feature_prefix");
    public static final ParseField NGRAMS = new ParseField("n_grams");
    public static final ParseField START = new ParseField("start");
    public static final ParseField LENGTH = new ParseField("length");
    public static final ParseField CUSTOM = new ParseField("custom");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<NGram, Void> PARSER = new ConstructingObjectParser<NGram, Void>(
            NAME,
            true,
            a -> new NGram((String)a[0],
                (List<Integer>)a[1],
                (Integer)a[2],
                (Integer)a[3],
                (Boolean)a[4],
                (String)a[5]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareIntArray(ConstructingObjectParser.constructorArg(), NGRAMS);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), START);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), LENGTH);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FEATURE_PREFIX);
    }

    public static NGram fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final String featurePrefix;
    private final List<Integer> nGrams;
    private final Integer start;
    private final Integer length;
    private final Boolean custom;

    NGram(String field, List<Integer> nGrams, Integer start, Integer length, Boolean custom, String featurePrefix) {
        this.field = field;
        this.featurePrefix = featurePrefix;
        this.nGrams = nGrams;
        this.start = start;
        this.length = length;
        this.custom = custom;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        if (featurePrefix != null) {
            builder.field(FEATURE_PREFIX.getPreferredName(), featurePrefix);
        }
        if (nGrams != null) {
            builder.field(NGRAMS.getPreferredName(), nGrams);
        }
        if (start != null) {
            builder.field(START.getPreferredName(), start);
        }
        if (length != null) {
            builder.field(LENGTH.getPreferredName(), length);
        }
        if (custom != null) {
            builder.field(CUSTOM.getPreferredName(), custom);
        }
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public String getFeaturePrefix() {
        return featurePrefix;
    }

    public List<Integer> getnGrams() {
        return nGrams;
    }

    public Integer getStart() {
        return start;
    }

    public Integer getLength() {
        return length;
    }

    public Boolean getCustom() {
        return custom;
    }

    public List<String> outputFields() {
        return allPossibleNGramOutputFeatureNames();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NGram nGram = (NGram) o;
        return Objects.equals(field, nGram.field) &&
            Objects.equals(featurePrefix, nGram.featurePrefix) &&
            Objects.equals(nGrams, nGram.nGrams) &&
            Objects.equals(start, nGram.start) &&
            Objects.equals(length, nGram.length) &&
            Objects.equals(custom, nGram.custom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featurePrefix, start, length, custom, nGrams);
    }

    private String nGramFeature(int nGram, int pos) {
        return featurePrefix
            + "."
            + nGram
            + pos;
    }

    private List<String> allPossibleNGramOutputFeatureNames() {
        int totalNgrams = 0;
        for (int nGram : nGrams) {
            totalNgrams += (length - (nGram - 1));
        }
        if (totalNgrams <= 0) {
            return Collections.emptyList();
        }
        List<String> ngramOutputs = new ArrayList<>(totalNgrams);

        for (int nGram : nGrams) {
            IntFunction<String> func = i -> nGramFeature(nGram, i);
            IntStream.range(0, (length - (nGram - 1))).mapToObj(func).forEach(ngramOutputs::add);
        }
        return ngramOutputs;
    }

    public static Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {

        private String field;
        private String featurePrefix;
        private List<Integer> nGrams;
        private Integer start;
        private Integer length;
        private Boolean custom;

        public Builder(String field) {
            this.field = field;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setCustom(boolean custom) {
            this.custom = custom;
            return this;
        }

        public Builder setFeaturePrefix(String featurePrefix) {
            this.featurePrefix = featurePrefix;
            return this;
        }

        public Builder setnGrams(List<Integer> nGrams) {
            this.nGrams = nGrams;
            return this;
        }

        public Builder setStart(Integer start) {
            this.start = start;
            return this;
        }

        public Builder setLength(Integer length) {
            this.length = length;
            return this;
        }

        public Builder setCustom(Boolean custom) {
            this.custom = custom;
            return this;
        }

        public NGram build() {
            return new NGram(field, nGrams, start, length, custom, featurePrefix);
        }
    }
}
