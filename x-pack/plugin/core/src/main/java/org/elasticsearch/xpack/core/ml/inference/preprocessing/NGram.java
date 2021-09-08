/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.util.RamUsageEstimator.sizeOf;

/**
 * PreProcessor for n-gram encoding a string
 */
public class NGram implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    private static final int DEFAULT_START = 0;
    private static final int DEFAULT_LENGTH = 50;
    private static final int MAX_LENGTH = 100;
    private static final int MIN_GRAM = 1;
    private static final int MAX_GRAM = 5;

    private static String defaultPrefix(Integer start, Integer length) {
        return "ngram_"
            + (start == null ? DEFAULT_START : start)
            + "_"
            + (length == null ? DEFAULT_LENGTH : length);
    }

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(NGram.class);
    public static final ParseField NAME = new ParseField("n_gram_encoding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_PREFIX = new ParseField("feature_prefix");
    public static final ParseField NGRAMS = new ParseField("n_grams");
    public static final ParseField START = new ParseField("start");
    public static final ParseField LENGTH = new ParseField("length");
    public static final ParseField CUSTOM = new ParseField("custom");

    private static final ConstructingObjectParser<NGram, PreProcessorParseContext> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<NGram, PreProcessorParseContext> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<NGram, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<NGram, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new NGram((String)a[0],
                (List<Integer>)a[1],
                (Integer)a[2],
                (Integer)a[3],
                a[4] == null ? c.isCustomByDefault() : (Boolean)a[4],
                (String)a[5]));
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareIntArray(ConstructingObjectParser.constructorArg(), NGRAMS);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), START);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), LENGTH);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), FEATURE_PREFIX);
        return parser;
    }

    public static NGram fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    public static NGram fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    private final String field;
    private final String featurePrefix;
    private final int[] nGrams;
    private final int start;
    private final int length;
    private final boolean custom;

    NGram(String field,
          List<Integer> nGrams,
          Integer start,
          Integer length,
          Boolean custom,
          String featurePrefix) {
        this(field,
            featurePrefix == null ? defaultPrefix(start, length) : featurePrefix,
            Sets.newHashSet(nGrams).stream().mapToInt(Integer::intValue).toArray(),
            start == null ? DEFAULT_START : start,
            length == null ? DEFAULT_LENGTH : length,
            custom != null && custom);
    }

    public NGram(String field, String featurePrefix, int[] nGrams, int start, int length, boolean custom) {
        this.field = ExceptionsHelper.requireNonNull(field, FIELD);
        this.featurePrefix = ExceptionsHelper.requireNonNull(featurePrefix, FEATURE_PREFIX);
        this.nGrams = ExceptionsHelper.requireNonNull(nGrams, NGRAMS);
        if (nGrams.length == 0) {
            throw ExceptionsHelper.badRequestException("[{}] must not be empty", NGRAMS.getPreferredName());
        }
        if (Arrays.stream(this.nGrams).anyMatch(i -> i < MIN_GRAM || i > MAX_GRAM)) {
            throw ExceptionsHelper.badRequestException(
                "[{}] is invalid [{}]; minimum supported value is [{}]; maximum supported value is [{}]",
                NGRAMS.getPreferredName(),
                Arrays.stream(nGrams).mapToObj(String::valueOf).collect(Collectors.joining(", ")),
                MIN_GRAM,
                MAX_GRAM);
        }
        this.start = start;
        if (start < 0 && length + start > 0) {
            throw ExceptionsHelper.badRequestException(
                "if [start] is negative, [length] + [start] must be less than 0");
        }
        this.length = length;
        if (length <= 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive integer", LENGTH.getPreferredName());
        }
        if (length > MAX_LENGTH) {
            throw ExceptionsHelper.badRequestException("[{}] must be not be greater than [{}]", LENGTH.getPreferredName(), MAX_LENGTH);
        }
        if (Arrays.stream(this.nGrams).anyMatch(i -> i > length)) {
            throw ExceptionsHelper.badRequestException(
                "[{}] and [{}] are invalid; all ngrams must be shorter than or equal to length [{}]",
                NGRAMS.getPreferredName(),
                LENGTH.getPreferredName(),
                length);
        }
        this.custom = custom;
    }

    public NGram(StreamInput in) throws IOException {
        this.field = in.readString();
        this.featurePrefix = in.readString();
        this.nGrams = in.readVIntArray();
        this.start = in.readInt();
        this.length = in.readVInt();
        this.custom = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeString(featurePrefix);
        out.writeVIntArray(nGrams);
        out.writeInt(start);
        out.writeVInt(length);
        out.writeBoolean(custom);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public List<String> inputFields() {
        return Collections.singletonList(field);
    }

    @Override
    public List<String> outputFields() {
        return allPossibleNGramOutputFeatureNames();
    }

    @Override
    public void process(Map<String, Object> fields) {
        Object value = fields.get(field);
        if (value == null) {
            return;
        }
        final String stringValue = value.toString();
        // String is too small for the starting point
        if (start > stringValue.length() || stringValue.length() + start < 0) {
            return;
        }
        final int startPos = start < 0 ? (stringValue.length() + start) : start;
        final int len = Math.min(startPos + length, stringValue.length());
        for (int nGram : nGrams) {
            for (int i = 0; i < len; i++) {
                if (startPos + i + nGram > len) {
                    break;
                }
                fields.put(nGramFeature(nGram, i), stringValue.substring(startPos + i, startPos + i + nGram));
            }
        }
    }

    @Override
    public Map<String, String> reverseLookup() {
        return outputFields().stream().collect(Collectors.toMap(Function.identity(), ignored -> field));
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return TextFieldMapper.CONTENT_TYPE;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += sizeOf(field);
        size += sizeOf(featurePrefix);
        size += sizeOf(nGrams);
        return size;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(FEATURE_PREFIX.getPreferredName(), featurePrefix);
        builder.field(NGRAMS.getPreferredName(), nGrams);
        builder.field(START.getPreferredName(), start);
        builder.field(LENGTH.getPreferredName(), length);
        builder.field(CUSTOM.getPreferredName(), custom);
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public String getFeaturePrefix() {
        return featurePrefix;
    }

    public int[] getnGrams() {
        return nGrams;
    }

    public int getStart() {
        return start;
    }

    public int getLength() {
        return length;
    }

    @Override
    public boolean isCustom() {
        return custom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NGram nGram = (NGram) o;
        return start == nGram.start &&
            length == nGram.length &&
            custom == nGram.custom &&
            Objects.equals(field, nGram.field) &&
            Objects.equals(featurePrefix, nGram.featurePrefix) &&
            Arrays.equals(nGrams, nGram.nGrams);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field, featurePrefix, start, length, custom);
        result = 31 * result + Arrays.hashCode(nGrams);
        return result;
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
}
