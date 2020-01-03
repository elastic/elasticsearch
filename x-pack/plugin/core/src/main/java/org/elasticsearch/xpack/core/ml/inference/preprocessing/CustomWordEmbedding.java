/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding.FeatureExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding.FeatureUtils;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding.FeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding.NGramFeatureExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding.RelevantScriptFeatureExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding.ScriptFeatureExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This is a pre-processor that embeds text into a numerical vector.
 *
 * It calculates a set of features based on script type, ngram hashes, and most common script values.
 *
 * The features are then concatenated with specific quantization scales and weights into a vector of length 80.
 *
 * This is a fork and a port of: https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/embedding_network.cc
 */
public class CustomWordEmbedding implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CustomWordEmbedding.class);
    public static final int MAX_STRING_SIZE_IN_BYTES = 10000;
    public static final ParseField NAME = new ParseField("custom_word_embedding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField DEST_FIELD = new ParseField("dest_field");
    public static final ParseField EMBEDDING_WEIGHTS = new ParseField("embedding_weights");
    public static final ParseField EMBEDDING_QUANT_SCALES = new ParseField("embedding_quant_scales");

    public static final ConstructingObjectParser<CustomWordEmbedding, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<CustomWordEmbedding, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<CustomWordEmbedding, Void> createParser(boolean lenient) {
        ConstructingObjectParser<CustomWordEmbedding, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new CustomWordEmbedding((short[][])a[0], (byte[][])a[1], (String)a[2], (String)a[3]));

        parser.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                List<List<Short>> listOfListOfShorts = parseArrays(EMBEDDING_QUANT_SCALES.getPreferredName(),
                    XContentParser::shortValue,
                    p);
                short[][] primitiveShorts = new short[listOfListOfShorts.size()][];
                int i = 0;
                for (List<Short> shorts : listOfListOfShorts) {
                    short[] innerShorts = new short[shorts.size()];
                    for (int j = 0; j < shorts.size(); j++) {
                        innerShorts[j] = shorts.get(j);
                    }
                    primitiveShorts[i++] = innerShorts;
                }
                return primitiveShorts;
            },
            EMBEDDING_QUANT_SCALES,
            ObjectParser.ValueType.VALUE_ARRAY);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                List<byte[]> values = new ArrayList<>();
                while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                    values.add(p.binaryValue());
                }
                byte[][] primitiveBytes = new byte[values.size()][];
                int i = 0;
                for (byte[] bytes : values) {
                    primitiveBytes[i++] = bytes;
                }
                return primitiveBytes;
            },
            EMBEDDING_WEIGHTS,
            ObjectParser.ValueType.VALUE_ARRAY);
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), DEST_FIELD);
        return parser;
    }

    private static <T> List<List<T>> parseArrays(String fieldName,
                                                 CheckedFunction<XContentParser, T, IOException> fromParser,
                                                 XContentParser p) throws IOException {
        if (p.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + fieldName + "]");
        }
        List<List<T>> values = new ArrayList<>();
        while(p.nextToken() != XContentParser.Token.END_ARRAY) {
            if (p.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("unexpected token [" + p.currentToken() + "] for [" + fieldName + "]");
            }
            List<T> innerList = new ArrayList<>();
            while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                if(p.currentToken().isValue() == false) {
                    throw new IllegalStateException("expected non-null value but got [" + p.currentToken() + "] " +
                        "for [" + fieldName + "]");
                }
                innerList.add(fromParser.apply(p));
            }
            values.add(innerList);
        }
        return values;
    }

    public static CustomWordEmbedding fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static CustomWordEmbedding fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final int CONCAT_LAYER_SIZE = 80;
    private static final int[] EMBEDDING_DIMENSIONS = new int[]{16, 16, 8, 8, 16, 16};

    // Order matters
    private static final List<FeatureExtractor> FEATURE_EXTRACTORS = Arrays.asList(
        new NGramFeatureExtractor(2, 1000),
        new NGramFeatureExtractor(4, 5000),
        new RelevantScriptFeatureExtractor(),
        new ScriptFeatureExtractor(),
        new NGramFeatureExtractor(3, 5000),
        new NGramFeatureExtractor(1, 100)
    );

    private final short[][] embeddingsQuantScales;
    private final byte[][] embeddingsWeights;
    private final String fieldName;
    private final String destField;

    public CustomWordEmbedding(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.destField = in.readString();
        this.embeddingsWeights = in.readArray(StreamInput::readByteArray, (length) -> new byte[length][]);
        this.embeddingsQuantScales = in.readArray((input -> {
            int length = input.readVInt();
            short[] shorts = new short[length];
            for (int i = 0; i < length; i++) {
                shorts[i] = in.readShort();
            }
            return shorts;
        }), (length) -> new short[length][]);
    }

    public CustomWordEmbedding(short[][] embeddingsQuantScales, byte[][] embeddingsWeights, String fieldName, String destField) {
        this.embeddingsQuantScales = embeddingsQuantScales;
        this.embeddingsWeights = embeddingsWeights;
        this.fieldName = fieldName;
        this.destField = destField;
    }

    /**
     * Derived from: https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/embedding_network.cc#L74
     */
    private double[] concatEmbeddings(List<FeatureValue[]> featureVectors) {
        double[] concat = new double[CONCAT_LAYER_SIZE];

        int offset = 0;
        // "esIndex" stands for "embedding space index".
        for (int esIndex = 0; esIndex < featureVectors.size(); ++esIndex) {
            byte[] embeddingWeight = this.embeddingsWeights[esIndex];
            short[] quants = this.embeddingsQuantScales[esIndex];
            int embeddingDim = EMBEDDING_DIMENSIONS[esIndex];

            FeatureValue[] featureVector = featureVectors.get(esIndex);
            assert (offset + embeddingDim <= concat.length);
            for (FeatureValue featureValue : featureVector) {
                // Multiplier for each embedding weight.
                int row = featureValue.getRow();
                double multiplier = featureValue.getWeight() * shortToDouble(quants[row]);

                // Iterate across columns for this row
                for (int i = 0; i < embeddingDim; ++i) {
                    double value = (getRowMajorData(embeddingWeight, embeddingDim, row, i)) * multiplier;
                    int concatIndex = offset + i;
                    concat[concatIndex] += value;
                }
            }
            offset += embeddingDim;
        }

        return concat;
    }

    /**
     * Internal logic to CLD3. The quantiles are stored as "compact float16" values.
     * These values are stored as `unsigned int16` in C++, best represented by a `short` Java
     * To "expand" these to a regular float, the mantissa bytes are filled with 0, a la a bit shift of 16
     * The memory is then statically cast to a float with the shift.
     *
     * The following satisfies this in java
     * @param s Represents a "compact float16", i.e. unsigned int16
     * @return Float representation with the short shifted over to zero out the underlying mantissa bytes
     */
    private static double shortToDouble(short s) {
        return Float.intBitsToFloat(s << 16);
    }

    private static int getRowMajorData(byte[] data, int colDim, int row, int col) {
        return data[row * colDim + col];
    }

    @Override
    public void process(Map<String, Object> fields) {
        Object field = fields.get(fieldName);
        if ((field instanceof String) == false) {
            return;
        }
        String text = (String)field;
        text = FeatureUtils.cleanAndLowerText(text);
        text = FeatureUtils.truncateToNumValidBytes(text, MAX_STRING_SIZE_IN_BYTES);
        String finalText = text;
        List<FeatureValue[]> processedFeatures = FEATURE_EXTRACTORS.stream()
            .map((featureExtractor) -> featureExtractor.extractFeatures(finalText))
            .collect(Collectors.toList());
        fields.put(destField, concatEmbeddings(processedFeatures));
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        for(byte[] bytes : embeddingsWeights) {
            size += RamUsageEstimator.sizeOf(bytes);
        }
        for(short[] shorts : embeddingsQuantScales) {
            size += RamUsageEstimator.sizeOf(shorts);
        }
        return size;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(destField);
        out.writeArray(StreamOutput::writeByteArray, embeddingsWeights);
        out.writeArray((output, value) -> {
            output.writeVInt(value.length);
            for(short s : value) {
                output.writeShort(s);
            }
        }, embeddingsQuantScales);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), fieldName);
        builder.field(DEST_FIELD.getPreferredName(), destField);
        builder.field(EMBEDDING_QUANT_SCALES.getPreferredName(), embeddingsQuantScales);
        builder.field(EMBEDDING_WEIGHTS.getPreferredName(), embeddingsWeights);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomWordEmbedding that = (CustomWordEmbedding) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(destField, that.destField)
            && Arrays.deepEquals(embeddingsWeights, that.embeddingsWeights)
            && Arrays.deepEquals(embeddingsQuantScales, that.embeddingsQuantScales);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, destField, Arrays.deepHashCode(embeddingsQuantScales), Arrays.deepHashCode(embeddingsWeights));
    }

}
