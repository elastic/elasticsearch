/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ContinuousFeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.FeatureExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.FeatureUtils;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.FeatureValue;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.NGramFeatureExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.RelevantScriptFeatureExtractor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ScriptDetector;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ScriptFeatureExtractor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This embedding class creates and embeds all the features layed out here
 *
 */
public class CLD3WordEmbedding implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CLD3WordEmbedding.class);
    public static final int MAX_STRING_SIZE_IN_BYTES = 10000;
    public static final ParseField NAME = new ParseField("cld3_word_embedding");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField DEST_FIELD = new ParseField("dest_field");
    public static final ParseField EMBEDDING_WEIGHTS = new ParseField("embedding_weights");
    public static final ParseField EMBEDDING_QUANT_SCALES = new ParseField("embedding_quant_scales");

    public static final ConstructingObjectParser<CLD3WordEmbedding, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<CLD3WordEmbedding, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<CLD3WordEmbedding, Void> createParser(boolean lenient) {
        ConstructingObjectParser<CLD3WordEmbedding, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new CLD3WordEmbedding((List<short[]>)a[0], (List<int[]>)a[1], (String)a[2], (String)a[3]));

        parser.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                if (p.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException(
                        "unexpected token [" + p.currentToken() + "] for [" + EMBEDDING_QUANT_SCALES.getPreferredName() + "]");
                }
                List<short[]> quantScales = new ArrayList<>();
                while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (p.currentToken() != XContentParser.Token.START_ARRAY) {
                        throw new IllegalArgumentException(
                            "unexpected token [" + p.currentToken() + "] for [" + EMBEDDING_QUANT_SCALES.getPreferredName() + "]");
                    }
                    List<Short> shortList = new ArrayList<>();
                    while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                        if(p.currentToken().isValue() == false) {
                            throw new IllegalStateException("expected non-null value but got [" + p.currentToken() + "] " +
                                "for [" + EMBEDDING_QUANT_SCALES.getPreferredName() + "]");
                        }
                        shortList.add(p.shortValue());
                    }
                    short[] shorts = new short[shortList.size()];
                    for (int i = 0; i < shortList.size(); i++) {
                        shorts[i] = shortList.get(i);
                    }
                    quantScales.add(shorts);
                }
                return quantScales;
            },
            EMBEDDING_QUANT_SCALES,
            ObjectParser.ValueType.VALUE_ARRAY);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> {
                if (p.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException(
                        "unexpected token [" + p.currentToken() + "] for [" + EMBEDDING_WEIGHTS.getPreferredName() + "]");
                }
                List<int[]> weights = new ArrayList<>();
                while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (p.currentToken() != XContentParser.Token.START_ARRAY) {
                        throw new IllegalArgumentException(
                            "unexpected token [" + p.currentToken() + "] for [" + EMBEDDING_WEIGHTS.getPreferredName() + "]");
                    }
                    List<Integer> integerList = new ArrayList<>();
                    while(p.nextToken() != XContentParser.Token.END_ARRAY) {
                        if(p.currentToken().isValue() == false) {
                            throw new IllegalStateException("expected non-null value but got [" + p.currentToken() + "] " +
                                "for [" + EMBEDDING_WEIGHTS.getPreferredName() + "]");
                        }
                        integerList.add(p.intValue());
                    }
                    int[] ints = new int[integerList.size()];
                    for (int i = 0; i < integerList.size(); i++) {
                        ints[i] = integerList.get(i);
                    }
                    weights.add(ints);
                }
                return weights;
            },
            EMBEDDING_WEIGHTS,
            ObjectParser.ValueType.VALUE_ARRAY);
        parser.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), DEST_FIELD);
        return parser;
    }

    public static CLD3WordEmbedding fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static CLD3WordEmbedding fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private static final int concatLayerSize = 80;
    private static final int[] embeddingDim = new int[]{16, 16, 8, 8, 16, 16};
    private static final int[] concatOffset = new int[]{0, 16, 32, 40, 48, 64};
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
    private final int[][] embeddingsWeights;
    private final String fieldName;
    private final String destField;

    private static short[][] toPrimitiveShorts(List<short[]> shorts) {
        short[][] primShorts = new short[shorts.size()][];
        int i = 0;
        for (short[] s : shorts) {
            primShorts[i++] = s;
        }
        return primShorts;
    }

    private static int[][] toPrimitiveInts(List<int[]> listOfInts) {
        int[][] primInts = new int[listOfInts.size()][];
        int i = 0;
        for (int[] ints : listOfInts) {
            primInts[i++] = ints;
        }
        return primInts;
    }

    public CLD3WordEmbedding(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.destField = in.readString();
        this.embeddingsWeights = in.readArray(StreamInput::readVIntArray, (length) -> new int[length][]);
        this.embeddingsQuantScales = in.readArray((input -> {
            int length = input.readVInt();
            short[] shorts = new short[length];
            for (int i = 0; i < length; i++) {
                shorts[i] = in.readShort();
            }
            return shorts;
        }), (length) -> new short[length][]);
    }

    private CLD3WordEmbedding(List<short[]> quantLists, List<int[]> weigthLists, String fieldName, String destField) {
        this(toPrimitiveShorts(quantLists), toPrimitiveInts(weigthLists), fieldName, destField);
    }

    public CLD3WordEmbedding(short[][] embeddingsQuantScales, int[][] embeddingsWeights, String fieldName, String destField) {
        this.embeddingsQuantScales = embeddingsQuantScales;
        this.embeddingsWeights = embeddingsWeights;
        this.fieldName = fieldName;
        this.destField = destField;
    }

    /**
     * Derived from: https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/embedding_network.cc#L74
     */
    private double[] concatEmbeddings(List<FeatureValue[]> featureVectors) {
        double[] concat = new double[concatLayerSize];

        // "esIndex" stands for "embedding space index".
        for (int esIndex = 0; esIndex < featureVectors.size(); ++esIndex) {
            int[] embeddingWeight = this.embeddingsWeights[esIndex];
            short[] quants = this.embeddingsQuantScales[esIndex];
            int embeddingDim = CLD3WordEmbedding.embeddingDim[esIndex];

            FeatureValue[] featureVector = featureVectors.get(esIndex);
            int featureOffset = concatOffset[esIndex];
            assert (featureOffset < concat.length);
            for (FeatureValue featureValue : featureVector) {
                // TODO - base + embedding_dim ignored as base==0

                // Multiplier for each embedding weight.
                int row = featureValue.getRow();
                double multiplier = featureValue.getWeight() * shortToFloat(quants[row]);

                // Iterate across columns for this row
                for (int i = 0; i < embeddingDim; ++i) {
                    // 128 is bias for UINT8 quantization, only one we currently support.
                    double value = (getRowMajorData(embeddingWeight, embeddingDim, row, i) - 128) * multiplier;
                    int concatIndex = featureOffset + i;
                    concat[concatIndex] += value;
                }
            }
        }

        return concat;
    }

    private static float shortToFloat(short s) {
        // We fill in the new mantissa bits with 0, and don't do anything smarter.
        // TODO java internals may be different
        int i = (s << 16);
        return Float.intBitsToFloat(i);
    }

    private static int getRowMajorData(int[] data, int colDim, int row, int col) {
        return data[row * colDim + col];
    }

    /**
     * Derived from: https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/language_identifier_features.cc#L56
     */
    public static FeatureValue[] getNGramFeatureValue(String text, int nGramSize, int idDimension) throws Exception {

        // First add terminators:
        // Split the text based on spaces to get tokens, adds "^"
        // to the beginning of each token, and adds "$" to the end of each token.
        // e.g.
        // " this text is written in english" goes to
        // "^$ ^this$ ^text$ ^is$ ^written$ ^in$ ^english$ ^$"
        StringBuilder newText = new StringBuilder("^");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == ' ') {
                newText.append("$ ^");
            } else {
                newText.append(c);
            }
        }
        newText.append("$");

        // Find the char ngrams
        // ^$ ^this$ ^text$ ^is$ ^written$ ^in$ ^english$ ^$"
        // nGramSize = 2
        // [{h$},{sh},{li},{gl},{in},{en},{^$},...]
        Map<String, Integer> charNGrams = new TreeMap<>();

        //TODO use lucene tokenizer ?
        int countSum = 0;
        for (int start = 0; start <= (newText.toString().length()) - nGramSize; ++start) {
            StringBuilder charNGram = new StringBuilder();

            int index;
            for (index = 0; index < nGramSize; ++index) {
                char currentChar = newText.toString().charAt(start + index);
                if (currentChar == ' ') {
                    break;
                }
                charNGram.append(currentChar);
            }

            if (index == nGramSize) {
                charNGrams.put(charNGram.toString(),
                    charNGrams.getOrDefault(charNGram.toString(), 0) + 1);
                ++countSum;
            }
        }

        FeatureValue[] results = new FeatureValue[charNGrams.size()];
        int index = 0;
        for (Map.Entry<String, Integer> entry : charNGrams.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();

            double weight = (double) value / (double) countSum;
            int id = Integer.remainderUnsigned(FeatureUtils.Hash32WithDefaultSeed(key), idDimension);

            results[index++] = new ContinuousFeatureValue(id, weight);
        }
        return results;
    }

    /**
     * Derived from: https://github.com/google/cld3/blob/master/src/relevant_script_feature.cc
     */
    static FeatureValue[] getRelevantScriptFeature(String text) throws UnsupportedEncodingException {
        if (text.isEmpty()) {
            return new FeatureValue[0];
        }

        // counts[s] is the number of characters with script s.
        // Use treemap so results are sorted in scriptid order
        TreeMap<ScriptDetector.Script, Integer> counts = new TreeMap<>();

        int totalCount = 0;

        for (int i = 1; i <= text.length(); ++i) {
            String curr = text.substring(i - 1, i);
            byte[] bytes = text.substring(i - 1, i).getBytes("UTF8");
            int numBytes = bytes.length;

            // Skip spaces, numbers, punctuation, and all other non-alpha ASCII
            // characters: these characters are used in so many languages, they do not
            // communicate language-related information.
            // TODO - check whether we need to look at mark
            if ((numBytes == 1) && !curr.chars().allMatch(Character::isLetter)) {
                continue;
            }

            ScriptDetector.Script script = ScriptDetector.getScript(curr);
            counts.put(script, counts.getOrDefault(script, 0) + 1);

            totalCount++;
        }

        FeatureValue[] result = new FeatureValue[counts.size()];
        int index = 0;

        for (Map.Entry<ScriptDetector.Script, Integer> entry : counts.entrySet()) {
            ScriptDetector.Script scriptId = entry.getKey();
            int count = entry.getValue();
            if (count > 0) {
                double weight = (double) count / (double) totalCount;
                result[index++] = new ContinuousFeatureValue(scriptId.toInt(), weight);
            }
        }

        return result;
    }

    @Override
    public void process(Map<String, Object> fields) {
        Object field = fields.get(fieldName);
        if ((field instanceof String) == false) {
            return;
        }
        String text = (String)field;

        List<FeatureValue[]> processedFeatures = new ArrayList<>(6);
        try {
            //These two preprocessing steps are to satisfy the cleaning done here in CLD3
            // https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/nnet_language_identifier.cc#L190..L226
            text = FeatureUtils.truncateToNumValidBytes(text, MAX_STRING_SIZE_IN_BYTES);
            text = FeatureUtils.cleanAndLowerText(text);
            String finalText = text;
            processedFeatures = FEATURE_EXTRACTORS.stream()
                .map((featureExtractor) -> featureExtractor.extractFeatures(finalText))
                .collect(Collectors.toList());
            fields.put(destField, concatEmbeddings(processedFeatures));
        } catch (Exception e) {
            throw new ElasticsearchException("Failure embedding the text in pre-processor", e);
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        for(int[] ints : embeddingsWeights) {
            size += RamUsageEstimator.sizeOf(ints);
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
        out.writeArray(StreamOutput::writeVIntArray, embeddingsWeights);
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
        CLD3WordEmbedding that = (CLD3WordEmbedding) o;
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
