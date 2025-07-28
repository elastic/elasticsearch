/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.CustomWordEmbedding;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceModel;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.divMut;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.sumDoubleArrays;
import static org.elasticsearch.xpack.core.ml.inference.utils.Statistics.softMax;

public class LangIdentNeuralNetwork implements StrictlyParsedTrainedModel, LenientlyParsedTrainedModel, InferenceModel {

    public static final ParseField NAME = new ParseField("lang_ident_neural_network");
    public static final ParseField EMBEDDED_VECTOR_FEATURE_NAME = new ParseField("embedded_vector_feature_name");
    public static final ParseField HIDDEN_LAYER = new ParseField("hidden_layer");
    public static final ParseField SOFTMAX_LAYER = new ParseField("softmax_layer");
    public static final ConstructingObjectParser<LangIdentNeuralNetwork, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<LangIdentNeuralNetwork, Void> LENIENT_PARSER = createParser(true);

    private static final List<String> LANGUAGE_NAMES = Arrays.asList(
        "eo",
        "co",
        "eu",
        "ta",
        "de",
        "mt",
        "ps",
        "te",
        "su",
        "uz",
        "zh-Latn",
        "ne",
        "nl",
        "sw",
        "sq",
        "hmn",
        "ja",
        "no",
        "mn",
        "so",
        "ko",
        "kk",
        "sl",
        "ig",
        "mr",
        "th",
        "zu",
        "ml",
        "hr",
        "bs",
        "lo",
        "sd",
        "cy",
        "hy",
        "uk",
        "pt",
        "lv",
        "iw",
        "cs",
        "vi",
        "jv",
        "be",
        "km",
        "mk",
        "tr",
        "fy",
        "am",
        "zh",
        "da",
        "sv",
        "fi",
        "ht",
        "af",
        "la",
        "id",
        "fil",
        "sm",
        "ca",
        "el",
        "ka",
        "sr",
        "it",
        "sk",
        "ru",
        "ru-Latn",
        "bg",
        "ny",
        "fa",
        "haw",
        "gl",
        "et",
        "ms",
        "gd",
        "bg-Latn",
        "ha",
        "is",
        "ur",
        "mi",
        "hi",
        "bn",
        "hi-Latn",
        "fr",
        "yi",
        "hu",
        "xh",
        "my",
        "tg",
        "ro",
        "ar",
        "lb",
        "el-Latn",
        "st",
        "ceb",
        "kn",
        "az",
        "si",
        "ky",
        "mg",
        "en",
        "gu",
        "es",
        "pl",
        "ja-Latn",
        "ga",
        "lt",
        "sn",
        "yo",
        "pa",
        "ku"
    );
    // The final value for our provided language names indicates if there is no valid text in the string to classify
    // This value should map back to "zxx", which is the code for "no linguistic content" in the BCP 47
    // system - see https://tools.ietf.org/search/bcp47
    private static final int MISSING_VALID_TXT_CLASSIFICATION = LANGUAGE_NAMES.size() - 1;
    private static final String MISSING_VALID_TXT_CLASSIFICATION_STR = "zxx";

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LangIdentNeuralNetwork.class);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<LangIdentNeuralNetwork, Void> createParser(boolean lenient) {
        ConstructingObjectParser<LangIdentNeuralNetwork, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new LangIdentNeuralNetwork((String) a[0], (LangNetLayer) a[1], (LangNetLayer) a[2])
        );
        parser.declareString(constructorArg(), EMBEDDED_VECTOR_FEATURE_NAME);
        parser.declareObject(
            constructorArg(),
            (p, c) -> lenient ? LangNetLayer.LENIENT_PARSER.apply(p, c) : LangNetLayer.STRICT_PARSER.apply(p, c),
            HIDDEN_LAYER
        );
        parser.declareObject(
            constructorArg(),
            (p, c) -> lenient ? LangNetLayer.LENIENT_PARSER.apply(p, c) : LangNetLayer.STRICT_PARSER.apply(p, c),
            SOFTMAX_LAYER
        );
        return parser;
    }

    public static LangIdentNeuralNetwork fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static LangIdentNeuralNetwork fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final LangNetLayer hiddenLayer;
    private final LangNetLayer softmaxLayer;
    private final String embeddedVectorFeatureName;

    public LangIdentNeuralNetwork(String embeddedVectorFeatureName, LangNetLayer hiddenLayer, LangNetLayer softmaxLayer) {
        this.embeddedVectorFeatureName = ExceptionsHelper.requireNonNull(embeddedVectorFeatureName, EMBEDDED_VECTOR_FEATURE_NAME);
        this.hiddenLayer = ExceptionsHelper.requireNonNull(hiddenLayer, HIDDEN_LAYER);
        this.softmaxLayer = ExceptionsHelper.requireNonNull(softmaxLayer, SOFTMAX_LAYER);
    }

    public LangIdentNeuralNetwork(StreamInput in) throws IOException {
        this.embeddedVectorFeatureName = in.readString();
        this.hiddenLayer = new LangNetLayer(in);
        this.softmaxLayer = new LangNetLayer(in);
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config, Map<String, String> featureDecoderMap) {
        if (config.requestingImportance()) {
            throw ExceptionsHelper.badRequestException("[{}] model does not supports feature importance", NAME.getPreferredName());
        }
        if (config instanceof ClassificationConfig == false) {
            throw ExceptionsHelper.badRequestException("[{}] model only supports classification", NAME.getPreferredName());
        }
        Object vector = fields.get(embeddedVectorFeatureName);
        if (vector instanceof List<?> == false) {
            throw ExceptionsHelper.badRequestException(
                "[{}] model could not find non-null collection of embeddings separated by unicode script type [{}]. "
                    + "Please verify that the input is a string.",
                NAME.getPreferredName(),
                embeddedVectorFeatureName
            );
        }
        List<?> embeddedVector = (List<?>) vector;
        final ClassificationConfig classificationConfig = (ClassificationConfig) config;
        if (embeddedVector.isEmpty()) {
            return new ClassificationInferenceResults(
                MISSING_VALID_TXT_CLASSIFICATION,
                MISSING_VALID_TXT_CLASSIFICATION_STR,
                Collections.emptyList(),
                Collections.emptyList(),
                classificationConfig,
                1.0,
                1.0
            );
        }
        double[] probabilities = new double[LANGUAGE_NAMES.size()];
        int totalLen = 0;
        for (Object vec : embeddedVector) {
            if (vec instanceof CustomWordEmbedding.StringLengthAndEmbedding == false) {
                continue;
            }
            CustomWordEmbedding.StringLengthAndEmbedding stringLengthAndEmbedding = (CustomWordEmbedding.StringLengthAndEmbedding) vec;
            int square = stringLengthAndEmbedding.getUtf8StringLen() * stringLengthAndEmbedding.getUtf8StringLen();
            totalLen += square;
            double[] h0 = hiddenLayer.productPlusBias(false, stringLengthAndEmbedding.getEmbedding());
            double[] score = softmaxLayer.productPlusBias(true, h0);
            sumDoubleArrays(probabilities, softMax(score), Math.max(square, 1));
        }
        if (totalLen != 0) {
            divMut(probabilities, totalLen);
        }
        Tuple<InferenceHelpers.TopClassificationValue, List<TopClassEntry>> topClasses = InferenceHelpers.topClasses(
            probabilities,
            LANGUAGE_NAMES,
            null,
            classificationConfig.getNumTopClasses(),
            PredictionFieldType.STRING
        );
        final InferenceHelpers.TopClassificationValue classificationValue = topClasses.v1();
        assert classificationValue.getValue() >= 0 && classificationValue.getValue() < LANGUAGE_NAMES.size()
            : "Invalid language predicted. Predicted language index " + topClasses.v1();
        return new ClassificationInferenceResults(
            classificationValue.getValue(),
            LANGUAGE_NAMES.get(classificationValue.getValue()),
            topClasses.v2(),
            Collections.emptyList(),
            classificationConfig,
            classificationValue.getProbability(),
            classificationValue.getScore()
        );
    }

    @Override
    public InferenceResults infer(double[] embeddedVector, InferenceConfig config) {
        throw new UnsupportedOperationException("[lang_ident] does not support nested inference");
    }

    @Override
    public void rewriteFeatureIndices(Map<String, Integer> newFeatureIndexMapping) {
        if (newFeatureIndexMapping != null && newFeatureIndexMapping.isEmpty() == false) {
            throw new UnsupportedOperationException("[lang_ident] does not support nested inference");
        }
    }

    @Override
    public String[] getFeatureNames() {
        return new String[] { embeddedVectorFeatureName };
    }

    @Override
    public TargetType targetType() {
        return TargetType.CLASSIFICATION;
    }

    @Override
    public void validate() {}

    @Override
    public long estimatedNumOperations() {
        long numOps = hiddenLayer.getBias().length; // adding bias
        numOps += hiddenLayer.getWeights().length; // multiplying weights
        numOps += softmaxLayer.getBias().length; // adding bias
        numOps += softmaxLayer.getWeights().length; // multiplying softmax weights
        return numOps;
    }

    @Override
    public boolean supportsFeatureImportance() {
        return false;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(hiddenLayer);
        size += RamUsageEstimator.sizeOf(softmaxLayer);
        return size;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(embeddedVectorFeatureName);
        this.hiddenLayer.writeTo(out);
        this.softmaxLayer.writeTo(out);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(EMBEDDED_VECTOR_FEATURE_NAME.getPreferredName(), embeddedVectorFeatureName);
        builder.field(HIDDEN_LAYER.getPreferredName(), hiddenLayer);
        builder.field(SOFTMAX_LAYER.getPreferredName(), softmaxLayer);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LangIdentNeuralNetwork that = (LangIdentNeuralNetwork) o;
        return Objects.equals(embeddedVectorFeatureName, that.embeddedVectorFeatureName)
            && Objects.equals(hiddenLayer, that.hiddenLayer)
            && Objects.equals(softmaxLayer, that.softmaxLayer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddedVectorFeatureName, hiddenLayer, softmaxLayer);
    }

}
