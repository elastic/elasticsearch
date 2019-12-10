package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.preprocessing.CLD3WordEmbedding.MAX_STRING_SIZE_IN_BYTES;
import static org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ContinuousNGramExtractionExamples.ALL_GOLDEN_NGRAMS;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class NGramFeatureExtractorTests extends ESTestCase {

    public void testExtractor() throws Exception {
        for (ContinuousNGramExtractionExamples.NGramExampleEntry entry : ALL_GOLDEN_NGRAMS) {
            String text = null;
            for (String[] goldenLang : LanguageExamples.goldLangText) {
                if (goldenLang[0].equals(entry.language)) {
                    text = goldenLang[1];
                }
            }
            if (text == null) {
                fail("unable to find text for language: " + entry.language);
            }
            Integer[] sortedIndices = IntStream.range(0, entry.nGrams.length)
                .boxed()
                .sorted(Comparator.comparing((i) -> entry.nGrams[i]))
                .toArray(Integer[]::new);
            double[] orderedWeights = new double[entry.weights.length];
            int[] orderedIds = new int[entry.ids.length];
            for(int i = 0; i < entry.nGrams.length; i++) {
                orderedIds[i] = entry.ids[sortedIndices[i]];
                orderedWeights[i] = entry.weights[sortedIndices[i]];
            }
            text = FeatureUtils.truncateToNumValidBytes(text, MAX_STRING_SIZE_IN_BYTES);
            text = FeatureUtils.cleanAndLowerText(text);
            FeatureValue[] features = new NGramFeatureExtractor(entry.nGramSize, entry.dimension).extractFeatures(text);

            int[] extractedIds = Arrays.stream(features).map(FeatureValue::getRow).mapToInt(Integer::valueOf).toArray();
            double[] extractedWeights = Arrays.stream(features).map(FeatureValue::getWeight).mapToDouble(Double::valueOf).toArray();

            String msg = "for language [" + entry.language + "] dimension [" + entry.dimension + "] ngrams [" + entry.nGrams + "]";
            assertThat("weights length mismatch " + msg, extractedWeights.length, equalTo(entry.weights.length));
            assertThat("ids length mismatch " + msg, extractedIds.length, equalTo(entry.ids.length));
            for(int i = 0; i < extractedIds.length; i++) {
                String assertMessage = "ids mismatch for id [" + i + "] and " + msg;
                assertThat(assertMessage, extractedIds[i], equalTo(orderedIds[i]));
            }

            double eps = 0.00001;
            for(int i = 0; i < extractedWeights.length; i++) {
                String assertMessage = " Difference [" + Math.abs(extractedWeights[i] - entry.weights[i]) + "]" +
                    "weights mismatch for weight [" + i + "] and " + msg;
                assertThat(
                    assertMessage,
                    extractedWeights[i],
                    closeTo(orderedWeights[i], eps));
            }

        }
    }

}
