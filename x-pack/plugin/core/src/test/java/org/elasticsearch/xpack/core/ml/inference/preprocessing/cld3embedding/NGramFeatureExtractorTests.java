package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.util.Arrays;

import static org.elasticsearch.xpack.core.ml.inference.preprocessing.CLD3WordEmbedding.MAX_STRING_SIZE_IN_BYTES;
import static org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding.ContinuousNGramExtractionExamples.GOLDEN_NGRAMS;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class NGramFeatureExtractorTests extends ESTestCase {

    public void testExtractor() throws Exception {
        for (ContinuousNGramExtractionExamples.NGramExampleEntry entry : GOLDEN_NGRAMS) {
            String text = null;
            for (String[] goldenLang : LanguageExamples.goldLangText) {
                if (goldenLang[0].equals(entry.language)) {
                    text = goldenLang[1];
                }
            }
            if (text == null) {
                fail("unable to find text for language: " + entry.language);
            }
            text = FeatureUtils.truncateToNumValidBytes(text, MAX_STRING_SIZE_IN_BYTES);
            text = FeatureUtils.cleanAndLowerText(text);
            FeatureValue[] features = new NGramFeatureExtractor(entry.nGrams, entry.dimension).extractFeatures(text);
            //  dit is n kort stukkie van die teks wat gebruik sal word vir die toets van die akkuraatheid van die nuwe benadering
            // processedChars: { ^, $,  , ^, d, i, t, $,  , ^, i, s, $,  , ^, n, $,  ,
            // ^, k, o, r, t, $,  , ^, s, t, u, k, k, i, e, $,  , ^, v, a, n, $,  , ^,
            // d, i, e, $,  , ^, t, e, k, s, $,  , ^, w, a, t, $,  , ^, g, e, b, r, u, i, k, $,
            // , ^, s, a, l, $,  , ^, w, o, r, d, $,  , ^, v, i, r, $,  , ^, d, i, e, $,  , ^, t, o, e, t, s, $,
            // , ^, v, a, n, $,  , ^, d, i, e, $,  , ^, a, k, k, u, r, a, a, t, h, e, i, d, $,  , ^, v, a, n, $,
            // , ^, d, i, e, $,  , ^, n, u, w, e, $,  , ^, b, e, n, a, d, e, r, i, n, g, $,  , ^, $,  }

            int[] extractedIds = Arrays.stream(features).map(FeatureValue::getRow).mapToInt(Integer::valueOf).toArray();
            double[] extractedWeights = Arrays.stream(features).map(FeatureValue::getWeight).mapToDouble(Double::valueOf).toArray();

            String msg = "for language [" + entry.language + "] dimension [" + entry.dimension + "] ngrams [" + entry.nGrams + "]";
            assertThat("weights length mismatch " + msg, extractedWeights.length, equalTo(entry.weights.length));
            assertThat("ids length mismatch " + msg, extractedIds.length, equalTo(entry.ids.length));
            for(int i = 0; i < extractedIds.length; i++) {
                String assertMessage = "ids mismatch for id [" + i + "] and " + msg;
                assertThat(assertMessage, extractedIds[i], equalTo(entry.ids[i]));
            }

            double eps = 0.00001;
            for(int i = 0; i < extractedWeights.length; i++) {
                String assertMessage = " Difference [" + Math.abs(extractedWeights[i] - entry.weights[i]) + "]" +
                    "weights mismatch for weight [" + i + "] and " + msg;
                assertThat(
                    assertMessage,
                    extractedWeights[i],
                    closeTo(entry.weights[i], eps));
            }

        }
    }

}
