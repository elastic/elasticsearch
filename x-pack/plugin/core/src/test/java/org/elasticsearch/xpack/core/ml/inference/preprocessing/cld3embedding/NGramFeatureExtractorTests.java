package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.preprocessing.CLD3WordEmbedding.MAX_STRING_SIZE_IN_BYTES;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class NGramFeatureExtractorTests extends ESTestCase {
    /**
     * Known failing languages (due to string pre-processing) and why
     *
     * This does not mean that these languages are not accurately chosen when necessary, just that the golden result text did extra
     *  processing for efficiency reasons that are not currently done.
     *
     *  - Bengali ("bn") does not get mutated by String#toLowerCase skipping it in tests for now
     *  - Bosnian ("bs") sections of the text removed
     *  - Croatian ("hr") inexplicable extra white space surrounding " κυαξαρης " in processed text
     *  - French ("fr") "créer ni supprimer des collections enfin il a accès a" removed
     *  - Hausa ("ha") removes many words final process text looks like " a cikin a kan sakamako daga sakwannin a kan kafar "
     *  - Hindi ("hi") removes " विज्ञापनों का अधिकतम लाभ " from the end of the text
     *  - Haitian Creole ("ht") The text " pitit leta fèt pou ba yo konkoul menm jan tou pou timoun " is removed
     *  - Hungarian ("hu") more text removal
     *  - Igbo ("ig") text removed
     *  - Icelandic ("is") text removed
     *  - Maori ("mi") text removed
     *  - Mongolian ("mn") text " журам боловсруулах орон нутгийн " removed
     *  - Marathi ("mr") text removed
     *  - Malay ("ms") text removed
     *  - Nepali ("ne") text removed
     *  - Punjabi ("pa") text removed
     *  - Tajik ("tg") text removed
     *  - Turkish ("tr") text removed
     *  - Zulu ("zu") text removed
     */
    private static final Set<String> KNOWN_FAILING_LANGUAGES = new HashSet<>(
        Arrays.asList("bn", "bs", "fr", "ha", "hi", "hr", "ht", "hu", "ig", "is", "mi", "mn", "mr", "ms", "ne", "pa", "tg", "tr", "zu")
    );

    public void testExtractor() {
        for (ContinuousNGramExtractionExamples.NGramExampleEntry entry : ContinuousNGramExtractionExamples.goldenNGrams) {
            String text = null;
            if (KNOWN_FAILING_LANGUAGES.contains(entry.language)) {
                continue;
            }
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
            text = FeatureUtils.cleanAndLowerText(text);
            text = FeatureUtils.truncateToNumValidBytes(text, MAX_STRING_SIZE_IN_BYTES);
            FeatureValue[] features = new NGramFeatureExtractor(entry.nGramSize, entry.dimension).extractFeatures(text);

            int[] extractedIds = Arrays.stream(features).map(FeatureValue::getRow).mapToInt(Integer::valueOf).toArray();
            double[] extractedWeights = Arrays.stream(features).map(FeatureValue::getWeight).mapToDouble(Double::valueOf).toArray();

            String msg = "for language [" + entry.language + "] dimension [" + entry.dimension + "] ngrams [" + entry.nGramSize + "]";
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
