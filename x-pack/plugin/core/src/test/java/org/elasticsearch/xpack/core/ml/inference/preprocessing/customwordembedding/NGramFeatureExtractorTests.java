/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.preprocessing.CustomWordEmbedding.MAX_STRING_SIZE_IN_BYTES;
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
     *  - Hungarian ("hu") CLD3 removed language in text processing
     *  - Igbo ("ig") CLD3 removed language in text processing
     *  - Icelandic ("is") CLD3 removed language in text processing
     *  - Maori ("mi") CLD3 removed language in text processing
     *  - Mongolian ("mn") text " журам боловсруулах орон нутгийн " removed
     *  - Marathi ("mr") CLD3 removed language in text processing
     *  - Malay ("ms") CLD3 removed language in text processing
     *  - Nepali ("ne") CLD3 removed language in text processing
     *  - Punjabi ("pa") CLD3 removed language in text processing
     *  - Tajik ("tg") CLD3 removed language in text processing
     *  - Turkish ("tr") CLD3 removed language in text processing
     *  - Zulu ("zu") CLD3 removed language in text processing
     */
    private static final Map<String, String> KNOWN_PROCESSING_FAILURE_LANGUAGES = new HashMap<>();
    static {
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("bn",
            "এনপির ওয়াক আউট তপন চৌধুরী হারবাল অ্যাসোসিয়েশনের সভাপতি " +
                "আন্তর্জাতিক পরামর ালিকপক্ষের কান্না শ্রমিকের অনিশ্চয়তা মতিঝিলে সমাবেশ " +
                "নিষিদ্ধ এফবিসিসিআইয় ের গ্র্যান্ডমাস্টার সিজন ব্রাজিলে বিশ্বকাপ ফুটবল" +
                " আয়োজনবিরোধী বিক্ষোভ দেশ ের দক্ষতা ও যোগ্যতার পাশাপাশি তারা " +
                "জাতীয় ইস্যুগুলোতে প্রাধান্য দিয়েছেন প া যাবে কি একজন দর্শকের " +
                "এমন প্রশ্নে জবাবে আব্দুল্লাহ আল নোমান বলেন এই ্রতিন ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("bs",
            " novi predsjednik mešihata islamske zajednice u srbiji izus i muftija dr mevlud ef dudić izjavio je u intervjuu za anadolu" +
                " agency aa kako je uvjeren da će doći do vraćanja jedinstva među muslimanima i unutar islamske zajednice na " +
                "prostoru sandžaka te da je njegova ruka pružena za povratak svih u okrilje islamske zajednice u srbiji nakon " +
                "skoro sedam godina podjela u tom u srbiji izabran januara a zvanična inauguracija će biti obavljena u prvoj polovini " +
                "februara kako se očekuje prisustvovat će joj i reisu l koji će i zvanično promovirati dudića boraviti u prvoj" +
                " zvaničnoj posjeti reisu kavazoviću što je njegov privi simbolični potez nakon imenovanja ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("fr",
            " a accès aux collections et aux frontaux qui lui ont été attribués il peut consulter et modifier ses collections et" +
                " exporter des configurations de collection toutefois il ne peut pas aux fonctions ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("ha", " a cikin a kan sakamako daga sakwannin a kan kafar ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("hi", " ं ऐडवर्ड्स विज्ञापनों के अनुभव पर आधारित हैं और इनकी मदद से आपको अपने ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("hr",
            " posljednja dva vladara su kijaksar  κυαξαρης  prije krista fraortov sin koji će proširiti teritorij medije i " +
                "astijag kijaksar je imao kćer ili unuku koja se zvala amitis a postala je ženom nabukodonosora ii kojoj je " +
                "ovaj izgradio viseće vrtove babilona kijaksar je modernizirao svoju vojsku i uništio ninivu prije krista naslijedio " +
                "ga je njegov sin posljednji medijski kralj astijag kojega je detronizirao srušio sa vlasti njegov unuk kir veliki" +
                " zemljom su zavladali perzijanci hrvatska je zemlja situacija u europi ona ima bogatu kulturu i ukusna jela ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("ht",
            " ak pitit tout sosyete a chita se pou sa leta dwe pwoteje yo nimewo leta fèt pou li pwoteje tout paran ak pitit nan " +
                "peyi a menm jan kit paran yo marye kit yo pa marye tout manman ki fè piti ak pou ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("hu",
            " a felhasználóim a google azonosító szöveget fogják látni minden tranzakció után ha a vásárlását regisztrációját oldalunk ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("ig", " chineke bụ aha ọzọ ndï omenala igbo kpọro chukwu mgbe ndị bekee bịara ha " +
            "mee ya nke ndi christian n echiche ndi ekpere chi omenala ndi igbo christianity judaism ma islam chineke nwere ọtụtụ " +
            "utu aha ma nwee nanị otu aha ụzọ abụọ e si akpọ aha ahụ bụ jehovah ma ọ bụ yahweh na ọtụtụ akwụkwọ nsọ e wepụla aha" +
            " chineke ma jiri utu aha bụ onyenwe anyị ma ọ bụ chineke dochie ya pụtara n ime ya ihe dị ka ugboro pụkụ asaa ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("is",
            " a afköst leitarorða þinna leitarorð neikvæð leitarorð auglýsingahópa byggja upp og skoða ítarleg gögn um árangur " +
                "leitarorða eins og samkeppni auglýsenda og leitarmagn er krafist notkun ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("mi",
            " haere ki te kainga o o haere ki te kainga o o kainga o ka tangohia he ki to rapunga kaore au mohio te tikanga" +
                " whakatiki o te ra he nga awhina o te ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("mn",
            " а боловсронгуй болгох орон нутгийн ажил үйлсийг уялдуулж зохицуулах дүрэм өмч хөрөнгө санхүүгийн ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("mr",
            " हैदराबाद उच्चार ऐका सहाय्य माहिती तेलुगू  హైదరాబాదు  उर्दू  حیدر آباد  हे भारतातील" +
                " आंध्र प्रदेश राज्याच्या राजधानीचे शहर आहे हैदराबादची लोकसंख्या " +
                "लाख हजार आहे मोत्यांचे शहर अशी एकेकाळी ओळख असलेल्या या शहराला ऐतिहासिक " +
                "सांस्कृतिक आणि स्थापत्यशास्त्रीय वारसा लाभला आहे नंतर त्याचप्रमाणे " +
                "औषधनिर्मिती आणि उद्योगधंद्यांची वाढ शहरात झाली दक्षिण मध्य तेलुगू केंद्र आहे ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("ms",
            " pengampunan beramai ramai supaya mereka pulang ke rumah masing masing orang orang besarnya enggan " +
                "mengiktiraf sultan yang dilantik oleh belanda sebagai yang dipertuan selangor orang ramai pula " +
                "tidak mahu menjalankan perniagaan bijih timah dengan belanda selagi raja yang berhak tidak ditabalkan " +
                "perdagang yang lain dibekukan terus kerana untuk membalas jasa beliau yang membantu belanda menentang " +
                "riau johor dan selangor di antara tiga orang sultan juga dipandang oleh rakyat ganti sultan ibrahim ditabalkan" +
                " raja muhammad iaitu raja muda walaupun baginda bukan anak isteri pertama bergelar sultan muhammad " +
                "ioleh cina di lukut tidak diambil tindakan sedangkan baginda sendiri banyak berhutang kepada ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("ne",
            " अरू ठाऊँबाटपनि खुलेको छ यो खाता अर ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("pa",
            " ਂ ਦਿਨਾਂ ਵਿਚ ਭਾਈ ਸਾਹਿਬ ਦੀ ਬੁੱਚੜ ਗੋਬਿੰਦ ਰਾਮ ਨਾਲ ਅੜਫਸ ਚੱਲ ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("tg",
            " адолат ва инсондӯстиро бар фашизм нажодпарастӣ ва адоват тарҷеҳ додааст чоп кунед ба дигарон фиристед ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("tr",
            " a ayarlarınızı görmeniz ve yönetmeniz içindir eğer kampanyanız için günlük bütçenizi gözden geçirebileceğiniz " +
                "yeri ve kampanya ayarlarını düzenle yi tıklayın sunumu ");
        KNOWN_PROCESSING_FAILURE_LANGUAGES.put("zu",
            " ana engu uma inkinga iqhubeka siza ubike kwi isexwayiso ngenxa yephutha lomlekeleli sikwazi ukubuyisela " +
                "emuva kuphela imiphumela engaqediwe ukuthola imiphumela eqediwe zama ukulayisha siza uthumele ");
    }

    private String processedText(String language, List<LanguageExamples.LanguageExampleEntry> languages) {
        if (KNOWN_PROCESSING_FAILURE_LANGUAGES.containsKey(language)) {
            return KNOWN_PROCESSING_FAILURE_LANGUAGES.get(language);
        }
        String text = null;
        for (LanguageExamples.LanguageExampleEntry goldenLang : languages) {
            if (goldenLang.getLanguage().equals(language)) {
                text = goldenLang.getText();
                break;
            }
        }
        if (text == null) {
            fail("unable to find text for language: " + language);
        }
        text = FeatureUtils.cleanAndLowerText(text);
        return FeatureUtils.truncateToNumValidBytes(text, MAX_STRING_SIZE_IN_BYTES);
    }

    public void testExtractor() throws Exception {
        List<LanguageExamples.LanguageExampleEntry> entries = new LanguageExamples().getLanguageExamples();
        for (NGramFeatureExtractorTests.NGramExampleEntry entry : getGoldenNGrams()) {
            String text = processedText(entry.language, entries);

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

    /**
     * These nGram examples were created through running Google's CLD3 network
     */
    public List<NGramFeatureExtractorTests.NGramExampleEntry> getGoldenNGrams() throws Exception {
        String path = "/org/elasticsearch/xpack/core/ml/inference/ngram_examples.json";
        try(XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                Files.newInputStream(getDataPath(path))) ) {
            List<NGramFeatureExtractorTests.NGramExampleEntry> entries = new ArrayList<>();
            while(parser.nextToken() != XContentParser.Token.END_ARRAY) {
                entries.add(NGramFeatureExtractorTests.NGramExampleEntry.PARSER.apply(parser, null));
            }
            return entries;
        }
    }

    public static class NGramExampleEntry implements ToXContentObject {

        private static final ParseField LANGUAGE = new ParseField("language");
        private static final ParseField NGRAMS = new ParseField("ngrams");
        private static final ParseField WEIGHTS = new ParseField("weights");
        private static final ParseField IDS = new ParseField("ids");
        private static final ParseField DIMENSION = new ParseField("dimension");
        private static final ParseField NGRAM_SIZE = new ParseField("ngram_size");

        public static ObjectParser<NGramExampleEntry, Void> PARSER = new ObjectParser<>(
            "ngram_example_entry",
            true,
            NGramExampleEntry::new);

        static {
            PARSER.declareString(NGramExampleEntry::setLanguage, LANGUAGE);
            PARSER.declareStringArray(NGramExampleEntry::setnGrams, NGRAMS);
            PARSER.declareDoubleArray(NGramExampleEntry::setWeights, WEIGHTS);
            PARSER.declareIntArray(NGramExampleEntry::setIds, IDS);
            PARSER.declareInt(NGramExampleEntry::setDimension, DIMENSION);
            PARSER.declareInt(NGramExampleEntry::setnGramSize, NGRAM_SIZE);
        }
        String language;
        String[] nGrams;
        double[] weights;
        int[] ids;
        int dimension;
        int nGramSize;

        private NGramExampleEntry() {

        }

        public void setLanguage(String language) {
            this.language = language;
        }

        public void setnGrams(String[] nGrams) {
            this.nGrams = nGrams;
        }

        public void setWeights(double[] weights) {
            this.weights = weights;
        }

        public void setWeights(List<Double> weights) {
            setWeights(weights.stream().mapToDouble(Double::doubleValue).toArray());
        }

        public void setIds(int[] ids) {
            this.ids = ids;
        }

        public void setIds(List<Integer> ids) {
            setIds(ids.stream().mapToInt(Integer::intValue).toArray());
        }

        public void setDimension(int dimension) {
            this.dimension = dimension;
        }

        public void setnGramSize(int nGramSize) {
            this.nGramSize = nGramSize;
        }

        public void setnGrams(List<String> nGrams) {
            setnGrams(nGrams.toArray(new String[0]));
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LANGUAGE.getPreferredName(), language);
            builder.field(WEIGHTS.getPreferredName(), weights);
            builder.field(IDS.getPreferredName(), ids);
            builder.field(NGRAMS.getPreferredName(), nGrams);
            builder.field(NGRAM_SIZE.getPreferredName(), nGramSize);
            builder.field(DIMENSION.getPreferredName(), dimension);
            builder.endObject();
            return builder;
        }
    }

}
