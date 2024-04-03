/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class WordBoundaryChunkerTests extends ESTestCase {

    private final String TEST_TEXT = "Word segmentation is the problem of dividing a string of written language into its component words.\n"
        + "In English and many other languages using some form of the Latin alphabet, the space is a good approximation of a word divider (word delimiter), although this concept has limits because of the variability with which languages emically regard collocations and compounds. Many English compound nouns are variably written (for example, ice box = ice-box = icebox; pig sty = pig-sty = pigsty) with a corresponding variation in whether speakers think of them as noun phrases or single nouns; there are trends in how norms are set, such as that open compounds often tend eventually to solidify by widespread convention, but variation remains systemic. In contrast, German compound nouns show less orthographic variation, with solidification being a stronger norm.";

    private final String[] MULTI_LINGUAL = new String[] {
        "Građevne strukture Mesa Verde dokaz su akumuliranog znanja i vještina koje su se stoljećima prenosile generacijama civilizacije Anasazi. Vrhunce svojih dosega ostvarili su u 12. i 13. stoljeću, kada su sagrađene danas najpoznatije građevine na liticama. Zidali su obrađenim pješčenjakom, tvrđim kamenom oblikovanim do veličine štruce kruha. Kao žbuku između ciglā stavljali su glinu razmočenu vodom. Tim su materijalom gradili prostorije veličine do 6 četvornih metara. U potkrovljima su skladištili žitarice i druge plodine, dok su kive - ceremonijalne prostorije - gradili ispred soba, ali ukopane u zemlju, nešto poput današnjih podruma. Kiva je bila vrhunski dizajnirana prostorija okruglog oblika s prostorom za vatru zimi te s dovodom hladnog zraka za klimatizaciju ljeti. U zidane konstrukcije stavljali su i lokalno posječena stabla, što današnjim arheolozima pomaže u preciznom datiranju nastanka pojedine građevine metodom dendrokronologije. Ta stabla pridonose i teoriji o mogućem konačnom slomu ondašnjeg društva. Nakon što su, tijekom nekoliko stoljeća, šume do kraja srušene, a njihova obnova zbog sušne klime traje i po 200 godina, nije proteklo puno vremena do konačnog urušavanja civilizacije, koja se, na svojem vrhuncu osjećala nepobjedivom. 90 % sagrađenih naseobina ispod stijena ima do deset prostorija. ⅓ od ukupnog broja sagrađenih kuća ima jednu ili dvije kamene prostorije",
        "Histoarysk wie in acre in stik lân dat 40 roeden (oftewol 1 furlong of ⅛ myl of 660 foet) lang wie, en 4 roeden (of 66 foet) breed. Men is fan tinken dat dat likernôch de grûnmjitte wie dy't men mei in jok oksen yn ien dei beploegje koe.",
        "創業当初の「太平洋化学工業社」から1959年太平洋化学工業株式会社へ、1987年には太平洋化学㈱に社名を変更。 1990年以降、海外拠点を増やし本格的な国際進出を始動。 創業者がつくりあげた化粧品会社を世界企業へと成長させるべく2002年3月英文社名AMOREPACIFICに改めた。",
        "امام محمد بن جرير رح جن جي ولادت باسعادت 224 هجري طبرستان جي شهر آمل ۾ ٿي ، هي اهو دور هو جڏهن سلطنت عباسيه جو عروج هو ۽ سندس سڄي جمار عهد خلافت عباسيه ۾ گذري ، طبرستان هن وقت پڻ سياست ۽ مذهبي حلقن جنهن ۾ معتزلي ، خوارج ، باطني جو گهوارو هو ۽ ابن جرير جي ٻيهر طبرستان ورڻ وقت روافض جو عروج ٿي ويو هو ابن جرير رح جو نالو ، محمد بن جرير بن يزيد بن ڪثير بن غالب الطبري الآملي هو سندس کوڙ سار لقب آهن جنهن ۾ الامام ، المجتهد ، المفسر ، المورخ، المحدث ، الحافظ ، العلامه ، اللغوي ، المقريءَ ۽ سندس اهي سڀئي القاب سندس بزرگيت تي دلالت ڪن ٿيون . سندس ڪنيت (ابن جرير) هئي ۽ طبرستان ۽ آمل ڏينهن نسبت هجڻ ڪري پاڻ الطبري ۽ الآملي سڏرائيندا هئا. ابن جرير رح هڪ آسودي گهراني ۾ اک کولي ، سندس پيءُ هڪ ڏينهن خواب ڏٺائين ته ابن جرير رح نبي ڪريم ﷺ جي ٻنهي هٿن جي وچ ۾ آهن ۽ نبي ڪريمﷺ جي هٿن مبارڪن ۾ پٿريون آهن جنهن کي ابن جرير رح کڻي اڇلائي رهيا آهن ، عالمن کان جڏهن هن جي تعبير پڇا ڪيائين ته انهن چيو ته اوهان جو پٽ وڏو ٿي ڪري دين جي خدمت سرانجام ڏيندو ۽ اهو خواب ابن جرير جو علم حاصل ڪرڻ جو سبب بڻيو. ابن جرير رح ستن سالن ۾ قرآن مجيد حفظ ڪيائين اٺن سالم ۾ امامت جهڙو فريضو انجام ڏنائين نون سالن ۾ حديث لکڻ شروع ڪيائين ۽ جڏهن سورهن سالن جا ٿيا ته اماماحمد بن حنبل رح جي زيارت جو شوق ۾ بغداد ڏانهن سفر ڪرڻ شروع ڪيائين ، سندس سڄو خرچ ۽ بار پيءُ کڻدو هو جڏهن سندس والد جو انتقال ٿيو ته ورثي ۾ زمين جو ٽڪڙو مليس جنهن جي آمدني مان ابن جرير رح پنهنجو گذر سفر فرمائيندا هئا .",
        "۱۔ ھن شق جي مطابق قادياني گروھ يا لاھوري گروھ جي ڪنھن رڪن کي جيڪو پاڻ کي 'احمدي' يا ڪنھن ٻي نالي سان پڪاري جي لاءِ ممنوع قرار ڏنو ويو آھي تہ ھو (الف) ڳالھائي، لکي يا ڪنھن ٻي طريقي سان ڪنھن خليفي يا آنحضور ﷺ جي ڪنھن صحابي کان علاوہڍه ڪنھن کي امير المومنين يا خليفہ المومنين يا خليفہ المسلمين يا صحابی يا رضي الله عنه چئي۔ (ب) آنحضور ﷺ جي گھروارين کان علاوه ڪنھن کي ام المومنين چئي۔ (ج) آنحضور ﷺ جي خاندان جي اھل بيت کان علاوہڍه ڪنھن کي اھل بيت چئي۔ (د) پنھنجي عبادت گاھ کي مسجد چئي۔",
        "سعد بن فضالہ جو شام کے جہاد میں سہیل کے ساتھ تھے بیان کرتے ہیں کہ ایک مرتبہ سہیل نے کہا کہ میں نے رسول اللہ ﷺ سے سنا ہے کہ خدا کی راہ میں ایک گھڑی صرف کرنا گھر کے تمام عمر کے اعمال سے بہتر ہے، اس لیے اب میں شام کا جہاد چھوڑ کر گھر نہ جاؤں گا اور یہیں جان دونگا، اس عہد پر اس سختی سے قائم رہے کہ طاعون عمواس میں بھی نہ ہٹے اور 18ھ میں اسی وبا میں شام کے غربت کدہ میں جان دی۔",
        "دعوت اسلام کے آغاز یعنی آنحضرتﷺ کے ارقم کے گھر میں تشریف لانے سے پہلے مشرف باسلام ہوئے،پھر ہجرت کے زمانہ میں مکہ سے مدینہ گئے آنحضرتﷺ نے غربت کی اجنبیت دورکرنے کے لیے ان میں اورابوعبیدہ بن تیہاں میں مواخاۃ کرادی۔",
        "ضرار اپنے قبیلہ کے اصحاب ثروت میں تھے، عرب میں سب سے بڑی دولت اونٹ کے گلے تھے، ضرار کے پاس ہزار اونٹوں کا گلہ تھا، اسلام کے جذب وولولے میں تمام مال ودولت چھوڑ کر خالی ہاتھ آستانِ نبوی پر پہنچے قبول اسلام کے بعد آنحضرتﷺ نے بنی صید اوربنی ہذیل کی طرف بھیجا۔",
        "(2) اگر زلیخا کو ملامت کرنے والی عورتیں آپ ﷺ کی جبین انور دیکھ پاتیں تو ہاتھوں کے بجائے اپنے دل کاٹنے کو ترجیح دیتیں۔صحیح بخاری میں ہے، حضرت عطاء بن یسار ؓہُنے حضرت عبداللہ بن عمرو ؓسے سیّدِ عالمﷺ کے وہ اوصاف دریافت کئے جو توریت میں مذکور ہیں تو انہوں نے فرمایا : ’’خدا کی قسم! حضور سیدُ المرسلینﷺ کے جو اوصاف قرآنِ کریم میں آئے ہیں انہیں میں سے بعض اوصاف توریت میں مذکور ہیں۔ اس کے بعد انہوں نے پڑھنا شروع کیا: اے نبی! ہم نے تمہیں شاہد و مُبَشِّر اور نذیر اور اُمِّیُّوں کا نگہبان بنا کر بھیجا، تم میرے بندے اور میرے رسول ہو، میں نے تمہارا نام متوکل رکھا،نہ بدخلق ہو نہ سخت مزاج، نہ بازاروں میں آواز بلند کرنے والے ہو نہ برائی سے برائی کو دفع کرنے والے بلکہ خطا کاروں کو معاف کرتے ہو اور ان پر احسان فرماتے ہو، اللہ تعالیٰ تمہیں نہ اٹھائے گا جب تک کہ تمہاری برکت سے غیر مستقیم ملت کو اس طرح راست نہ فرمادے کہ لوگ صدق و یقین کے ساتھ ’’ لَآاِلٰہَ اِلَّا اللہُ مُحَمَّدٌ رَّسُوْلُ اللہِ‘‘ پکارنے لگیں اور تمہاری بدولت اندھی آنکھیں بینا اور بہرے کان شنوا (سننے والے) اور پردوں میں لپٹے ہوئے دل کشادہ ہوجائیں۔ اور کعب احبارؓسے سرکارِ رسالت ﷺکی صفات میں توریت شریف کا یہ مضمون بھی منقول ہے کہ’’ اللہ تعالیٰ نے آپ ﷺکی صفت میں فرمایا کہ’’ میں اُنہیں ہر خوبی کے قابل کروں گا، اور ہر خُلقِ کریم عطا فرماؤں گا، اطمینانِ قلب اور وقار کو اُن کا لباس بناؤں گا اور طاعات وا حسان کو ان کا شعار کروں گا۔ تقویٰ کو ان کا ضمیر، حکمت کو ان کا راز، صدق و وفا کو اُن کی طبیعت ،عفوو کرم کو اُن کی عادت ، عدل کو ان کی سیرت، اظہارِ حق کو اُن کی شریعت، ہدایت کو اُن کا امام اور اسلام کو اُن کی ملت بناؤں گا۔ احمد اُن کا نام ہے، مخلوق کو اُن کے صدقے میں گمراہی کے بعد ہدایت اور جہالت کے بعد علم و معرفت اور گمنامی کے بعد رفعت و منزلت عطا کروں گا۔ اُنہیں کی برکت سے قلت کے بعد کثرت اور فقر کے بعد دولت اور تَفَرُّقے کے بعد محبت عنایت کروں گا، اُنہیں کی بدولت مختلف قبائل، غیر مجتمع خواہشوں اور اختلاف رکھنے والے دلوں میں اُلفت پیدا کروں گا اور اُن کی اُمت کو تمام اُمتوں سے بہتر کروں گا۔ ایک اور حدیث میں توریت سے حضور سید المرسلینﷺسے یہ اوصاف منقول ہیں ’’میرے بندے احمد مختار، ان کی جائے ولادت مکہ مکرمہ اور جائے ہجرت مدینہ طیبہ ہے،اُن کی اُمت ہر حال میں اللہ تعالٰی کی کثیر حمد کرنے والی ہے۔ مُنَزَّہٌ عَنْ شَرِیْکٍ فِیْ مَحَاسِنِہٖ",
        "بالآخر آنحضرتﷺ کے اس عفو وکرم نے یہ معجزہ دکھایا کہ سہیل حنین کی واپسی کے وقت آپ کے ساتھ ہوگئے اورمقام جعرانہ پہنچ کر خلعتِ اسلام سے سرفراز ہوئے آنحضرت ﷺ نے ازراہ مرحمت حنین کے مالِ غنیمت میں سے سو اونٹ عطا فرمائے، گو فتح مکہ کے بعد کے مسلمانوں کا شمار مؤلفۃ القلوب میں ہے، لیکن سہیل اس زمرہ میں اس حیثیت سے ممتاز ہیں کہ اسلام کے بعد ان سے کوئی بات اسلام کے خلاف ظہور پزیر نہیں ہوئی ،حافظ ابن حجرعسقلانی لکھتے ہیں، کان محمودالا سلام من حین اسلم۔", };

    public void testSingleSplit() {
        var chunker = new WordBoundaryChunker();
        var chunks = chunker.chunk(TEST_TEXT, 10_000, 0);
        assertThat(chunks, hasSize(1));
        assertEquals(TEST_TEXT, chunks.get(0));
    }

    public void testChunkSizeOnWhiteSpaceNoOverlap() {
        var numWhiteSpaceSeparatedWords = TEST_TEXT.split("\\s+").length;
        var chunker = new WordBoundaryChunker();

        for (var chunkSize : new int[] { 10, 20, 100, 300 }) {
            var chunks = chunker.chunk(TEST_TEXT, chunkSize, 0);
            int expectedNumChunks = (numWhiteSpaceSeparatedWords + chunkSize - 1) / chunkSize;
            assertThat(chunks, hasSize(expectedNumChunks));
        }
    }

    public void testMultilingual() {
        var chunker = new WordBoundaryChunker();
        for (var input : MULTI_LINGUAL) {
            var chunks = chunker.chunk(input, 10, 0);
            assertTrue(chunks.size() > 1);
        }
    }

    // public void testWindowSpanning() {
    // // for (int numWords : new int[] {22, 50, 73, 100}) {
    // for (int numWords : new int[] { 50 }) {
    // var sb = new StringBuilder();
    // for (int i = 0; i < numWords; i++) {
    // sb.append(Integer.toString(i)).append(' ');
    // }
    // var whiteSpacedText = sb.toString();
    // assertExpectedNumberOfChunks(whiteSpacedText, numWords, 10, 4);
    // assertExpectedNumberOfChunks(whiteSpacedText, numWords, 10, 2);
    // assertExpectedNumberOfChunks(whiteSpacedText, numWords, 20, 4);
    // assertExpectedNumberOfChunks(whiteSpacedText, numWords, 20, 10);
    // }
    // }

    public void testWindowSpanningWords() {
        int windowSize = 10;
        int overlap = 4;
        int numWords = randomIntBetween(4, 50);
        var input = new StringBuilder();
        for (int i = 0; i < numWords; i++) {
            input.append(Integer.toString(i)).append(' ');
        }
        var whiteSpacedText = input.toString();

        var chunks = new WordBoundaryChunker().chunk(whiteSpacedText, windowSize, overlap);
        int start = 0;
        int chunkIndex = 0;
        int newWordsPerWindow = windowSize - overlap;
        while (start + newWordsPerWindow < numWords) {
            var sb = new StringBuilder();
            int end = Math.min(start + windowSize, numWords);
            for (int i = start; i < end; i++) {
                sb.append(Integer.toString(i)).append(' ');
            }
            assertEquals(sb.toString().stripTrailing(), chunks.get(chunkIndex).stripTrailing());

            start += newWordsPerWindow;
            chunkIndex++;
        }

        assertEquals(chunks.size(), chunkIndex);
    }

    public void testWindowSpanning_TextShorterThanWindow() {
        var sb = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            sb.append(Integer.toString(i)).append(' ');
        }

        // window size is > num words
        var chunks = new WordBoundaryChunker().chunk(sb.toString(), 10, 5);
        assertThat(chunks, hasSize(1));
    }

    private void assertExpectedNumberOfChunks(String input, int numWords, int windowSize, int overlap) {
        var chunks = new WordBoundaryChunker().chunk(input, windowSize, overlap);
        int expected = expectedNumberOfChunks(numWords, windowSize, overlap);
        assertEquals(expected, chunks.size());
    }

    private int expectedNumberOfChunks(int numWords, int windowSize, int overlap) {
        if (numWords < windowSize) {
            return 1;
        }

        int newWordsPerWindow = windowSize - overlap;
        // int v = windowSize / newWordsPerWindow;
        int without = numWords - overlap;

        int expectedNumChunks = (without + newWordsPerWindow - 1) / newWordsPerWindow;
        return expectedNumChunks;
    }

    public void testInvalidParams() {
        var chunker = new WordBoundaryChunker();
        var e = expectThrows(IllegalArgumentException.class, () -> chunker.chunk("not evaluated", 4, 10));
        assertThat(e.getMessage(), containsString("Invalid chunking parameters, overlap [10] must be < window size [4]"));
    }
}
