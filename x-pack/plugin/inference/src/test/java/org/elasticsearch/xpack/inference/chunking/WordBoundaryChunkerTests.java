/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import com.ibm.icu.text.BreakIterator;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class WordBoundaryChunkerTests extends ESTestCase {

    @SuppressWarnings("checkstyle:linelength")
    public static final String TEST_TEXT =
        "Word segmentation is the problem of dividing a string of written language into its component words.\n"
            + "In English and many other languages using some form of the Latin alphabet, the space is a good approximation of a word divider "
            + "(word delimiter), although this concept has limits because of the variability with which languages emically regard collocations "
            + "and compounds. Many English compound nouns are variably written (for example, ice box = ice-box = icebox; pig sty = pig-sty = "
            + "pigsty) with a corresponding variation in whether speakers think of them as noun phrases or single nouns; there are trends in "
            + "how norms are set, such as that open compounds often tend eventually to solidify by widespread convention, but variation remains"
            + " systemic. In contrast, German compound nouns show less orthographic variation, with solidification being a stronger norm.";

    public static final String[] MULTI_LINGUAL = new String[] {
        "Građevne strukture Mesa Verde dokaz su akumuliranog znanja i vještina koje su se stoljećima prenosile generacijama civilizacije"
            + " Anasazi. Vrhunce svojih dosega ostvarili su u 12. i 13. stoljeću, kada su sagrađene danas najpoznatije građevine na "
            + "liticama. Zidali su obrađenim pješčenjakom, tvrđim kamenom oblikovanim do veličine štruce kruha. Kao žbuku između ciglā "
            + "stavljali su glinu razmočenu vodom. Tim su materijalom gradili prostorije veličine do 6 četvornih metara. U potkrovljima "
            + "su skladištili žitarice i druge plodine, dok su kive - ceremonijalne prostorije - gradili ispred soba, ali ukopane u zemlju,"
            + " nešto poput današnjih podruma. Kiva je bila vrhunski dizajnirana prostorija okruglog oblika s prostorom za vatru zimi te s"
            + " dovodom hladnog zraka za klimatizaciju ljeti. U zidane konstrukcije stavljali su i lokalno posječena stabla, što današnjim"
            + " arheolozima pomaže u preciznom datiranju nastanka pojedine građevine metodom dendrokronologije. Ta stabla pridonose i"
            + " teoriji o mogućem konačnom slomu ondašnjeg društva. Nakon što su, tijekom nekoliko stoljeća, šume do kraja srušene, a "
            + "njihova obnova zbog sušne klime traje i po 200 godina, nije proteklo puno vremena do konačnog urušavanja civilizacije, "
            + "koja se, na svojem vrhuncu osjećala nepobjedivom. 90 % sagrađenih naseobina ispod stijena ima do deset prostorija. ⅓ od "
            + "ukupnog broja sagrađenih kuća ima jednu ili dvije kamene prostorije",
        "Histoarysk wie in acre in stik lân dat 40 roeden (oftewol 1 furlong of ⅛ myl of 660 foet) lang wie, en 4 roeden (of 66 foet) "
            + "breed. Men is fan tinken dat dat likernôch de grûnmjitte wie dy't men mei in jok oksen yn ien dei beploegje koe.",
        "創業当初の「太平洋化学工業社」から1959年太平洋化学工業株式会社へ、1987年には太平洋化学㈱に社名を変更。 1990年以降、海外拠点を増やし本格的な国際進出を始動。"
            + " 創業者がつくりあげた化粧品会社を世界企業へと成長させるべく2002年3月英文社名AMOREPACIFICに改めた。",
        "۱۔ ھن شق جي مطابق قادياني گروھ يا لاھوري گروھ جي ڪنھن رڪن کي جيڪو پاڻ کي 'احمدي' يا ڪنھن ٻي نالي سان پڪاري جي لاءِ ممنوع قرار "
            + "ڏنو ويو آھي تہ ھو (الف) ڳالھائي، لکي يا ڪنھن ٻي طريقي سان ڪنھن خليفي يا آنحضور ﷺ جي ڪنھن صحابي کان علاوہڍه ڪنھن کي امير"
            + " المومنين يا"
            + " خليفہ المومنين يا خليفہ المسلمين يا صحابی يا رضي الله عنه چئي۔ (ب) آنحضور ﷺ جي گھروارين کان علاوه ڪنھن کي ام المومنين "
            + "چئي۔ (ج) آنحضور ﷺ جي خاندان جي اھل بيت کان علاوہڍه ڪنھن کي اھل بيت چئي۔ (د) پنھنجي عبادت گاھ کي مسجد چئي۔" };

    public static int NUM_WORDS_IN_TEST_TEXT;
    static {
        var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
        wordIterator.setText(TEST_TEXT);
        int wordCount = 0;
        while (wordIterator.next() != BreakIterator.DONE) {
            wordCount++;
        }
        NUM_WORDS_IN_TEST_TEXT = wordCount;
    }

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
            assertThat("chunk size= " + chunkSize, chunks, hasSize(expectedNumChunks));
        }
    }

    public void testMultilingual() {
        var chunker = new WordBoundaryChunker();
        for (var input : MULTI_LINGUAL) {
            var chunks = chunker.chunk(input, 10, 0);
            assertTrue(chunks.size() > 1);
        }
    }

    public void testNumberOfChunks() {
        for (int numWords : new int[] { 10, 22, 50, 73, 100 }) {
            var sb = new StringBuilder();
            for (int i = 0; i < numWords; i++) {
                sb.append(i).append(' ');
            }
            var whiteSpacedText = sb.toString();
            assertExpectedNumberOfChunks(whiteSpacedText, numWords, 10, 4);
            assertExpectedNumberOfChunks(whiteSpacedText, numWords, 10, 2);
            assertExpectedNumberOfChunks(whiteSpacedText, numWords, 20, 4);
            assertExpectedNumberOfChunks(whiteSpacedText, numWords, 20, 10);
        }
    }

    public void testNumberOfChunksWithWordBoundaryChunkingSettings() {
        for (int numWords : new int[] { 10, 22, 50, 73, 100 }) {
            var sb = new StringBuilder();
            for (int i = 0; i < numWords; i++) {
                sb.append(i).append(' ');
            }
            var whiteSpacedText = sb.toString();
            assertExpectedNumberOfChunksWithWordBoundaryChunkingSettings(
                whiteSpacedText,
                numWords,
                new WordBoundaryChunkingSettings(10, 4)
            );
            assertExpectedNumberOfChunksWithWordBoundaryChunkingSettings(
                whiteSpacedText,
                numWords,
                new WordBoundaryChunkingSettings(10, 2)
            );
            assertExpectedNumberOfChunksWithWordBoundaryChunkingSettings(
                whiteSpacedText,
                numWords,
                new WordBoundaryChunkingSettings(20, 4)
            );
            assertExpectedNumberOfChunksWithWordBoundaryChunkingSettings(
                whiteSpacedText,
                numWords,
                new WordBoundaryChunkingSettings(20, 10)
            );
        }
    }

    public void testInvalidChunkingSettingsProvided() {
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(randomIntBetween(20, 300), 0);
        assertThrows(IllegalArgumentException.class, () -> { new WordBoundaryChunker().chunk(TEST_TEXT, chunkingSettings); });
    }

    public void testWindowSpanningWithOverlapNumWordsInOverlapSection() {
        int chunkSize = 10;
        int windowSize = 3;
        for (int numWords : new int[] { 7, 8, 9, 10 }) {
            var sb = new StringBuilder();
            for (int i = 0; i < numWords; i++) {
                sb.append(i).append(' ');
            }
            var chunks = new WordBoundaryChunker().chunk(sb.toString(), chunkSize, windowSize);
            assertEquals("numWords= " + numWords, 1, chunks.size());
        }

        var sb = new StringBuilder();
        for (int i = 0; i < 11; i++) {
            sb.append(i).append(' ');
        }
        var chunks = new WordBoundaryChunker().chunk(sb.toString(), chunkSize, windowSize);
        assertEquals(2, chunks.size());
    }

    public void testWindowSpanningWords() {
        int numWords = randomIntBetween(4, 120);
        var input = new StringBuilder();
        for (int i = 0; i < numWords; i++) {
            input.append(i).append(' ');
        }
        var whiteSpacedText = input.toString().stripTrailing();

        var chunks = new WordBoundaryChunker().chunk(whiteSpacedText, 20, 10);
        assertChunkContents(chunks, numWords, 20, 10);
        chunks = new WordBoundaryChunker().chunk(whiteSpacedText, 10, 4);
        assertChunkContents(chunks, numWords, 10, 4);
        chunks = new WordBoundaryChunker().chunk(whiteSpacedText, 15, 3);
        assertChunkContents(chunks, numWords, 15, 3);
    }

    private void assertChunkContents(List<String> chunks, int numWords, int windowSize, int overlap) {
        int start = 0;
        int chunkIndex = 0;
        int newWordsPerWindow = windowSize - overlap;
        boolean reachedEnd = false;
        while (reachedEnd == false) {
            var sb = new StringBuilder();
            // the trailing whitespace from the previous chunk is
            // included in this chunk
            if (chunkIndex > 0) {
                sb.append(" ");
            }
            int end = Math.min(start + windowSize, numWords);
            for (int i = start; i < end; i++) {
                sb.append(i).append(' ');
            }
            // delete the trailing whitespace
            sb.deleteCharAt(sb.length() - 1);

            assertEquals("numWords= " + numWords, sb.toString(), chunks.get(chunkIndex));

            reachedEnd = end == numWords;
            start += newWordsPerWindow;
            chunkIndex++;
        }

        assertEquals("numWords= " + numWords, chunks.size(), chunkIndex);
    }

    public void testWindowSpanning_TextShorterThanWindow() {
        var sb = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            sb.append(i).append(' ');
        }

        // window size is > num words
        var chunks = new WordBoundaryChunker().chunk(sb.toString(), 10, 5);
        assertThat(chunks, hasSize(1));
    }

    public void testEmptyString() {
        var chunks = new WordBoundaryChunker().chunk("", 10, 5);
        assertThat(chunks, contains(""));
    }

    public void testWhitespace() {
        var chunks = new WordBoundaryChunker().chunk(" ", 10, 5);
        assertThat(chunks, contains(" "));
    }

    public void testPunctuation() {
        int chunkSize = 1;
        var chunks = new WordBoundaryChunker().chunk("Comma, separated", chunkSize, 0);
        assertThat(chunks, contains("Comma", ", separated"));

        chunks = new WordBoundaryChunker().chunk("Mme. Thénardier", chunkSize, 0);
        assertThat(chunks, contains("Mme", ". Thénardier"));

        chunks = new WordBoundaryChunker().chunk("Won't you chunk", chunkSize, 0);
        assertThat(chunks, contains("Won't", " you", " chunk"));

        chunkSize = 10;
        chunks = new WordBoundaryChunker().chunk("Won't you chunk", chunkSize, 0);
        assertThat(chunks, contains("Won't you chunk"));
    }

    private void assertExpectedNumberOfChunksWithWordBoundaryChunkingSettings(
        String input,
        int numWords,
        WordBoundaryChunkingSettings chunkingSettings
    ) {
        var chunks = new WordBoundaryChunker().chunk(input, chunkingSettings);
        int expected = expectedNumberOfChunks(numWords, chunkingSettings.maxChunkSize, chunkingSettings.overlap);
        assertEquals(expected, chunks.size());
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

        // the first chunk has windowSize words, because of overlap
        // the subsequent will consume fewer new words
        int wordsRemainingAfterFirstChunk = numWords - windowSize;
        int newWordsPerWindow = windowSize - overlap;
        int numberOfFollowingChunks = (wordsRemainingAfterFirstChunk + newWordsPerWindow - 1) / newWordsPerWindow;
        // the +1 accounts for the first chunk
        return 1 + numberOfFollowingChunks;
    }

    public void testInvalidParams() {
        var chunker = new WordBoundaryChunker();
        var e = expectThrows(IllegalArgumentException.class, () -> chunker.chunk("not evaluated", 4, 10));
        assertThat(e.getMessage(), containsString("Invalid chunking parameters, overlap [10] must be < chunk size / 2 [4 / 2 = 2]"));

        e = expectThrows(IllegalArgumentException.class, () -> chunker.chunk("not evaluated", 10, 6));
        assertThat(e.getMessage(), containsString("Invalid chunking parameters, overlap [6] must be < chunk size / 2 [10 / 2 = 5]"));
    }
}
