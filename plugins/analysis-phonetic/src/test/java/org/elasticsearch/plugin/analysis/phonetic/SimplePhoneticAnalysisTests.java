/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.phonetic.DaitchMokotoffSoundexFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SimplePhoneticAnalysisTests extends ESTestCase {

    private TestAnalysis analysis;

    @Before
    public void setup() throws IOException {
        String yaml = "/org/elasticsearch/plugin/analysis/phonetic/phonetic-1.yml";
        Settings settings = Settings.builder()
            .loadFromStream(yaml, getClass().getResourceAsStream(yaml), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();
        this.analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisPhoneticPlugin());
    }

    public void testPhoneticTokenFilterFactory() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("phonetic");
        MatcherAssert.assertThat(filterFactory, instanceOf(PhoneticTokenFilterFactory.class));
    }

    public void testPhoneticTokenFilterBeiderMorseNoLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("ABADIAS"));
        String[] expected = new String[] {
            "abYdias",
            "abYdios",
            "abadia",
            "abadiaS",
            "abadias",
            "abadio",
            "abadioS",
            "abadios",
            "abodia",
            "abodiaS",
            "abodias",
            "abodio",
            "abodioS",
            "abodios",
            "avadias",
            "avadios",
            "avodias",
            "avodios",
            "obadia",
            "obadiaS",
            "obadias",
            "obadio",
            "obadioS",
            "obadios",
            "obodia",
            "obodiaS",
            "obodias",
            "obodioS" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterBeiderMorseWithLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilterfrench");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("Rimbault"));
        String[] expected = new String[] {
            "rimbD",
            "rimbDlt",
            "rimba",
            "rimbalt",
            "rimbo",
            "rimbolt",
            "rimbu",
            "rimbult",
            "rmbD",
            "rmbDlt",
            "rmba",
            "rmbalt",
            "rmbo",
            "rmbolt",
            "rmbu",
            "rmbult" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterDaitchMotokoff() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("daitch_mokotoff");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("chauptman"));
        String[] expected = new String[] { "473660", "573660" };
        assertThat(filterFactory.create(tokenizer), instanceOf(DaitchMokotoffSoundexFilter.class));
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterKoelnerPhonetik() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("BRAUN"));
        // koelnerphonetik treats "AUN"/"OWN" as an orthographic ambiguity and codes both spellings, joined by
        // "_": "BRAUN" -> B(1) R(7) [AUN silent] N(6) = "176", plus the generated variant "BROWN" -> B(1) R(7)
        // [O silent] W(3) N(6) = "1736".
        String[] expected = new String[] { "176_1736" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterHaasePhonetik() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("haasephonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("SCHMIDT"));
        // haasephonetik similarly treats "SCH"/"CH" as ambiguous and codes both spellings: "SCHMIDT" -> S(8)
        // [CH silent] M(6) I(-) D+T(2) = "862", plus the generated variant "CHMIDT" -> C(4) [H silent] M(6)
        // I(-) D+T(2) = "462".
        String[] expected = new String[] { "862_462" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    // Two independent pattern matches combine within one token: "AUN"/"OWN" (as in the BRAUN test above) and
    // "RB"/"RW" each double the candidate list once, so "BRAUNRB" yields all four combinations (BRAUNRB,
    // BROWNRB, BRAUNRW, BROWNRW), comfortably under the cap. Guards against a refactor of the exact-budget
    // branching logic silently dropping or corrupting legitimate multi-pattern combinations.
    public void testPhoneticTokenFilterKoelnerPhonetikMultiplePatternMatches() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("BRAUNRB"));
        String[] expected = new String[] { "17671_173671_17673_173673" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    // partition() splits "AUN-RB" into parts "AUNRB", "AUN", "RB", each independently producing its own
    // variations (4 + 2 + 2 = 8 total), all comfortably under the shared budget. Guards against a refactor of
    // the cross-part budget accounting silently dropping or corrupting legitimate multi-segment output.
    public void testPhoneticTokenFilterKoelnerPhonetikPunctuationSeparatedBelowCap() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("AUN-RB"));
        String[] expected = new String[] { "0671_03671_0673_03673_06_036_71_73" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    // Regression test for a heap-exhaustion bug: KoelnerPhonetik.getVariations() doubled its candidate list on
    // every pattern match, so a single token with many repeated pattern occurrences (e.g. "AUN") could produce
    // billions of variations from a token only tens of bytes long. Growth is now capped; a token with far more
    // pattern matches than any real name would contain must still yield a small, bounded number of variations.
    public void testPhoneticTokenFilterKoelnerPhonetikBoundsVariationExplosion() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("AUN".repeat(1000)));
        assertVariationCountIsBounded(filterFactory.create(tokenizer));
    }

    public void testPhoneticTokenFilterHaasePhonetikBoundsVariationExplosion() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("haasephonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("SCH".repeat(1000)));
        assertVariationCountIsBounded(filterFactory.create(tokenizer));
    }

    // Regression test for a related heap-exhaustion bug in KoelnerPhonetik.partition(): a token with n
    // punctuation-separated segments enumerates every contiguous run of segments as its own part (n(n+1)/2
    // parts), so a token built from many short hyphen-separated segments could multiply that count by each
    // part's own variations and still exhaust heap, independent of the single-part cap above. Growth across
    // all parts of one token now shares a single bounded budget.
    public void testPhoneticTokenFilterKoelnerPhonetikBoundsPartitionExplosion() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("AUN-".repeat(200)));
        assertVariationCountIsBounded(filterFactory.create(tokenizer));
    }

    public void testPhoneticTokenFilterHaasePhonetikBoundsPartitionExplosion() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("haasephonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("SCH-".repeat(200)));
        assertVariationCountIsBounded(filterFactory.create(tokenizer));
    }

    public void testKoelnerPhonetikGeneratePartsIsBounded() {
        KoelnerPhonetik encoder = new KoelnerPhonetik();
        List<String> parts = encoder.generateParts("AUN-".repeat(200));
        assertThat(parts.size(), lessThanOrEqualTo(16));
    }

    // Regression test: String.split(regex, limit) with a positive limit, unlike the unlimited single-argument
    // split() it replaced, does not drop trailing empty strings. A token ending in a separator therefore
    // produced a spurious trailing empty part, and the "skip the final full-string concatenation" numbering
    // shifted to treat the real segment as non-final, duplicating it as its own part too. Trailing separators
    // must have no effect on the segments generateParts() produces.
    public void testKoelnerPhonetikGeneratePartsIgnoresTrailingSeparators() {
        KoelnerPhonetik encoder = new KoelnerPhonetik();
        assertEquals(List.of("BRAUN"), encoder.generateParts("BRAUN-"));
        // A run of several trailing separators must not produce multiple empty parts either.
        assertEquals(List.of("BRAUN"), encoder.generateParts("BRAUN---"));
    }

    public void testKoelnerPhonetikGeneratePartsIgnoresLongRunOfTrailingSeparators() {
        KoelnerPhonetik encoder = new KoelnerPhonetik();
        assertEquals(List.of("BRAUN"), encoder.generateParts("BRAUN" + "-".repeat(20)));
    }

    // Same regression, observed through the public encode() path: trailing punctuation must not change a
    // token's phonetic codes at all, since it carries no letters of its own.
    public void testPhoneticTokenFilterKoelnerPhonetikTrailingPunctuation() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("BRAUN-"));
        String[] expected = new String[] { "176_1736" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterKoelnerPhonetikLongRunOfTrailingPunctuation() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("BRAUN" + "-".repeat(20)));
        String[] expected = new String[] { "176_1736" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterKoelnerPhonetikBoundsPartialBudgetOvershoot() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("koelnerphonetikfilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("AUN-RB-RB-AUN"));
        assertVariationCountIsBounded(filterFactory.create(tokenizer));
    }

    private static void assertVariationCountIsBounded(TokenStream tokenStream) throws IOException {
        CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        int tokenCount = 0;
        // The tokenizer itself may split a very long adversarial "word" into several tokens; the variation
        // bound must hold for each one individually.
        while (tokenStream.incrementToken()) {
            int variationCount = termAtt.toString().split("_").length;
            assertThat(variationCount, lessThanOrEqualTo(16));
            tokenCount++;
        }
        assertTrue(tokenCount > 0);
        tokenStream.close();
    }

}
