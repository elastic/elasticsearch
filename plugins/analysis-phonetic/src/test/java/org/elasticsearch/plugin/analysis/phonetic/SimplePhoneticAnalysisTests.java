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
