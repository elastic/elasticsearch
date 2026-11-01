/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.apache.commons.codec.EncoderException;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.StringReader;

/**
 * Tests the fixed and legacy handling of the anchored Haase variation rules ({@code ^CH} and the
 * {@code $}-anchored ones), and that the phonetic filter factory selects between them by index
 * creation version.
 */
public class HaasePhonetikTests extends ESTestCase {

    public void testStartingChKeepsFollowingLetter() throws EncoderException {
        // the L (digit 5) used to be dropped from both the "CH" and the "SCH" spelling
        assertEquals("45_85", new HaasePhonetik().encode("CHLOE"));
    }

    public void testLegacyStartingChDropsFollowingLetter() throws EncoderException {
        assertEquals("4_8", new HaasePhonetik(KoelnerPhonetik.DEFAULT_MAX_VARIATIONS, true).encode("CHLOE"));
    }

    public void testStartingChFollowedByAnotherRuleMatch() throws EncoderException {
        // The A the legacy code skipped is the first letter of the AUX match (AUX ranks below ^CH, so it is
        // found in the next round): advancing to the match end keeps it, and the AUX -> O variations CHO
        // and SCHO (codes 4 and 8) appear. The legacy code jumped past the A and never found AUX.
        assertEquals("48_848_4_8", new HaasePhonetik().encode("CHAUX"));
        assertEquals("48_848", new HaasePhonetik(KoelnerPhonetik.DEFAULT_MAX_VARIATIONS, true).encode("CHAUX"));
    }

    public void testStartingChFollowedByVowelDoesNotChangeCodes() throws EncoderException {
        // Only initial vowels contribute a digit, so the vowel the legacy code skipped after CH never
        // altered the codes: the fix only changes codes when the skipped letter was a consonant (or, as
        // above, started another rule's match).
        for (String word : new String[] { "CHARLES", "CHAIM" }) {
            assertEquals(
                word,
                new HaasePhonetik(KoelnerPhonetik.DEFAULT_MAX_VARIATIONS, true).encode(word),
                new HaasePhonetik().encode(word)
            );
        }
    }

    public void testEndAnchoredRulesDoNotChangeCodes() throws EncoderException {
        for (String word : new String[] { "MARIA", "OTTO", "BEAU", "ANNA" }) {
            assertEquals(
                word,
                new HaasePhonetik(KoelnerPhonetik.DEFAULT_MAX_VARIATIONS, true).encode(word),
                new HaasePhonetik().encode(word)
            );
        }
    }

    public void testKoelnerPhonetikUnaffected() throws EncoderException {
        assertEquals(
            new KoelnerPhonetik().encode("CHLOE"),
            new KoelnerPhonetik(KoelnerPhonetik.DEFAULT_MAX_VARIATIONS, true).encode("CHLOE")
        );
    }

    public void testFilterFactoryGatesOnIndexVersion() throws IOException {
        assertEquals("4_8", factoryCode(IndexVersionUtils.getPreviousVersion(IndexVersions.HAASE_PHONETIK_ANCHORED_VARIATIONS)));
        assertEquals("45_85", factoryCode(IndexVersions.HAASE_PHONETIK_ANCHORED_VARIATIONS));
        assertEquals("45_85", factoryCode(IndexVersion.current()));
    }

    private String factoryCode(IndexVersion created) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, created)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.haase.type", "phonetic")
            .put("index.analysis.filter.haase.encoder", "haasephonetik")
            .build();
        TokenFilterFactory factory = new AnalysisPhoneticPlugin().getTokenFilters()
            .get("phonetic")
            .get(
                IndexSettingsModule.newIndexSettings("index", settings),
                null,
                "haase",
                settings.getByPrefix("index.analysis.filter.haase.")
            );
        try (Tokenizer tokenizer = new WhitespaceTokenizer()) {
            tokenizer.setReader(new StringReader("CHLOE"));
            var stream = factory.create(tokenizer);
            var term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            assertTrue(stream.incrementToken());
            String code = term.toString();
            stream.end();
            return code;
        }
    }
}
