/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class RelevantScriptFeatureExtractorTests extends ESTestCase {
    
    private final RelevantScriptFeatureExtractor extractor = new RelevantScriptFeatureExtractor();
    
    private void testEvaluateRelevantScriptResults(FeatureValue[] results,
                                                   int size,
                                                   int index,
                                                   ScriptDetector.Script id,
                                                   float weight) {
        assertEquals(size, results.length);
        assertTrue(index >= 0 && index < results.length);
        assertThat(results[index], instanceOf(ContinuousFeatureValue.class));
        assertEquals(results[index].getRow(), id.toInt());
        assertEquals(results[index].getWeight(), weight, 0.0001f);
    }

    public void testRelevantScriptFeatureCommonCases() {
        FeatureValue[] results;

        results = extractor.extractFeatures("just some plain text");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptOtherUtf8OneByte, 1.0f );

        results = extractor.extractFeatures("ヸヂ゠ヂ");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptKatakana, 1.0f );

        // 4 Latin letters mixed with 4 Katakana letters.
        results = extractor.extractFeatures("ヸtヂe゠xtヂ");
        testEvaluateRelevantScriptResults(results, 2, 0, ScriptDetector.Script.kScriptOtherUtf8OneByte, 0.5f );
        testEvaluateRelevantScriptResults(results, 2, 1, ScriptDetector.Script.kScriptKatakana, 0.5f );

        results = extractor.extractFeatures("\"just some 121212%^^( ヸヂ゠ヂ   text\"");
        testEvaluateRelevantScriptResults(results, 2, 0, ScriptDetector.Script.kScriptOtherUtf8OneByte, 0.75f );
        testEvaluateRelevantScriptResults(results, 2, 1, ScriptDetector.Script.kScriptKatakana, 0.25f );
    }

    public void testRelevantScriptFeatureCornerCases() {
        FeatureValue[] results;

        // Empty string.
        results = extractor.extractFeatures("");
        assertEquals(0, results.length);

        // Only whitespaces.
        results = extractor.extractFeatures("   ");
        assertEquals(0, results.length);

        // Only numbers and punctuation.
        results = extractor.extractFeatures("12----)(");
        assertEquals(0, results.length);

        // Only numbers, punctuation, and spaces.
        results = extractor.extractFeatures("12--- - ) ( ");
        assertEquals(0, results.length);

        // One UTF8 character by itself.
        results = extractor.extractFeatures("ゟ");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );

        results = extractor.extractFeatures("ה");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHebrew, 1.0f );

        // One UTF8 character with some numbers / punctuation / spaces: character at
        // one extremity or in the middle.
        results = extractor.extractFeatures("1234ゟ");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );

        results = extractor.extractFeatures("ゟ12-(");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );

        results = extractor.extractFeatures("8*1ゟ12----");
        testEvaluateRelevantScriptResults(results, 1, 0, ScriptDetector.Script.kScriptHiragana, 1.0f );
    }

}
