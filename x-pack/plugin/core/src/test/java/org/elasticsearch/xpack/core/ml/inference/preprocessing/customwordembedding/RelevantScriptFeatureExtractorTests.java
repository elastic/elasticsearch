/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class RelevantScriptFeatureExtractorTests extends ESTestCase {
    
    private final RelevantScriptFeatureExtractor extractor = new RelevantScriptFeatureExtractor();

    private static final double eps = 0.000001;

    public void testNonMixedScripts() {
        FeatureValue[] results;

        results = extractor.extractFeatures("just some plain text");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptOtherUtf8OneByte.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));

        results = extractor.extractFeatures("ヸヂ゠ヂ");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptKatakana.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));

        // One UTF8 character by itself.
        results = extractor.extractFeatures("ゟ");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptHiragana.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));

        results = extractor.extractFeatures("ה");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptHebrew.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));
    }

    public void testMixedScripts() {
        FeatureValue[] results;

        results = extractor.extractFeatures("ヸtヂe゠xtヂ");
        assertThat(results.length, equalTo(2));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptOtherUtf8OneByte.toInt()));
        assertThat(results[0].getWeight(), closeTo(0.5, eps));
        assertThat(results[1].getRow(), equalTo(ScriptDetector.Script.kScriptKatakana.toInt()));
        assertThat(results[1].getWeight(), closeTo(0.5, eps));

        results = extractor.extractFeatures("just some 121212%^^( ヸヂ゠ヂ   text");
        assertThat(results.length, equalTo(2));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptOtherUtf8OneByte.toInt()));
        assertThat(results[0].getWeight(), closeTo(0.75, eps));
        assertThat(results[1].getRow(), equalTo(ScriptDetector.Script.kScriptKatakana.toInt()));
        assertThat(results[1].getWeight(), closeTo(0.25, eps));
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

        // One UTF8 character with some numbers / punctuation / spaces: character at
        // one extremity or in the middle.
        results = extractor.extractFeatures("1234ゟ");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptHiragana.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));

        results = extractor.extractFeatures("ゟ12-(");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptHiragana.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));

        results = extractor.extractFeatures("8*1ゟ12----");
        assertThat(results.length, equalTo(1));
        assertThat(results[0].getRow(), equalTo(ScriptDetector.Script.kScriptHiragana.toInt()));
        assertThat(results[0].getWeight(), closeTo(1.0, eps));;
    }

}
