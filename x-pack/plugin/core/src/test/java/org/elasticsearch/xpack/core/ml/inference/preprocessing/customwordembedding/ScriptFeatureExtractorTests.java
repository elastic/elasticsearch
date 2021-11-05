/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.io.IOException;
import java.util.List;

public class ScriptFeatureExtractorTests extends ESTestCase {

    private void testScriptId(String text, ScriptCode expected) {
        long actual = ScriptFeatureExtractor.getScriptFeatureValue(text);
        assertEquals(expected.toInt(), actual);
    }

    public void testBasicExamples() {
        testScriptId("food", ScriptCode.Latin);
        testScriptId("字", ScriptCode.Hani);
        testScriptId("워드", ScriptCode.MAX_SCRIPT_CODE);
    }

    public void testSimpleJa() {
        testScriptId("オリンピック大会", ScriptCode.Hani);
        testScriptId("にほんごの クラスは かようびと もくようびで", ScriptCode.Hani);
    }

    public void testAllExamples() throws IOException {

        // compare against cld3 expected text type
        long[] expected = new long[] {
            1,
            6,
            1,
            3,
            3,
            10,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            2,
            1,
            1,
            1,
            1,
            1,
            6,
            1,
            1,
            1,
            1,
            1,
            12,
            1,
            9,
            1,
            1,
            1,
            1,
            4,
            1,
            1,
            1,
            1,
            5,
            24,
            1,
            23,
            3,
            30,
            16,
            102,
            1,
            20,
            1,
            1,
            1,
            1,
            3,
            17,
            3,
            9,
            1,
            1,
            22,
            9,
            1,
            1,
            1,
            11,
            1,
            1,
            1,
            3,
            18,
            1,
            1,
            1,
            1,
            3,
            1,
            1,
            1,
            1,
            14,
            15,
            3,
            19,
            1,
            3,
            6,
            1,
            1,
            5,
            1,
            24,
            1 };

        List<LanguageExamples.LanguageExampleEntry> entries = new LanguageExamples().getLanguageExamples();
        assertEquals(expected.length, entries.size());
        for (int i = 0; i < entries.size(); ++i) {
            String text = entries.get(i).getText();

            assertEquals(expected[i], ScriptFeatureExtractor.getScriptFeatureValue(text));
        }
    }

}
