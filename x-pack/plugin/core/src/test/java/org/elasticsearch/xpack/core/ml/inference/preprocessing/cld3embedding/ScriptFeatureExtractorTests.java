/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

public class ScriptFeatureExtractorTests extends ESTestCase {

    private void testScriptId(String text, ULScript expected) {
        long actual = ScriptFeatureExtractor.getScriptFeatureValue(text);
        assertEquals(expected.toInt(), actual);
    }

    public void testBasicExamples() {
        testScriptId("food", ULScript.ULScript_Latin);
        // "字" = 23383, 0x5b37
        testScriptId("字", ULScript.ULScript_Hani);
        testScriptId("워드", ULScript.NUM_ULSCRIPTS);
    }

    public void testSimpleJa() {
        // TODO - test more にほんごの クラスは かようびと もくようびで (KATAKANA)
        testScriptId("オリンピック大会", ULScript.ULScript_Hani);
        testScriptId("にほんごの クラスは かようびと もくようびで", ULScript.ULScript_Hani);
    }

    public void testAllExamples() {

        // compare against cld3 expected text type
        long[] expected = new long[]{1, 6, 1, 3, 3, 10, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 6, 1, 1, 1, 1, 1, 12, 1, 9, 1, 1, 1, 1,
            4, 1, 1, 1, 1, 5, 24, 1, 23, 3, 30, 16, 102, 1, 20, 1, 1, 1, 1, 3, 17, 3, 9, 1, 1, 22, 9, 1, 1, 1, 11, 1, 1, 1, 3, 18, 1,
            1, 1, 1, 3, 1, 1, 1, 1, 14, 15, 3, 19, 1, 3, 6, 1, 1, 5, 1, 24, 1};

        assertEquals(expected.length, LanguageExamples.goldLangText.length);
        for (int i = 0; i < LanguageExamples.goldLangText.length; ++i) {
            String text = LanguageExamples.goldLangText[i][1];

            assertEquals(expected[i], ScriptFeatureExtractor.getScriptFeatureValue(text));
        }
    }

    public void testAllCodePoints() {
        // Test to test all unicode code points against cld3 output - currently different
        // TODO - script_ids between cld3 and Java codepoint script are different
        // TODO - investigate this further
       /*
        final int numCodePoints = 17 * 65536;

       for (int i = 0; i <= numCodePoints; ++i) {
           int [] codePoints = {i};
           String text = new String(codePoints, 0, 1);
           long actual = ScriptFeatureExtractor.getScriptFeatureValue(text);
           Character.UnicodeScript unicodeScript = Character.UnicodeScript.of(text.codePointAt(0));
           Matcher m = Pattern.compile("\\b\\p{L}").matcher(text);

           System.out.println(i + ":" + actual + " " + unicodeScript + " " + m.find());
       }

        int i = 13319;
        int[] codePoints = {i};
        String text = new String(codePoints, 0, 1);
        long actual = ScriptFeatureExtractor.getScriptFeatureValue(text);
        Character.UnicodeScript unicodeScript = Character.UnicodeScript.of(text.codePointAt(0));
        Matcher m = Pattern.compile("\\b\\p{L}").matcher(text);

        System.out.println(i + ":" + actual + " " + unicodeScript + " " + m.find());
        */
    }
}

