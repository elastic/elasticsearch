/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

public class ScriptDetectorTests extends ESTestCase {

    public void testGreekScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("Ͱ".codePointAt(0)), is(ScriptDetector.Script.kScriptGreek));
        assertThat(ScriptDetector.Script.fromCodePoint("Ͽ".codePointAt(0)), is(ScriptDetector.Script.kScriptGreek));
        assertThat(ScriptDetector.Script.fromCodePoint("δ".codePointAt(0)), is(ScriptDetector.Script.kScriptGreek));
        assertThat(ScriptDetector.Script.fromCodePoint("Θ".codePointAt(0)), is(ScriptDetector.Script.kScriptGreek));
        assertThat(ScriptDetector.Script.fromCodePoint("Δ".codePointAt(0)), is(ScriptDetector.Script.kScriptGreek));
        assertThat(ScriptDetector.Script.fromCodePoint("a".codePointAt(0)), is(not(ScriptDetector.Script.kScriptGreek)));
        assertThat(ScriptDetector.Script.fromCodePoint("a".codePointAt(0)), is(not(ScriptDetector.Script.kScriptGreek)));
    }

    public void testCyrillicScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("Ѐ".codePointAt(0)), is(ScriptDetector.Script.kScriptCyrillic));
        assertThat(ScriptDetector.Script.fromCodePoint("ӿ".codePointAt(0)), is(ScriptDetector.Script.kScriptCyrillic));
        assertThat(ScriptDetector.Script.fromCodePoint("ш".codePointAt(0)), is(ScriptDetector.Script.kScriptCyrillic));
        assertThat(ScriptDetector.Script.fromCodePoint("Б".codePointAt(0)), is(ScriptDetector.Script.kScriptCyrillic));
        assertThat(ScriptDetector.Script.fromCodePoint("Ӱ".codePointAt(0)), is(ScriptDetector.Script.kScriptCyrillic));
    }

    public void testHebrewScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("֑".codePointAt(0)), is(ScriptDetector.Script.kScriptHebrew));
        assertThat(ScriptDetector.Script.fromCodePoint("״".codePointAt(0)), is(ScriptDetector.Script.kScriptHebrew));
        assertThat(ScriptDetector.Script.fromCodePoint("ד".codePointAt(0)), is(ScriptDetector.Script.kScriptHebrew));
        assertThat(ScriptDetector.Script.fromCodePoint("ה".codePointAt(0)), is(ScriptDetector.Script.kScriptHebrew));
        assertThat(ScriptDetector.Script.fromCodePoint("צ".codePointAt(0)), is(ScriptDetector.Script.kScriptHebrew));
    }

    public void testArabicScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("م".codePointAt(0)), is(ScriptDetector.Script.kScriptArabic));
        assertThat(ScriptDetector.Script.fromCodePoint("خ".codePointAt(0)), is(ScriptDetector.Script.kScriptArabic));
    }

    public void testHangulJamoScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("ᄀ".codePointAt(0)), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.Script.fromCodePoint("ᇿ".codePointAt(0)), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.Script.fromCodePoint("ᄡ".codePointAt(0)), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.Script.fromCodePoint("ᆅ".codePointAt(0)), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.Script.fromCodePoint("ᅘ".codePointAt(0)), is(ScriptDetector.Script.kScriptHangulJamo));
    }

    public void testHiraganaScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("ぁ".codePointAt(0)), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.Script.fromCodePoint("ゟ".codePointAt(0)), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.Script.fromCodePoint("こ".codePointAt(0)), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.Script.fromCodePoint("や".codePointAt(0)), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.Script.fromCodePoint("ぜ".codePointAt(0)), is(ScriptDetector.Script.kScriptHiragana));
    }

    public void testKatakanaScript() {
        assertThat(ScriptDetector.Script.fromCodePoint("゠".codePointAt(0)), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.Script.fromCodePoint("ヿ".codePointAt(0)), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.Script.fromCodePoint("ヂ".codePointAt(0)), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.Script.fromCodePoint("ザ".codePointAt(0)), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.Script.fromCodePoint("ヸ".codePointAt(0)), is(ScriptDetector.Script.kScriptKatakana));
    }

    public void testOtherOneByte() {
        assertThat(ScriptDetector.Script.fromCodePoint("^".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8OneByte));
        assertThat(ScriptDetector.Script.fromCodePoint("$".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8OneByte));
    }

    public void testOtherTwoBytes() {
        // Unrecognized 2-byte scripts.  For info on the scripts mentioned below, see
        // http://www.unicode.org/charts/#scripts Note: the scripts below are uniquely
        // associated with a language.  Still, the number of queries in those
        // languages is small and we didn't want to increase the code size and
        // latency, so (at least for now) we do not treat them specially.
        // The following three tests are, respectively, for Armenian, Syriac and
        // Thaana.
        assertThat(ScriptDetector.Script.fromCodePoint("Ձ".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8TwoBytes));
        assertThat(ScriptDetector.Script.fromCodePoint("ܔ".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8TwoBytes));
        assertThat(ScriptDetector.Script.fromCodePoint("ށ".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8TwoBytes));
    }

    public void testOtherThreeBytes() {
        // Unrecognized 3-byte script: CJK Unified Ideographs. Not uniquely associated with a language.
        assertThat(ScriptDetector.Script.fromCodePoint("万".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8ThreeBytes));
        assertThat(ScriptDetector.Script.fromCodePoint("両".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8ThreeBytes));
    }

    public void testOtherFourBytes() {
        // Unrecognized 4-byte script: CJK Unified Ideographs Extension C. Not uniquely associated with a language.
        assertThat(ScriptDetector.Script.fromCodePoint("𪩘".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8FourBytes));

        // Unrecognized 4-byte script: CJK Unified Ideographs Extension E. Not uniquely associated with a language.
        assertThat(ScriptDetector.Script.fromCodePoint("𫢸".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8FourBytes));
        assertThat(ScriptDetector.Script.fromCodePoint("𫫇".codePointAt(0)), is(ScriptDetector.Script.kScriptOtherUtf8FourBytes));
    }
}
