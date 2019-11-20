/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

public class ScriptDetectorTests extends ESTestCase {

    public void testGreekScript() {
        // The first two conditions check first / last character from the Greek and
        // Coptic script. The last two ones are negative tests.
        assertThat(ScriptDetector.Script.kScriptGreek, is(ScriptDetector.getScript("Ͱ")));
        assertThat(ScriptDetector.Script.kScriptGreek, is(ScriptDetector.getScript("Ͽ")));
        assertThat(ScriptDetector.Script.kScriptGreek, is(ScriptDetector.getScript("δ")));
        assertThat(ScriptDetector.Script.kScriptGreek, is(ScriptDetector.getScript("Θ")));
        assertThat(ScriptDetector.Script.kScriptGreek, is(ScriptDetector.getScript("Δ")));
        assertThat(ScriptDetector.Script.kScriptGreek, is(not(ScriptDetector.getScript("a"))));
        assertThat(ScriptDetector.Script.kScriptGreek, is(not(ScriptDetector.getScript("a"))));
    }

    public void testCyrillicScript() {
        assertThat(ScriptDetector.Script.kScriptCyrillic, is(ScriptDetector.getScript("Ѐ")));
        assertThat(ScriptDetector.Script.kScriptCyrillic, is(ScriptDetector.getScript("ӿ")));
        assertThat(ScriptDetector.Script.kScriptCyrillic, is(ScriptDetector.getScript("ш")));
        assertThat(ScriptDetector.Script.kScriptCyrillic, is(ScriptDetector.getScript("Б")));
        assertThat(ScriptDetector.Script.kScriptCyrillic, is(ScriptDetector.getScript("Ӱ")));
    }

    public void testHebrewScript() {
        assertThat(ScriptDetector.Script.kScriptHebrew, is(ScriptDetector.getScript("֑")));
        assertThat(ScriptDetector.Script.kScriptHebrew, is(ScriptDetector.getScript("״")));
        assertThat(ScriptDetector.Script.kScriptHebrew, is(ScriptDetector.getScript("ד")));
        assertThat(ScriptDetector.Script.kScriptHebrew, is(ScriptDetector.getScript("ה")));
        assertThat(ScriptDetector.Script.kScriptHebrew, is(ScriptDetector.getScript("צ")));
    }

    public void testArabicScript() {
        assertThat(ScriptDetector.getScript("م"), is(ScriptDetector.Script.kScriptArabic));
        assertThat(ScriptDetector.getScript("خ"), is(ScriptDetector.Script.kScriptArabic));
    }

    public void testHangulJamoScript() {
        assertThat(ScriptDetector.getScript("ᄀ"), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.getScript("ᇿ"), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.getScript("ᄡ"), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.getScript("ᆅ"), is(ScriptDetector.Script.kScriptHangulJamo));
        assertThat(ScriptDetector.getScript("ᅘ"), is(ScriptDetector.Script.kScriptHangulJamo));
    }

    public void testHiraganaScript() {
        assertThat(ScriptDetector.getScript("ぁ"), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.getScript("ゟ"), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.getScript("こ"), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.getScript("や"), is(ScriptDetector.Script.kScriptHiragana));
        assertThat(ScriptDetector.getScript("ぜ"), is(ScriptDetector.Script.kScriptHiragana));
    }

    public void testKatakanaScript() {
        assertThat(ScriptDetector.getScript("゠"), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.getScript("ヿ"), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.getScript("ヂ"), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.getScript("ザ"), is(ScriptDetector.Script.kScriptKatakana));
        assertThat(ScriptDetector.getScript("ヸ"), is(ScriptDetector.Script.kScriptKatakana));
    }

    public void testOtherScripts() {
        assertThat(ScriptDetector.getScript("^"), is(ScriptDetector.Script.kScriptOtherUtf8OneByte));
        assertThat(ScriptDetector.getScript("$"), is(ScriptDetector.Script.kScriptOtherUtf8OneByte));

        // Unrecognized 2-byte scripts.  For info on the scripts mentioned below, see
        // http://www.unicode.org/charts/#scripts Note: the scripts below are uniquely
        // associated with a language.  Still, the number of queries in those
        // languages is small and we didn't want to increase the code size and
        // latency, so (at least for now) we do not treat them specially.
        // The following three tests are, respectively, for Armenian, Syriac and
        // Thaana.
        assertThat(ScriptDetector.getScript("Ձ"), is(ScriptDetector.Script.kScriptOtherUtf8TwoBytes));
        assertThat(ScriptDetector.getScript("ܔ"), is(ScriptDetector.Script.kScriptOtherUtf8TwoBytes));
        assertThat(ScriptDetector.getScript("ށ"), is(ScriptDetector.Script.kScriptOtherUtf8TwoBytes));

        // Unrecognized 3-byte script: CJK Unified Ideographs: not uniquely associated
        // with a language.
        assertThat(ScriptDetector.getScript("万"), is(ScriptDetector.Script.kScriptOtherUtf8ThreeBytes));
        assertThat(ScriptDetector.getScript("両"), is(ScriptDetector.Script.kScriptOtherUtf8ThreeBytes));

        // TODO - investigate these
        /*
        // Unrecognized 4-byte script: CJK Unified Ideographs Extension C.  Note:
        // there is a nice UTF-8 encoder / decoder at https://mothereff.in/utf-8
        assertFalse(ScriptDetector.Script.kScriptOtherUtf8FourBytes != ScriptDetector.getScript("\u00F0\u00AA\u009C\u0094"));

        // Unrecognized 4-byte script: CJK Unified Ideographs Extension E
        assertFalse(ScriptDetector.Script.kScriptOtherUtf8FourBytes != ScriptDetector.getScript("\u00F0\u00AB\u00A0\u00B5") ||
            ScriptDetector.Script.kScriptOtherUtf8FourBytes != ScriptDetector.getScript("\u00F0\u00AC\u00BA\u00A1"));
            */
    }
}
