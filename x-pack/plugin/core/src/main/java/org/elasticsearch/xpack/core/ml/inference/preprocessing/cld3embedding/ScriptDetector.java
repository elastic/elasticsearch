/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import java.nio.charset.StandardCharsets;

import static java.lang.Character.UnicodeBlock.ARABIC;
import static java.lang.Character.UnicodeBlock.CYRILLIC;
import static java.lang.Character.UnicodeBlock.GREEK;
import static java.lang.Character.UnicodeBlock.HANGUL_JAMO;
import static java.lang.Character.UnicodeBlock.HEBREW;
import static java.lang.Character.UnicodeBlock.HIRAGANA;
import static java.lang.Character.UnicodeBlock.KATAKANA;

/**
 * Derived from https://github.com/google/cld3/blob/master/src/script_detector.h
 */
public final class ScriptDetector {

    private ScriptDetector() { }

    // Unicode scripts we care about.  To get compact and fast code, we detect only
    // a few Unicode scripts that offer a strong indication about the language of
    // the text (e.g., Hiragana -> Japanese).
    public enum Script {
        // Special value to indicate internal errors in the script detection code.
        kScriptError(0),

        // Special values for all Unicode scripts that we do not detect.  One special
        // value for Unicode characters of 1, 2, 3, respectively 4 bytes (as we
        // already have that information, we use it).  kScriptOtherUtf8OneByte means
        // ~Latin and kScriptOtherUtf8FourBytes means ~Han.
        kScriptOtherUtf8OneByte(1),
        kScriptOtherUtf8TwoBytes(2),
        kScriptOtherUtf8ThreeBytes(3),
        kScriptOtherUtf8FourBytes(4),

        kScriptGreek(5),
        kScriptCyrillic(6),
        kScriptHebrew(7),
        kScriptArabic(8),
        kScriptHangulJamo(9),  // Used primarily for Korean.
        kScriptHiragana(10),    // Used primarily for Japanese.
        kScriptKatakana(11),    // Used primarily for Japanese.

        // Add new scripts here.

        // Do not add any script after kNumRelevantScripts.  This value indicates the
        // number of elements in this enum Script (except this value) such that we can
        // easily iterate over the scripts.
        kNumRelevantScripts(12);

        private final int code;

        Script(int code) {
            this.code = code;
        }

        public int toInt() {
            return code;
        }

        public static Script fromCodePoint(int codePoint) {
            // Using blocks for the HANGUL vs HANGUL_JANO distinctions
            // If one exists. Needs investigated
            Character.UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
            if (block.equals(GREEK)) {
                return kScriptGreek;
            }
            if (block.equals(CYRILLIC)) {
                return kScriptCyrillic;
            }
            if (block.equals(ARABIC)) {
                return kScriptArabic;
            }
            if (block.equals(HEBREW)) {
                return kScriptHebrew;
            }
            if (block.equals(KATAKANA)) {
                return kScriptKatakana;
            }
            if (block.equals(HIRAGANA)) {
                return kScriptHiragana;
            }
            if (block.equals(HANGUL_JAMO)) {
                return kScriptHangulJamo;
            }

            // Not one of our special cases, need to determine the utf8 byte size
            String str = new String(Character.toChars(codePoint));
            byte[] utf8Bytes = str.getBytes(StandardCharsets.UTF_8);
            switch (utf8Bytes.length) {
                case 1:
                    return kScriptOtherUtf8OneByte;
                case 2:
                    return kScriptOtherUtf8TwoBytes;
                case 3:
                    return kScriptOtherUtf8ThreeBytes;
                case 4:
                    return kScriptOtherUtf8FourBytes;
                default:
                    return kScriptError;
            }
        }
    }
}
