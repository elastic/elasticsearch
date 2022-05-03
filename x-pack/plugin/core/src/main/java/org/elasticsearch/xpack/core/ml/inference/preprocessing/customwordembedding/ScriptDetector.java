/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import static java.lang.Character.UnicodeBlock.ARABIC;
import static java.lang.Character.UnicodeBlock.CYRILLIC;
import static java.lang.Character.UnicodeBlock.GREEK;
import static java.lang.Character.UnicodeBlock.HANGUL_JAMO;
import static java.lang.Character.UnicodeBlock.HEBREW;
import static java.lang.Character.UnicodeBlock.HIRAGANA;
import static java.lang.Character.UnicodeBlock.KATAKANA;

/**
 * Derived from https://github.com/google/cld3/blob/master/src/script_detector.h
 *
 * We take advantage of Java codepoints to determine the specific script value we care about
 */
public final class ScriptDetector {

    private ScriptDetector() {}

    // Unicode scripts we care about. To get compact and fast code, we detect only
    // a few Unicode scripts that offer a strong indication about the language of
    // the text (e.g., Hiragana -> Japanese).
    public enum Script {
        // Special value to indicate internal errors in the script detection code.
        kScriptError(0),

        // Special values for all Unicode scripts that we do not detect. One special
        // value for Unicode characters of 1, 2, 3, respectively 4 bytes (as we
        // already have that information, we use it). kScriptOtherUtf8OneByte means
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
        kScriptKatakana(11);    // Used primarily for Japanese.

        private final int code;

        Script(int code) {
            this.code = code;
        }

        public int toInt() {
            return code;
        }

        public static Script fromCodePoint(int codePoint) {
            // Using blocks for the HANGUL vs HANGUL_JAMO distinctions
            // If one exists. Needs investigated
            Character.UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
            if (GREEK.equals(block)) {
                return kScriptGreek;
            }
            if (CYRILLIC.equals(block)) {
                return kScriptCyrillic;
            }
            if (ARABIC.equals(block)) {
                return kScriptArabic;
            }
            if (HEBREW.equals(block)) {
                return kScriptHebrew;
            }
            if (KATAKANA.equals(block)) {
                return kScriptKatakana;
            }
            if (HIRAGANA.equals(block)) {
                return kScriptHiragana;
            }
            if (HANGUL_JAMO.equals(block)) {
                return kScriptHangulJamo;
            }

            // Not one of our special cases, need to determine the utf8 byte size
            if (codePoint > 0) {
                // Fits in a single UTF-8 byte
                if (codePoint < 128) {
                    return kScriptOtherUtf8OneByte;
                }
                if (codePoint < 2048) {
                    return kScriptOtherUtf8TwoBytes;
                }
                if (codePoint < 65536) {
                    return kScriptOtherUtf8ThreeBytes;
                }
                if (codePoint < 1114112) {
                    return kScriptOtherUtf8FourBytes;
                }
            }

            return kScriptError;
        }
    }
}
