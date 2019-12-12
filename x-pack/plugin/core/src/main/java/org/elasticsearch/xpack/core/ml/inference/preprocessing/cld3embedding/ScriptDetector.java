/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

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
    }

    // Returns Script for the UTF8 character that is the first UTF8 character in p
    // TODO - investigate if java String codepoints could be used whilst retaining same
    // TODO - feature space as cld3
    public static Script getScript(String p) {
        if (p.isEmpty()){
            return Script.kScriptError;
        }

        int numBytes;
        byte[] bytes;

        // TODO Java is UTF16 So, if we actually care about accuracy here we need to dig further
        // Derived from: https://github.com/google/cld3/blob/484afe9ba7438d078e60b3a26e7fb590213c0e17/src/utils.cc#L228
        bytes = p.substring(0, 1).getBytes(StandardCharsets.UTF_8);
        numBytes = bytes.length;

        switch (numBytes) {
            case 1:
                return Script.kScriptOtherUtf8OneByte;

            case 2: {
                // 2-byte UTF8 characters have 11 bits of information.  unsigned int has
                // at least 16 bits (http://en.cppreference.com/w/cpp/language/types) so
                // it's enough.  It's also usually the fastest int type on the current
                // CPU, so it's better to use than int32.
                final int kGreekStart = 0x370;

                // Commented out (unsued in the code): kGreekEnd = 0x3FF;
                final int kCyrillicStart = 0x400;
                final int kCyrillicEnd = 0x4FF;
                final int kHebrewStart = 0x590;

                // Commented out (unsued in the code): kHebrewEnd = 0x5FF;
                final int kArabicStart = 0x600;
                final int kArabicEnd = 0x6FF;
                int codepoint = ((bytes[0] & 0x1F) << 6) | (bytes[1] & 0x3F);
                if (codepoint > kCyrillicEnd) {
                    if (codepoint >= kArabicStart) {
                        if (codepoint <= kArabicEnd) {
                            return Script.kScriptArabic;
                        }
                    } else {
                        // At this point, codepoint < kArabicStart = kHebrewEnd + 1, so
                        // codepoint <= kHebrewEnd.
                        if (codepoint >= kHebrewStart) {
                            return Script.kScriptHebrew;
                        }
                    }
                } else {
                    if (codepoint >= kCyrillicStart) {
                        return Script.kScriptCyrillic;
                    } else {
                        // At this point, codepoint < kCyrillicStart = kGreekEnd + 1, so
                        // codepoint <= kGreekEnd.
                        if (codepoint >= kGreekStart) {
                            return Script.kScriptGreek;
                        }
                    }
                }
                return Script.kScriptOtherUtf8TwoBytes;
            }

            case 3: {
                // 3-byte UTF8 characters have 16 bits of information.  unsigned int has
                // at least 16 bits.
                final int kHangulJamoStart = 0x1100;
                final int kHangulJamoEnd = 0x11FF;
                final int kHiraganaStart = 0x3041;
                final int kHiraganaEnd = 0x309F;

                // Commented out (unsued in the code): kKatakanaStart = 0x30A0;
                final int kKatakanaEnd = 0x30FF;
                int codepoint =
                    ((bytes[0] & 0x0F) << 12) | ((bytes[1] & 0x3F) << 6) | (bytes[2] & 0x3F);
                if (codepoint > kHiraganaEnd) {
                    // On this branch, codepoint > kHiraganaEnd = kKatakanaStart - 1, so
                    // codepoint >= kKatakanaStart.
                    if (codepoint <= kKatakanaEnd) {
                        return Script.kScriptKatakana;
                    }
                } else {
                    if (codepoint >= kHiraganaStart) {
                        return Script.kScriptHiragana;
                    } else {
                        if (InRange(codepoint, kHangulJamoStart, kHangulJamoEnd)) {
                            return Script.kScriptHangulJamo;
                        }
                    }
                }
                return Script.kScriptOtherUtf8ThreeBytes;
            }

            case 4:
                return Script.kScriptOtherUtf8FourBytes;

            default:
                return Script.kScriptError;
        }
    }

    private static boolean InRange(int value, int low, int hi) {
        return (value >= low) && (value <= hi);
    }
}
