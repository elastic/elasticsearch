/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.apache.lucene.util.Counter;

import java.util.OptionalInt;

public final class ScriptFeatureExtractor implements FeatureExtractor {

    private static ULScript getScriptIdOfFirstLetter(String text) {
        OptionalInt optionalCp = text.codePoints().filter(Character::isLetter).findFirst();
        if (optionalCp.isPresent() == false) {
            return ULScript.ULScript_Common;
        }
        Character.UnicodeScript unicodeScript = Character.UnicodeScript.of(optionalCp.getAsInt());
        return ULScript.unicodeScriptToULScript(unicodeScript);
    }

    // Get script feature value for the string
    static int getScriptFeatureValue(String text) {
        ULScript ulScript = getScriptIdOfFirstLetter(text);
        if (ulScript != ULScript.ULScript_Hani) {
            return ulScript.toInt();
        }
        // Out of the codepoints captured by ULScript_Hani, separately count those
        // in Hangul (Korean script) and those in a script other than Hangul.
        Counter hangulCounter = Counter.newCounter();
        Counter nonHangulCounter = Counter.newCounter();
        // TODO this assumes whole string is same characterset
        text.codePoints().forEach(cp -> {
            if (Character.isSpaceChar(cp)) {
                return;
            }
            if (Character.UnicodeScript.of(cp).equals(Character.UnicodeScript.HANGUL)) {
                hangulCounter.addAndGet(1);
            } else {
                nonHangulCounter.addAndGet(1);
            }
        });

        return hangulCounter.get() > nonHangulCounter.get() ? ULScript.NUM_ULSCRIPTS.toInt() : ULScript.ULScript_Hani.toInt();
    }

    @Override
    public FeatureValue[] extractFeatures(String text) {
        return new FeatureValue[] { new DiscreteFeatureValue(getScriptFeatureValue(text)) };
    }
}
