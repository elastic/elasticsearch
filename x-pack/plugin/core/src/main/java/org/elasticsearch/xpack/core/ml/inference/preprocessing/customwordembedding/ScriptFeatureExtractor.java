/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.apache.lucene.util.Counter;

import java.util.OptionalInt;

/**
 * This is the lone discrete feature.
 *
 * It returns an array of {@link FeatureValue} with length 1.
 *
 * The feature has a non-continuous weight (it is always 1.0). The "id" is the related {@link ScriptCode} for the first letter
 * Java codepoint.
 *
 * The exception to this is if {@link ScriptCode#Hani} is returned. In that case the number of {@link Character.UnicodeScript#HANGUL}
 * characters are counted. If more exist than not, {@link ScriptCode#MAX_SCRIPT_CODE} is returned.
 * Otherwise its {@link ScriptCode#Hani}
 */
public final class ScriptFeatureExtractor implements FeatureExtractor {

    private static ScriptCode getScriptIdOfFirstLetter(String text) {
        OptionalInt optionalCp = text.codePoints().filter(Character::isLetter).findFirst();
        if (optionalCp.isPresent() == false) {
            return ScriptCode.Common;
        }
        Character.UnicodeScript unicodeScript = Character.UnicodeScript.of(optionalCp.getAsInt());
        return ScriptCode.unicodeScriptToULScript(unicodeScript);
    }

    // Get script feature value for the string
    static int getScriptFeatureValue(String text) {
        ScriptCode scriptCode = getScriptIdOfFirstLetter(text);
        if (scriptCode != ScriptCode.Hani) {
            return scriptCode.toInt();
        }
        // Out of the codepoints captured by ULScript_Hani, separately count those
        // in Hangul (Korean script) and those in a script other than Hangul.
        Counter hangulCounter = Counter.newCounter();
        Counter nonHangulCounter = Counter.newCounter();
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

        return hangulCounter.get() > nonHangulCounter.get() ? ScriptCode.MAX_SCRIPT_CODE.toInt() : ScriptCode.Hani.toInt();
    }

    @Override
    public FeatureValue[] extractFeatures(String text) {
        return new FeatureValue[] { new DiscreteFeatureValue(getScriptFeatureValue(text)) };
    }
}
