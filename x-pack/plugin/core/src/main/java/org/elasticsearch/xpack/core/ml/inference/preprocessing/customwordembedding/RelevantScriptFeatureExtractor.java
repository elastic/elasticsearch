/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.apache.lucene.util.Counter;

import java.util.Map;
import java.util.TreeMap;

/**
 * Derived from: https://github.com/google/cld3/blob/master/src/relevant_script_feature.cc
 *
 * This extracts an array of {@link FeatureValue} from the given text.
 *
 * These values contain the particular script id for each code point (determined via {@link ScriptDetector})
 * and their average occurrence in the text.
 *
 */
public class RelevantScriptFeatureExtractor implements FeatureExtractor {

    @Override
    public FeatureValue[] extractFeatures(String text) {
        if (text.isEmpty()) {
            return new FeatureValue[0];
        }

        // counts[s] is the number of characters with script s.
        // Use treemap so results are sorted in scriptid order
        final Counter totalCount = Counter.newCounter();
        TreeMap<ScriptDetector.Script, Counter> counts = new TreeMap<>();

        text.codePoints().forEach(cp -> {
            // Get anything that is a letter, or anything complex enough warranting a check (more than one UTF-8 byte).
            // cp > Byte.MAX_VALUE works as the first 127 codepoints are the same as the ASCII encoding,
            // which is the same as one UTF-8 byte.
            if(Character.isLetter(cp) || cp > Byte.MAX_VALUE) {
                ScriptDetector.Script script = ScriptDetector.Script.fromCodePoint(cp);
                counts.computeIfAbsent(script, (s) -> Counter.newCounter()).addAndGet(1);
                totalCount.addAndGet(1L);
            }
        });

        FeatureValue[] result = new FeatureValue[counts.size()];
        int index = 0;

        for (Map.Entry<ScriptDetector.Script, Counter> entry : counts.entrySet()) {
            ScriptDetector.Script scriptId = entry.getKey();
            long count = entry.getValue().get();
            double weight = (double) count / (double) totalCount.get();
            result[index++] = new ContinuousFeatureValue(scriptId.toInt(), weight);
        }

        return result;
    }

}
