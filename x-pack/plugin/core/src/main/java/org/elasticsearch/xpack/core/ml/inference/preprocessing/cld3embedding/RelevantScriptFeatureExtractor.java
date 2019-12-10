/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

public class RelevantScriptFeatureExtractor implements FeatureExtractor {

    @Override
    public FeatureValue[] extractFeatures(String text) {
        try {
            if (text.isEmpty()) {
                return new FeatureValue[0];
            }

            // counts[s] is the number of characters with script s.
            // Use treemap so results are sorted in scriptid order
            TreeMap<ScriptDetector.Script, Integer> counts = new TreeMap<>();

            int totalCount = 0;

            for (int i = 1; i <= text.length(); ++i) {
                String curr = text.substring(i - 1, i);
                byte[] bytes = text.substring(i - 1, i).getBytes("UTF8");
                int numBytes = bytes.length;

                // Skip spaces, numbers, punctuation, and all other non-alpha ASCII
                // characters: these characters are used in so many languages, they do not
                // communicate language-related information.
                // TODO - check whether we need to look at mark
                if ((numBytes == 1) && !curr.chars().allMatch(Character::isLetter)) {
                    continue;
                }

                ScriptDetector.Script script = ScriptDetector.getScript(curr);
                counts.put(script, counts.getOrDefault(script, 0) + 1);

                totalCount++;
            }

            FeatureValue[] result = new FeatureValue[counts.size()];
            int index = 0;

            for (Map.Entry<ScriptDetector.Script, Integer> entry : counts.entrySet()) {
                ScriptDetector.Script scriptId = entry.getKey();
                int count = entry.getValue();
                if (count > 0) {
                    double weight = (double) count / (double) totalCount;
                    result[index++] = new ContinuousFeatureValue(scriptId.toInt(), weight);
                }
            }

            return result;
        } catch (UnsupportedEncodingException ex) {
            throw ExceptionsHelper.serverError("Unexpected failure while extracting relevant script information", ex);
        }
    }

}
