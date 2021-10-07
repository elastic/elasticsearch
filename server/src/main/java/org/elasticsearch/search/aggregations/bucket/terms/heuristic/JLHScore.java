/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class JLHScore extends SignificanceHeuristic {
    public static final String NAME = "jlh";
    public static final ObjectParser<JLHScore, Void> PARSER = new ObjectParser<>(NAME, JLHScore::new);

    public JLHScore() {}

    /**
     * Read from a stream.
     */
    public JLHScore(StreamInput in) {
        // Nothing to read.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Calculates the significance of a term in a sample against a background of
     * normal distributions by comparing the changes in frequency. This is the heart
     * of the significant terms feature.
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, "JLHScore");
        if ((subsetSize == 0) || (supersetSize == 0)) {
            // avoid any divide by zero issues
            return 0;
        }
        if (supersetFreq == 0) {
            // If we are using a background context that is not a strict superset, a foreground
            // term may be missing from the background, so for the purposes of this calculation
            // we assume a value of 1 for our calculations which avoids returning an "infinity" result
            supersetFreq = 1;
        }
        double subsetProbability = (double) subsetFreq / (double) subsetSize;
        double supersetProbability = (double) supersetFreq / (double) supersetSize;

        // Using absoluteProbabilityChange alone favours very common words e.g. you, we etc
        // because a doubling in popularity of a common term is a big percent difference
        // whereas a rare term would have to achieve a hundred-fold increase in popularity to
        // achieve the same difference measure.
        // In favouring common words as suggested features for search we would get high
        // recall but low precision.
        double absoluteProbabilityChange = subsetProbability - supersetProbability;
        if (absoluteProbabilityChange <= 0) {
            return 0;
        }
        // Using relativeProbabilityChange tends to favour rarer terms e.g.mis-spellings or
        // unique URLs.
        // A very low-probability term can very easily double in popularity due to the low
        // numbers required to do so whereas a high-probability term would have to add many
        // extra individual sightings to achieve the same shift.
        // In favouring rare words as suggested features for search we would get high
        // precision but low recall.
        double relativeProbabilityChange = (subsetProbability / supersetProbability);

        // A blend of the above metrics - favours medium-rare terms to strike a useful
        // balance between precision and recall.
        return absoluteProbabilityChange * relativeProbabilityChange;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    public static class JLHScoreBuilder implements SignificanceHeuristicBuilder {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME).endObject();
            return builder;
        }
    }
}
