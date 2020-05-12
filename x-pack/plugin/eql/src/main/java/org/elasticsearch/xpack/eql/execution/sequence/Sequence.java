/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.util.Check;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Holder class representing the instance of a sequence. Used at runtime by the engine to track sequences.
 * Defined by its key and stage.
 * This class is NOT immutable (to optimize memory) which means its associations need to be managed.
 */
public class Sequence {

    private final SequenceKey key;
    private final int stages;
    private final Match[] matches;

    private int currentStage = 0;

    public Sequence(SequenceKey key, int stages, long timestamp, SearchHit firstHit) {
        Check.isTrue(stages >= 2, "A sequence requires at least 2 criteria, given [{}]", stages);
        this.key = key;
        this.stages = stages;
        this.matches = new Match[stages];
        this.matches[0] = new Match(timestamp, firstHit);
    }

    public int putMatch(int stage, SearchHit hit, long timestamp) {
        if (stage == currentStage + 1) {
            int previousStage = currentStage;
            currentStage = stage;
            matches[currentStage] = new Match(timestamp, hit);
            return previousStage;
        }
        throw new EqlIllegalArgumentException("Incorrect stage [{}] specified for Sequence[key={}, stage=]", stage, key, currentStage);
    }

    public SequenceKey key() {
        return key;
    }

    public int currentStage() {
        return currentStage;
    }

    public long currentTimestamp() {
        return matches[currentStage].timestamp();
    }

    public long timestamp(int stage) {
        // stages not initialized yet return an out-of-band value to have no impact on the interval range
        if (stage > currentStage) {
            return Long.MAX_VALUE;
        }
        return matches[stage].timestamp();
    }

    public List<SearchHit> hits() {
        List<SearchHit> hits = new ArrayList<>(matches.length);
        for (Match m : matches) {
            hits.add(m.hit());
        }
        return hits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentStage, key);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        Sequence other = (Sequence) obj;
        return Objects.equals(currentStage, other.currentStage)
                && Objects.equals(key, other.key);
    }
    
    @Override
    public String toString() {
        int numberOfDigits = stages > 100 ? 3 : stages > 10 ? 2 : 1;
        NumberFormat nf = NumberFormat.getIntegerInstance(Locale.ROOT);
        nf.setMinimumIntegerDigits(numberOfDigits);

        StringBuilder sb = new StringBuilder();
        sb.append(format(null, "[Seq<{}>[{}/{}]]",
                key,
                nf.format(currentStage()),
                nf.format(stages - 1)));

        for (int i = 0; i < matches.length; i++) {
            sb.append(format(null, "\n [{}]={{}}", nf.format(i), matches[i]));
        }
        
        return sb.toString();
    }
}