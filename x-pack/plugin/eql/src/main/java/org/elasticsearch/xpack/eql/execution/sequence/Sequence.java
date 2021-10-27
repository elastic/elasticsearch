/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
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
public class Sequence implements Comparable<Sequence>, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Sequence.class);

    private final SequenceKey key;
    private final int stages;
    private final Match[] matches;

    private int currentStage = 0;

    public Sequence(SequenceKey key, int stages, Ordinal ordinal, HitReference firstHit) {
        Check.isTrue(stages >= 2, "A sequence requires at least 2 criteria, given [{}]", stages);
        this.key = key;
        this.stages = stages;
        this.matches = new Match[stages];
        this.matches[0] = new Match(ordinal, firstHit);
    }

    public void putMatch(int stage, Ordinal ordinal, HitReference hit) {
        if (stage == currentStage + 1) {
            currentStage = stage;
            matches[currentStage] = new Match(ordinal, hit);
        } else {
            throw new EqlIllegalArgumentException("Invalid stage [{}] specified for sequence[key={}, stage={}]", stage, key, currentStage);
        }
    }

    public SequenceKey key() {
        return key;
    }

    public Ordinal ordinal() {
        return matches[currentStage].ordinal();
    }

    public Ordinal startOrdinal() {
        return matches[0].ordinal();
    }

    public List<HitReference> hits() {
        List<HitReference> hits = new ArrayList<>(matches.length);
        for (Match m : matches) {
            hits.add(m.hit());
        }
        return hits;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(matches);
    }

    @Override
    public int compareTo(Sequence o) {
        return ordinal().compareTo(o.ordinal());
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
        return Objects.equals(currentStage, other.currentStage) && Objects.equals(key, other.key);
    }

    @Override
    public String toString() {
        int numberOfDigits = stages > 100 ? 3 : stages > 10 ? 2 : 1;
        NumberFormat nf = NumberFormat.getIntegerInstance(Locale.ROOT);
        nf.setMinimumIntegerDigits(numberOfDigits);

        StringBuilder sb = new StringBuilder();
        sb.append(format(null, "[Seq<{}>[{}/{}]]", key, nf.format(currentStage), nf.format(stages - 1)));

        for (int i = 0; i < matches.length; i++) {
            sb.append(format(null, "\n [{}]={{}}", nf.format(i), matches[i]));
        }

        return sb.toString();
    }
}
