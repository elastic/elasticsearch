package org.apache.lucene.index;

import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.SimilarityBase;

public class FreqIsScoreSimilarity extends SimilarityBase {
    @Override
    protected double score(BasicStats stats, double freq, double docLen) {
        return freq;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
