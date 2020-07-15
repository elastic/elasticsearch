/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript.LeafFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Abstract base class for building queries based on {@link StringScriptFieldScript}.
 */
abstract class AbstractStringScriptFieldQuery extends Query {
    private final Script script;
    private final StringScriptFieldScript.LeafFactory leafFactory;
    private final String fieldName;

    AbstractStringScriptFieldQuery(Script script, LeafFactory leafFactory, String fieldName) {
        this.script = script;
        this.leafFactory = Objects.requireNonNull(leafFactory);
        this.fieldName = Objects.requireNonNull(fieldName);
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(List<String> values);

    @Override
    public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false; // scripts aren't really cacheable at this point
            }

            @Override
            public Scorer scorer(LeafReaderContext ctx) throws IOException {
                StringScriptFieldScript script = leafFactory.newInstance(ctx);
                DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() throws IOException {
                        return AbstractStringScriptFieldQuery.this.matches(script.resultsForDoc(approximation().docID()));
                    }

                    @Override
                    public float matchCost() {
                        // TODO we don't have a good way of estimating the complexity of the script so we just go with 9000
                        return 9000f;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }
        };
    }

    final Script script() {
        return script;
    }

    protected final String fieldName() {
        return fieldName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), script, fieldName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractStringScriptFieldQuery other = (AbstractStringScriptFieldQuery) obj;
        return script.equals(other.script) && fieldName.equals(other.fieldName);
    }
}
