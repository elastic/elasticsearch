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
import org.elasticsearch.xpack.runtimefields.mapper.AbstractFieldScript;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Abstract base class for building queries based on script fields.
 */
abstract class AbstractScriptFieldQuery<S extends AbstractFieldScript> extends Query {
    /**
     * We don't have the infrastructure to estimate the match cost of a script
     * so we just use a big number.
     */
    protected static final float MATCH_COST = 9000f;

    private final Script script;
    private final String fieldName;
    private final Function<LeafReaderContext, S> scriptContextFunction;

    AbstractScriptFieldQuery(Script script, String fieldName, Function<LeafReaderContext, S> scriptContextFunction) {
        this.script = Objects.requireNonNull(script);
        this.fieldName = Objects.requireNonNull(fieldName);
        this.scriptContextFunction = scriptContextFunction;
    }

    final Function<LeafReaderContext, S> scriptContextFunction() {
        return scriptContextFunction;
    }

    final Script script() {
        return script;
    }

    final String fieldName() {
        return fieldName;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false; // scripts aren't really cacheable at this point
            }

            @Override
            public Scorer scorer(LeafReaderContext ctx) {
                S scriptContext = scriptContextFunction.apply(ctx);
                DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() {
                        return AbstractScriptFieldQuery.this.matches(scriptContext, approximation.docID());
                    }

                    @Override
                    public float matchCost() {
                        return MATCH_COST;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }
        };
    }

    protected abstract boolean matches(S scriptContext, int docId);

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), script, fieldName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractScriptFieldQuery<?> other = (AbstractScriptFieldQuery<?>) obj;
        return script.equals(other.script) && fieldName.equals(other.fieldName);
    }
}
