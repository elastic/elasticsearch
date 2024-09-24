/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Abstract base class for building queries based on script fields.
 */
public abstract class AbstractScriptFieldQuery<S extends AbstractFieldScript> extends Query {
    /**
     * We don't have the infrastructure to estimate the match cost of a script
     * so we just use a big number.
     */
    protected static final float MATCH_COST = 9000f;

    private final Script script;
    private final String fieldName;
    private final Function<LeafReaderContext, S> scriptContextFunction;

    protected AbstractScriptFieldQuery(Script script, String fieldName, Function<LeafReaderContext, S> scriptContextFunction) {
        this.script = Objects.requireNonNull(script);
        this.fieldName = Objects.requireNonNull(fieldName);
        this.scriptContextFunction = scriptContextFunction;
    }

    final Function<LeafReaderContext, S> scriptContextFunction() {
        return scriptContextFunction;
    }

    public final Script script() {
        return script;
    }

    public final String fieldName() {
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
            public ScorerSupplier scorerSupplier(LeafReaderContext ctx) throws IOException {
                S scriptContext = scriptContextFunction.apply(ctx);
                DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
                Scorer scorer = new ConstantScoreScorer(score(), scoreMode, createTwoPhaseIterator(scriptContext, approximation));
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                Explanation constantExplanation = super.explain(context, doc);
                if (constantExplanation.isMatch()) {
                    return explainMatch(boost, constantExplanation.getDescription());
                }
                return constantExplanation;
            }
        };
    }

    /**
     * Creates the {@link TwoPhaseIterator} for the current leaf reader. Override to inject custom behaviour or provide
     * additional context to the matches method when needed.
     */
    protected TwoPhaseIterator createTwoPhaseIterator(S scriptContext, DocIdSetIterator approximation) {
        return new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() {
                return AbstractScriptFieldQuery.this.matches(scriptContext, approximation.docID());
            }

            @Override
            public float matchCost() {
                return MATCH_COST;
            }
        };
    }

    protected abstract boolean matches(S scriptContext, int docId);

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), script, fieldName);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        AbstractScriptFieldQuery<?> other = (AbstractScriptFieldQuery<?>) obj;
        return script.equals(other.script) && fieldName.equals(other.fieldName);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    static Explanation explainMatch(float boost, String description) {
        return Explanation.match(
            boost,
            description,
            Explanation.match(
                boost,
                "boost * runtime_field_score",
                Explanation.match(boost, "boost"),
                Explanation.match(1.0, "runtime_field_score is always 1")
            )
        );
    }
}
