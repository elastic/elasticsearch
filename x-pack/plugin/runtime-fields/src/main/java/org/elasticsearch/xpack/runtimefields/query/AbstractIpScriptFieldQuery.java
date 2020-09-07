/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.IpScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

/**
 * Abstract base class for building queries based on {@link StringScriptFieldScript}.
 */
abstract class AbstractIpScriptFieldQuery extends AbstractScriptFieldQuery {
    private final IpScriptFieldScript.LeafFactory leafFactory;

    AbstractIpScriptFieldQuery(Script script, IpScriptFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName);
        this.leafFactory = Objects.requireNonNull(leafFactory);
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(BytesRef[] values, int conut);

    @Override
    public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false; // scripts aren't really cacheable at this point
            }

            @Override
            public Scorer scorer(LeafReaderContext ctx) throws IOException {
                IpScriptFieldScript script = leafFactory.newInstance(ctx);
                DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() throws IOException {
                        script.runForDoc(approximation().docID());
                        return AbstractIpScriptFieldQuery.this.matches(script.values(), script.count());
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

    protected static InetAddress decode(BytesRef ref) {
        return InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(ref)));
    }
}
