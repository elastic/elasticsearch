/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationCollectorTests extends AggregatorTestCase {
    public void testTerms() throws IOException {
        assertNeedsScores(termsBuilder().field("f"), false);
    }

    public void testSubTerms() throws IOException {
        assertNeedsScores(termsBuilder().field("f").subAggregation(new TermsAggregationBuilder("i").field("f")), false);
    }

    public void testScoreConsumingScript() throws IOException {
        assertNeedsScores(termsBuilder().script(new Script("no_scores")), false);
    }

    public void testNonScoreConsumingScript() throws IOException {
        assertNeedsScores(termsBuilder().script(new Script("with_scores")), true);
    }

    public void testSubScoreConsumingScript() throws IOException {
        assertNeedsScores(termsBuilder().field("f").subAggregation(termsBuilder().script(new Script("no_scores"))), false);
    }

    public void testSubNonScoreConsumingScript() throws IOException {
        assertNeedsScores(termsBuilder().field("f").subAggregation(termsBuilder().script(new Script("with_scores"))), true);
    }

    public void testTopHits() throws IOException {
        assertNeedsScores(new TopHitsAggregationBuilder("h"), true);
    }

    public void testSubTopHits() throws IOException {
        assertNeedsScores(termsBuilder().field("f").subAggregation(new TopHitsAggregationBuilder("h")), true);
    }

    private TermsAggregationBuilder termsBuilder() {
        return new TermsAggregationBuilder("t");
    }

    private void assertNeedsScores(AggregationBuilder builder, boolean expected) throws IOException {
        withAggregator(
            builder,
            new MatchAllDocsQuery(),
            iw -> {},
            (indexSearcher, agg) -> assertEquals(expected, agg.scoreMode().needsScores())
        );
    }

    @Override
    protected ScriptService getMockScriptService() {
        ScriptService scriptService = mock(ScriptService.class);
        when(scriptService.compile(any(), any())).then(inv -> {
            Script script = (Script) inv.getArguments()[0];
            AggregationScript.Factory factory = switch (script.getIdOrCode()) {
                case "no_scores" -> (params, lookup) -> new AggregationScript.LeafFactory() {
                    @Override
                    public AggregationScript newInstance(LeafReaderContext ctx) throws IOException {
                        return null;
                    }

                    @Override
                    public boolean needs_score() {
                        return false;
                    }
                };
                case "with_scores" -> (params, lookup) -> new AggregationScript.LeafFactory() {
                    @Override
                    public AggregationScript newInstance(LeafReaderContext ctx) throws IOException {
                        return null;
                    }

                    @Override
                    public boolean needs_score() {
                        return true;
                    }
                };
                default -> throw new UnsupportedOperationException();
            };
            return factory;
        });
        return scriptService;
    }
}
