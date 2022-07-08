/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
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
        assertFalse(needsScores(termsBuilder().field("f")));
    }

    public void testSubTerms() throws IOException {
        assertFalse(needsScores(termsBuilder().field("f").subAggregation(new TermsAggregationBuilder("i").field("f"))));
    }

    public void testScoreConsumingScript() throws IOException {
        assertFalse(needsScores(termsBuilder().script(new Script("no_scores"))));
    }

    public void testNonScoreConsumingScript() throws IOException {
        assertTrue(needsScores(termsBuilder().script(new Script("with_scores"))));
    }

    public void testSubScoreConsumingScript() throws IOException {
        assertFalse(needsScores(termsBuilder().field("f").subAggregation(termsBuilder().script(new Script("no_scores")))));
    }

    public void testSubNonScoreConsumingScript() throws IOException {
        assertTrue(needsScores(termsBuilder().field("f").subAggregation(termsBuilder().script(new Script("with_scores")))));
    }

    public void testTopHits() throws IOException {
        assertTrue(needsScores(new TopHitsAggregationBuilder("h")));
    }

    public void testSubTopHits() throws IOException {
        assertTrue(needsScores(termsBuilder().field("f").subAggregation(new TopHitsAggregationBuilder("h"))));
    }

    private TermsAggregationBuilder termsBuilder() {
        return new TermsAggregationBuilder("t");
    }

    private boolean needsScores(AggregationBuilder builder) throws IOException {
        try (
            Directory directory = newDirectory();
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            DirectoryReader reader = indexWriter.getReader()
        ) {
            return createAggregator(builder, new IndexSearcher(reader), new KeywordFieldType("f")).scoreMode().needsScores();
        }
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
