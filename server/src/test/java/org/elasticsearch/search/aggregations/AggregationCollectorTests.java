/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;

import java.io.IOException;

import static org.mockito.Matchers.any;
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
            AggregationScript.Factory factory;
            switch (script.getIdOrCode()) {
                case "no_scores":
                    factory = (params, lookup) -> new AggregationScript.LeafFactory() {
                        @Override
                        public AggregationScript newInstance(LeafReaderContext ctx) throws IOException {
                            return null;
                        }

                        @Override
                        public boolean needs_score() {
                            return false;
                        }
                    };
                    break;
                case "with_scores":
                    factory = (params, lookup) -> new AggregationScript.LeafFactory() {
                        @Override
                        public AggregationScript newInstance(LeafReaderContext ctx) throws IOException {
                            return null;
                        }

                        @Override
                        public boolean needs_score() {
                            return true;
                        }
                    };
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            return factory;
        });
        return scriptService;
    }
}
