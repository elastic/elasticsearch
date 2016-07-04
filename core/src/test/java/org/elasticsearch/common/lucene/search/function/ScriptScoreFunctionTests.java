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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.AbstractDoubleSearchScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ScriptScoreFunctionTests extends ESTestCase {
    /**
     * Tests https://github.com/elastic/elasticsearch/issues/2426
     */
    public void testScriptScoresReturnsNaN() throws IOException {
        // script that always returns NaN
        ScoreFunction scoreFunction = new ScriptScoreFunction(new Script("Double.NaN"), new SearchScript() {
            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                return new AbstractDoubleSearchScript() {
                    @Override
                    public double runAsDouble() {
                        return Double.NaN;
                    }

                    @Override
                    public void setDocument(int doc) {
                        // do nothing: we are a fake with no lookup
                    }
                };
            }
            
            @Override
            public boolean needsScores() {
                return false;
            }
        });
        LeafScoreFunction leafScoreFunction = scoreFunction.getLeafScoreFunction(null);
        GeneralScriptException expected = expectThrows(GeneralScriptException.class, () -> {
            leafScoreFunction.score(randomInt(), randomFloat());
        });
        assertTrue(expected.getMessage().contains("returned NaN"));
    }
}
