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
import org.elasticsearch.script.AbstractFloatSearchScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ScriptScoreFunctionTests extends ElasticsearchTestCase {

    /**
     * Tests https://github.com/elasticsearch/elasticsearch/issues/2426
     */
    @Test
    public void testScriptScoresReturnsNaN() throws IOException {
        ScoreFunction scoreFunction = new ScriptScoreFunction("Float.NaN", null, new FloatValueScript(Float.NaN));
        LeafScoreFunction leafScoreFunction = scoreFunction.getLeafScoreFunction(null);
        try {
            leafScoreFunction.score(randomInt(), randomFloat());
            fail("should have thrown an exception about the script_score returning NaN");
        } catch (ScriptException e) {
            assertThat("message contains error about script_score returning NaN: " + e.getMessage(),
                    e.getMessage().contains("NaN"), equalTo(true));
        }
    }

    static class FloatValueScript implements SearchScript {

        private final float value;

        FloatValueScript(float value) {
            this.value = value;
        }

        @Override
        public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
            return new AbstractFloatSearchScript() {

                @Override
                public float runAsFloat() {
                    return value;
                }

                @Override
                public void setDocument(int doc) {
                    // nothing here
                }
            };
        }
    }
}
