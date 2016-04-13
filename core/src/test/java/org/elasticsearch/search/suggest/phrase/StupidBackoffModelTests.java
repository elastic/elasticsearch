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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class StupidBackoffModelTests extends SmoothingModelTestCase {

    @Override
    protected SmoothingModel createTestModel() {
        return createRandomModel();
    }

    static SmoothingModel createRandomModel() {
        return new StupidBackoff(randomDoubleBetween(0.0, 10.0, false));
    }

    /**
     * mutate the given model so the returned smoothing model is different
     */
    @Override
    protected StupidBackoff createMutation(SmoothingModel input) {
        StupidBackoff original = (StupidBackoff) input;
        return new StupidBackoff(original.getDiscount() + 0.1);
    }

    @Override
    void assertWordScorer(WordScorer wordScorer, SmoothingModel input) {
        assertThat(wordScorer, instanceOf(StupidBackoffScorer.class));
        StupidBackoff testModel = (StupidBackoff) input;
        assertEquals(testModel.getDiscount(), ((StupidBackoffScorer) wordScorer).discount(), Double.MIN_VALUE);
    }

    @Override
    protected SmoothingModel fromXContent(QueryParseContext context) throws IOException {
        return StupidBackoff.innerFromXContent(context);
    }
}
