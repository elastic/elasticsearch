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

public class LaplaceModelTests extends SmoothingModelTestCase {

    @Override
    protected SmoothingModel createTestModel() {
        return createRandomModel();
    }


    static SmoothingModel createRandomModel() {
        return new Laplace(randomDoubleBetween(0.0, 10.0, false));
    }

    /**
     * mutate the given model so the returned smoothing model is different
     */
    @Override
    protected Laplace createMutation(SmoothingModel input) {
        Laplace original = (Laplace) input;
        return new Laplace(original.getAlpha() + 0.1);
    }

    @Override
    void assertWordScorer(WordScorer wordScorer, SmoothingModel input) {
        Laplace model = (Laplace) input;
        assertThat(wordScorer, instanceOf(LaplaceScorer.class));
        assertEquals(model.getAlpha(), ((LaplaceScorer) wordScorer).alpha(), Double.MIN_VALUE);
    }

    @Override
    protected SmoothingModel fromXContent(QueryParseContext context) throws IOException {
        return Laplace.innerFromXContent(context);
    }
}
