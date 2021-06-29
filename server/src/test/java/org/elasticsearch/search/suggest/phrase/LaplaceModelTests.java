/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.common.xcontent.XContentParser;

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
    protected SmoothingModel fromXContent(XContentParser parser) throws IOException {
        return Laplace.fromXContent(parser);
    }
}
