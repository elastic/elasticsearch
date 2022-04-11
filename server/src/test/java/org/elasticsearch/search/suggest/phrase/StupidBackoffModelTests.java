/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.xcontent.XContentParser;

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
    protected SmoothingModel fromXContent(XContentParser parser) throws IOException {
        return StupidBackoff.fromXContent(parser);
    }
}
