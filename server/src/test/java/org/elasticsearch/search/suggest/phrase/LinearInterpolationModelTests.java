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

public class LinearInterpolationModelTests extends SmoothingModelTestCase {

    @Override
    protected SmoothingModel createTestModel() {
        return createRandomModel();
    }

    static LinearInterpolation createRandomModel() {
        double trigramLambda = randomDoubleBetween(0.0, 10.0, false);
        double bigramLambda = randomDoubleBetween(0.0, 10.0, false);
        double unigramLambda = randomDoubleBetween(0.0, 10.0, false);
        // normalize so parameters sum to 1
        double sum = trigramLambda + bigramLambda + unigramLambda;
        return new LinearInterpolation(trigramLambda / sum, bigramLambda / sum, unigramLambda / sum);
    }

    /**
     * mutate the given model so the returned smoothing model is different
     */
    @Override
    protected LinearInterpolation createMutation(SmoothingModel input) {
        LinearInterpolation original = (LinearInterpolation) input;
        // swap two values permute original lambda values
        switch (randomIntBetween(0, 2)) {
        case 0:
            // swap first two
            return new LinearInterpolation(original.getBigramLambda(), original.getTrigramLambda(), original.getUnigramLambda());
        case 1:
            // swap last two
            return new LinearInterpolation(original.getTrigramLambda(), original.getUnigramLambda(), original.getBigramLambda());
        case 2:
        default:
            // swap first and last
            return new LinearInterpolation(original.getUnigramLambda(), original.getBigramLambda(), original.getTrigramLambda());
        }
    }

    @Override
    void assertWordScorer(WordScorer wordScorer, SmoothingModel in) {
        LinearInterpolation testModel = (LinearInterpolation) in;
        LinearInterpolatingScorer testScorer = (LinearInterpolatingScorer) wordScorer;
        assertThat(wordScorer, instanceOf(LinearInterpolatingScorer.class));
        assertEquals(testModel.getTrigramLambda(), (testScorer).trigramLambda(), 1e-15);
        assertEquals(testModel.getBigramLambda(), (testScorer).bigramLambda(), 1e-15);
        assertEquals(testModel.getUnigramLambda(), (testScorer).unigramLambda(), 1e-15);
    }

    @Override
    protected SmoothingModel fromXContent(XContentParser parser) throws IOException {
        return LinearInterpolation.fromXContent(parser);
    }
}
