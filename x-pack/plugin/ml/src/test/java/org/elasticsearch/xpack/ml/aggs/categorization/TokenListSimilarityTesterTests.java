/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategory.TokenAndWeight;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TokenListSimilarityTesterTests extends CategorizationTestCase {

    public void testWeightedEditDistance() {

        // These tests give a weight of 3 to dictionary words and 1 to other tokens

        List<TokenAndWeight> sourceShutDown1 = List.of(
            tw("ml13", 1),
            tw("4608.1.p2ps", 1),
            tw("Info", 3),
            tw("Source", 3),
            tw("ML_SERVICE2", 1),
            tw("on", 3),
            tw("has", 3),
            tw("shut", 3),
            tw("down", 3)
        );
        List<TokenAndWeight> sourceShutDown2 = List.of(
            tw("ml13", 1),
            tw("4606.1.p2ps", 1),
            tw("Info", 3),
            tw("Source", 3),
            tw("MONEYBROKER", 1),
            tw("on", 3),
            tw("has", 3),
            tw("shut", 3),
            tw("down", 3)
        );
        List<TokenAndWeight> serviceStart = List.of(
            tw("ml13", 1),
            tw("4606.1.p2ps", 1),
            tw("Info", 3),
            tw("Service", 3),
            tw("ML_FEED", 1),
            tw("id", 3),
            tw("of", 3),
            tw("has", 3),
            tw("started", 3)
        );
        List<TokenAndWeight> noImageData = List.of(
            tw("ml13", 1),
            tw("4607.1.p2ps", 1),
            tw("Debug", 3),
            tw("Did", 3),
            tw("not", 3),
            tw("receive", 3),
            tw("an", 3),
            tw("image", 3),
            tw("data", 3),
            tw("for", 3),
            tw("ML_FEED", 1),
            tw("4205.T", 1),
            tw("on", 3),
            tw("Recalling", 3),
            tw("item", 3)
        );
        List<TokenAndWeight> empty = List.of();

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, sourceShutDown2), equalTo(2));
        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown2, sourceShutDown1), equalTo(2));

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, serviceStart), equalTo(17));
        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, sourceShutDown1), equalTo(17));

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown2, serviceStart), equalTo(16));
        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, sourceShutDown2), equalTo(16));

        assertThat(TokenListSimilarityTester.weightedEditDistance(noImageData, serviceStart), equalTo(36));
        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, noImageData), equalTo(36));

        assertThat(TokenListSimilarityTester.weightedEditDistance(noImageData, sourceShutDown1), equalTo(36));
        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, noImageData), equalTo(36));

        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, empty), equalTo(21));
        assertThat(TokenListSimilarityTester.weightedEditDistance(empty, serviceStart), equalTo(21));
    }

    public void testPositionalWeightedEditDistance() {

        // These tests give a weight of 3 to dictionary words and 1 to other tokens
        // along with positional weighting: x2 current weighting for first 4 tokens,
        // x1 current weighting for next 3, x0 current weighting for the rest..
        // We expect the resulting edit distance to differ from that where no positional
        // weighting is in play, but still to be symmetric regarding the order of the strings passed.

        List<TokenAndWeight> sourceShutDown1 = List.of(
            tw("ml13", 2),
            tw("4608.1.p2ps", 2),
            tw("Info", 6),
            tw("Source", 6),
            tw("ML_SERVICE2", 1),
            tw("on", 3),
            tw("has", 3),
            tw("shut", 0),
            tw("down", 0)
        );
        List<TokenAndWeight> sourceShutDown2 = List.of(
            tw("ml13", 2),
            tw("4606.1.p2ps", 2),
            tw("Info", 6),
            tw("Source", 6),
            tw("MONEYBROKER", 1),
            tw("on", 3),
            tw("has", 3),
            tw("shut", 0),
            tw("down", 0)
        );
        List<TokenAndWeight> serviceStart = List.of(
            tw("ml13", 2),
            tw("4606.1.p2ps", 2),
            tw("Info", 6),
            tw("Service", 6),
            tw("ML_FEED", 1),
            tw("id", 3),
            tw("of", 3),
            tw("has", 0),
            tw("started", 0)
        );
        List<TokenAndWeight> noImageData = List.of(
            tw("ml13", 2),
            tw("4607.1.p2ps", 2),
            tw("Debug", 6),
            tw("Did", 6),
            tw("not", 1),
            tw("receive", 3),
            tw("an", 3),
            tw("image", 0),
            tw("data", 0),
            tw("for", 0),
            tw("ML_FEED", 0),
            tw("4205.T", 0),
            tw("on", 0),
            tw("Recalling", 0),
            tw("item", 0)
        );
        List<TokenAndWeight> empty = List.of();

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, sourceShutDown2), equalTo(3));
        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown2, sourceShutDown1), equalTo(3));

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, sourceShutDown2), equalTo(3));
        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown2, sourceShutDown1), equalTo(3));

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, serviceStart), equalTo(15));
        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, sourceShutDown1), equalTo(15));

        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown2, serviceStart), equalTo(13));
        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, sourceShutDown2), equalTo(13));

        assertThat(TokenListSimilarityTester.weightedEditDistance(noImageData, serviceStart), equalTo(21));
        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, noImageData), equalTo(21));

        assertThat(TokenListSimilarityTester.weightedEditDistance(noImageData, sourceShutDown1), equalTo(21));
        assertThat(TokenListSimilarityTester.weightedEditDistance(sourceShutDown1, noImageData), equalTo(21));

        assertThat(TokenListSimilarityTester.weightedEditDistance(serviceStart, empty), equalTo(23));
        assertThat(TokenListSimilarityTester.weightedEditDistance(empty, serviceStart), equalTo(23));
    }
}
