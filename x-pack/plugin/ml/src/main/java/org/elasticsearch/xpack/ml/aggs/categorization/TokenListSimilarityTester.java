/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategory.TokenAndWeight;

import java.util.List;

/**
 * Partial port of the C++ class
 * <a href="https://github.com/elastic/ml-cpp/blob/main/include/core/CStringSimilarityTester.h"><code>CStringSimilarityTester</code></a>.
 */
public class TokenListSimilarityTester {

    /**
     * Calculate the weighted edit distance between two sequences. Each
     * element of each sequence has an associated weight, such that some
     * elements can be considered more expensive to add/remove/replace than
     * others.
     *
     * Unfortunately, in the case of arbitrary weightings, the
     * Berghel-Roach algorithm cannot be applied. Ukkonen gives a
     * counter-example on page 114 of Information and Control, Vol 64,
     * Nos. 1-3, January/February/March 1985. The problem is that the
     * matrix diagonals are not necessarily monotonically increasing.
     * See http://www.cs.helsinki.fi/u/ukkonen/InfCont85.PDF.
     *
     * TODO: It may be possible to apply some of the lesser optimisations
     *       from section 2 of Ukkonen's paper to this algorithm.
     */
    public static int weightedEditDistance(List<TokenAndWeight> first, List<TokenAndWeight> second) {

        // This is similar to the traditional Levenshtein distance, as
        // described in http://en.wikipedia.org/wiki/Levenshtein_distance,
        // but adding the concept of different costs for each element. If
        // you are trying to understand this method, you should first make
        // sure you fully understand traditional Levenshtein distance.

        int firstLen = first.size();
        int secondLen = second.size();

        // Rule out boundary cases.
        if (firstLen == 0) {
            return second.stream().mapToInt(TokenAndWeight::getWeight).sum();
        }

        if (secondLen == 0) {
            return first.stream().mapToInt(TokenAndWeight::getWeight).sum();
        }

        // We need to store just two columns of the matrix; the current and
        // previous columns. We allocate two arrays and then swap their
        // meanings.
        int[] currentCol = new int[secondLen + 1];
        int[] prevCol = new int[secondLen + 1];

        // Populate the left column.
        currentCol[0] = 0;
        for (int downMinusOne = 0; downMinusOne < secondLen; ++downMinusOne) {
            currentCol[downMinusOne + 1] = currentCol[downMinusOne] + second.get(downMinusOne).getWeight();
        }

        // Calculate the other entries in the matrix.
        for (TokenAndWeight firstTokenAndWeight : first) {
            {
                int[] temp = prevCol;
                prevCol = currentCol;
                currentCol = temp;
            }
            int firstCost = firstTokenAndWeight.getWeight();
            currentCol[0] = prevCol[0] + firstCost;

            for (int downMinusOne = 0; downMinusOne < secondLen; ++downMinusOne) {
                TokenAndWeight secondTokenAndWeight = second.get(downMinusOne);
                int secondCost = secondTokenAndWeight.getWeight();

                // There are 3 options, and due to the possible differences
                // in the weightings, we must always evaluate all 3:

                // 1) Deletion => cell to the left's value plus cost of
                // deleting the element from the first sequence.
                int option1 = prevCol[downMinusOne + 1] + firstCost;

                // 2) Insertion => cell above's value plus cost of
                // inserting the element from the second sequence.
                int option2 = currentCol[downMinusOne] + secondCost;

                // 3) Substitution => cell above left's value plus the
                // higher of the two element weights.
                // OR
                // No extra cost in the case where the corresponding
                // elements are equal.
                int option3 = prevCol[downMinusOne] + ((firstTokenAndWeight.getTokenId() == secondTokenAndWeight.getTokenId())
                    ? 0
                    : Math.max(firstCost, secondCost));

                // Take the cheapest option of the 3.
                currentCol[downMinusOne + 1] = Math.min(Math.min(option1, option2), option3);
            }
        }

        // Result is the value in the bottom right hand corner of the matrix.
        return currentCol[secondLen];
    }
}
