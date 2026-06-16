/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class SimilarityMeasureTests extends ESTestCase {

    public void testParseSimilarity_ValidValues_ReturnsEnum() {
        for (SimilarityMeasure measure : SimilarityMeasure.values()) {
            assertThat(SimilarityMeasure.parseSimilarity(measure.toString()), is(measure));
            assertThat(SimilarityMeasure.parseSimilarity(measure.name()), is(measure));
        }
    }

    public void testParseSimilarity_InvalidValue_ThrowsExceptionWithAcceptedValues() {
        var invalidValue = randomAlphaOfLength(8);
        var e = expectThrows(IllegalArgumentException.class, () -> SimilarityMeasure.parseSimilarity(invalidValue));
        assertThat(e.getMessage(), is(Strings.format("Invalid value [%s]; expected one of [cosine, dot_product, l2_norm]", invalidValue)));
    }
}
