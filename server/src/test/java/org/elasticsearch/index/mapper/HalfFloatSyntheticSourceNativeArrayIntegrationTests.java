/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.sandbox.document.HalfFloatPoint;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class HalfFloatSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {

    public void testSynthesizeArray() throws Exception {
        var inputArrayValues = new Float[][] { new Float[] { 0.78151345F, 0.6886488F, 0.6882413F } };
        var expectedArrayValues = new Float[inputArrayValues.length][inputArrayValues[0].length];
        for (int i = 0; i < inputArrayValues.length; i++) {
            for (int j = 0; j < inputArrayValues[i].length; j++) {
                expectedArrayValues[i][j] = HalfFloatPoint.sortableShortToHalfFloat(
                    HalfFloatPoint.halfFloatToSortableShort(inputArrayValues[i][j])
                );
            }
        }

        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", getFieldTypeName())
            .endObject()
            .endObject()
            .endObject();

        verifySyntheticArray(inputArrayValues, expectedArrayValues, mapping, "_id");
    }

    @Override
    protected String getFieldTypeName() {
        return "half_float";
    }

    @Override
    protected Float getRandomValue() {
        return HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
    }

    @Override
    protected String getMalformedValue() {
        return RandomStrings.randomAsciiOfLength(random(), 8);
    }
}
