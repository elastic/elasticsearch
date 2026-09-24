/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.test.ESTestCase;

public class DenseVectorTests extends ESTestCase {

    public void testBuiltInEndpointBatchCapForJinaEis() {
        assertEquals(DenseVector.EIS_JINA_V5_MAX_BATCH_SIZE, DenseVector.builtInEndpointBatchCap(DenseVector.EIS_JINA_V5_INFERENCE_ID));
    }

    public void testBuiltInEndpointBatchCapForE5() {
        assertEquals(
            DenseVector.DEFAULT_INFERENCE_ID_MAX_BATCH_SIZE,
            DenseVector.builtInEndpointBatchCap(DenseVector.DEFAULT_INFERENCE_ID)
        );
    }

    public void testBuiltInEndpointBatchCapForUserEndpointIsUnbounded() {
        assertEquals(Integer.MAX_VALUE, DenseVector.builtInEndpointBatchCap("my-own-embedding-endpoint"));
    }
}
