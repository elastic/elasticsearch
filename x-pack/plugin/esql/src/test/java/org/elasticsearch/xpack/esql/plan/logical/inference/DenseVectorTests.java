/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.test.ESTestCase;

public class DenseVectorTests extends ESTestCase {

    public void testDefaultBatchSizeForJinaEis() {
        assertEquals(DenseVector.EIS_JINA_V5_MAX_BATCH_SIZE, DenseVector.defaultBatchSizeFor(DenseVector.EIS_JINA_V5_INFERENCE_ID));
    }

    public void testDefaultBatchSizeForE5() {
        assertEquals(DenseVector.DEFAULT_INFERENCE_ID_MAX_BATCH_SIZE, DenseVector.defaultBatchSizeFor(DenseVector.DEFAULT_INFERENCE_ID));
    }

    public void testDefaultBatchSizeForUnnamedEndpoint() {
        assertEquals(DenseVector.UNNAMED_ENDPOINT_BATCH_SIZE, DenseVector.defaultBatchSizeFor("my-own-embedding-endpoint"));
    }

    public void testUnnamedEndpointBatchSizeIsBelowTheServiceLimit() {
        assertTrue(DenseVector.UNNAMED_ENDPOINT_BATCH_SIZE < 20);
    }
}
