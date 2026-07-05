/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESIntegTestCase;

/**
 * This test suite runs indexing tests on a "mixed" cluster, i.e. a cluster where some
 * nodes use the GPU for indexing, and other use the CPU.
 * This is achieved by turning the {@link GPUPlugin#VECTORS_INDEXING_USE_GPU_NODE_SETTING} on/off
 * randomly (see {@link BaseGPUIndexTestCase#nodeSettings})
 */
@LuceneTestCase.SuppressCodecs("*") // use our custom codec
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3)
public class GPUIndexMixedNodeIT extends BaseGPUIndexTestCase {

    // Disable GPU indexing on some (random) nodes
    @Override
    protected boolean isGpuEnabledOnAllNodes() {
        return false;
    }
}
