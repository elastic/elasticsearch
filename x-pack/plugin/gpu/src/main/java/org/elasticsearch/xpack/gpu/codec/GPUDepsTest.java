/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.LibraryException;

// Delete me just a test
class GPUDepsTest {

    void foo() {
        try {
            var resources = CuVSResources.create();
        } catch (LibraryException ex) {
            throw ex;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
