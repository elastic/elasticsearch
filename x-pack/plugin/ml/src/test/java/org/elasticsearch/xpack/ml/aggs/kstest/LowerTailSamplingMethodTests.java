/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

public class LowerTailSamplingMethodTests extends SamplingMethodTests {

    @Override
    SamplingMethod createInstance() {
        return new SamplingMethod.LowerTail();
    }

    @Override
    boolean isDescending() {
        return true;
    }
}
