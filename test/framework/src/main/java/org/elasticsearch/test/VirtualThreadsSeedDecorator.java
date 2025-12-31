/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.SeedDecorator;

import org.elasticsearch.common.hash.MurmurHash3;

/**
 * Virtual threads are one-shot, so the PRNG will keep returning the same
 * few numbers at the start of the sequence on them. This breaks at least one
 * test, and probably makes many more less-random than we'd like. This won't
 * be deterministic because it incorporates {@link Thread#threadId()}, but
 * I don't think that determinism is reliable in multi-thread tests anyway.
 */
public class VirtualThreadsSeedDecorator implements SeedDecorator {

    @Override
    public void initialize(Class<?> suiteClass) {
        // Don't do anything per-suite
    }

    @Override
    public long decorate(long seed) {
        if (Thread.currentThread().isVirtual()) {
            // I have no idea if this is sound, but it seems to produce random-ish values
            return seed ^ MurmurHash3.fmix(Thread.currentThread().threadId());
        }
        return seed;
    }
}
