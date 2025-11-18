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

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MurmurHash3;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link Randomness} class creates random generators with the same seed
 * in every thread.
 * <p>
 * This means that repeatedly calling:
 * <pre>
 *   {@code
 *     new Thread(() -> System.out.println(Randomness.get().nextInt())).start();
 *   }
 * </pre>
 * will print the same number in every thread.
 * <p>
 * For some use cases, this is not desirable, e.g. when testing that the random
 * behavior obeys certain statistical properties.
 * <p>
 * To fix this, annotate a test class with:
 * <pre>
 *   {@code
 *     @SeedDecorators(MixWithIncrement.class)
 *   }
 * </pre>
 * In this way, an additional seed is mixed into the seed of the random generators.
 * This additional seed can be updated be calling:
 * <pre>
 *   {@code
 *     MixWithIncrement.next()
 *   }
 * </pre>
 * to make sure that new threads will get a different seed.
 */
public class MixWithIncrement implements SeedDecorator {

    private static final AtomicLong mix = new AtomicLong(1);

    @Override
    public void initialize(Class<?> aClass) {
        next();
    }

    public long decorate(long seed) {
        return seed ^ mix.get();
    }

    public static void next() {
        mix.updateAndGet(MurmurHash3::fmix);
    }
}
