/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.carrotsearch.randomizedtesting;

/**
 * Exposes methods that allow to use a {@link RandomizedContext} without using a {@link RandomizedRunner}
 * This was specifically needed by the REST tests since they run with a custom junit runner ({@link org.elasticsearch.test.rest.junit.RestTestSuiteRunner})
 */
public final class StandaloneRandomizedContext {

    private StandaloneRandomizedContext() {

    }

    /**
     * Creates a new {@link RandomizedContext} associated to the current thread
     */
    public static void createRandomizedContext(Class<?> testClass, Randomness runnerRandomness) {
        //the randomized runner is passed in as null, which is fine as long as we don't try to access it afterwards
        RandomizedContext randomizedContext = RandomizedContext.create(Thread.currentThread().getThreadGroup(), testClass, null);
        randomizedContext.push(runnerRandomness.clone(Thread.currentThread()));
    }

    /**
     * Destroys the {@link RandomizedContext} associated to the current thread
     */
    public static void disposeRandomizedContext() {
        RandomizedContext.current().dispose();
    }

    public static void pushRandomness(Randomness randomness) {
        RandomizedContext.current().push(randomness);
    }

    public static void popAndDestroy() {
        RandomizedContext.current().popAndDestroy();
    }

    /**
     * Returns the string formatted seed associated to the current thread's randomized context
     */
    public static String getSeedAsString() {
        return SeedUtils.formatSeed(RandomizedContext.current().getRandomness().getSeed());
    }

    /**
     * Util method to extract the seed out of a {@link Randomness} instance
     */
    public static long getSeed(Randomness randomness) {
        return randomness.getSeed();
    }
}
