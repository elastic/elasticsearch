/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

public class InjectionTests extends ScriptTestCase {

    public void testInjection() {
        assertEquals(1968,
                exec("org.elasticsearch.painless.FeatureTestObject.staticNumberArgument(8);"));
    }

    public void testInstanceInjection() {
        assertEquals(123000,
                exec("org.elasticsearch.painless.FeatureTestObject f = new org.elasticsearch.painless.FeatureTestObject(100, 0); " +
                        "f.injectTimesX(5)"));
    }

    public void testInstanceInjectWithLambda() {
        assertEquals(246000,
                exec("org.elasticsearch.painless.FeatureTestObject f = new org.elasticsearch.painless.FeatureTestObject(100, 0); " +
                        "f.injectWithLambda(x -> 2*x, 5)"));
    }

    public void testInstanceInjectWithDefLambda() {
        assertEquals(246000,
                exec("def f = new org.elasticsearch.painless.FeatureTestObject(100, 0); f.injectWithLambda(x -> 2*x, (short)5)"));
    }

    public void testInjectionOnDefNoInject() {
        assertEquals(123000,
                exec("def d = new org.elasticsearch.painless.FeatureTestObject(100, 0); d.injectTimesX((short)5)"));
    }

    public void testInjectionOnMethodReference() {
        assertEquals(7380,
                exec(
                        "def ft0 = new org.elasticsearch.painless.FeatureTestObject(2, 0); " +
                                "org.elasticsearch.painless.FeatureTestObject ft1 = new org.elasticsearch.painless.FeatureTestObject(1000, 0); " +
                                "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"));
    }

    public void testInjectionOnMethodReference2() {
        assertEquals(7380,
                exec(
                        "org.elasticsearch.painless.FeatureTestObject ft0 = new org.elasticsearch.painless.FeatureTestObject(2, 0); " +
                                "def ft1 = new org.elasticsearch.painless.FeatureTestObject(1000, 0); " +
                                "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"));
    }

    public void testInjectionOnMethodReference3() {
        assertEquals(7380,
                exec(
                        "def ft0 = new org.elasticsearch.painless.FeatureTestObject(2, 0); " +
                                "def ft1 = new org.elasticsearch.painless.FeatureTestObject(1000, 0); " +
                                "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"));
    }

    public void testAugmentedInstanceInjection() {
        assertEquals(123000,
                exec("org.elasticsearch.painless.FeatureTestObject f = new org.elasticsearch.painless.FeatureTestObject(100, 0); " +
                        "f.augmentInjectTimesX(5)"));
    }

    public void testAugmentedInstanceInjectWithLambda() {
        assertEquals(246000,
                exec("org.elasticsearch.painless.FeatureTestObject f = new org.elasticsearch.painless.FeatureTestObject(100, 0); " +
                        "f.augmentInjectWithLambda(x -> 2*x, 5)"));
    }

    public void testAugmentedInstanceInjectWithDefLambda() {
        assertEquals(246000,
                exec("def f = new org.elasticsearch.painless.FeatureTestObject(100, 0); f.augmentInjectWithLambda(x -> 2*x, (short)5)"));
    }

    public void testAugmentedInjectionOnDefNoInject() {
        assertEquals(123000,
                exec("def d = new org.elasticsearch.painless.FeatureTestObject(100, 0); d.augmentInjectTimesX((short)5)"));
    }

    public void testAugmentedInjectionOnMethodReference() {
        assertEquals(7380,
                exec(
                        "def ft0 = new org.elasticsearch.painless.FeatureTestObject(2, 0); " +
                                "org.elasticsearch.painless.FeatureTestObject ft1 = new org.elasticsearch.painless.FeatureTestObject(1000, 0); " +
                                "ft1.augmentTimesSupplier(ft0::augmentInjectTimesX, (short)3, 5)"));
    }

    public void testAugmentedInjectionOnMethodReference2() {
        assertEquals(7380,
                exec(
                        "org.elasticsearch.painless.FeatureTestObject ft0 = new org.elasticsearch.painless.FeatureTestObject(2, 0); " +
                                "def ft1 = new org.elasticsearch.painless.FeatureTestObject(1000, 0); " +
                                "ft1.augmentTimesSupplier(ft0::augmentInjectTimesX, (short)3, 5)"));
    }

    public void testAugmentedInjectionOnMethodReference3() {
        assertEquals(7380,
                exec(
                        "def ft0 = new org.elasticsearch.painless.FeatureTestObject(2, 0); " +
                                "def ft1 = new org.elasticsearch.painless.FeatureTestObject(1000, 0); " +
                                "ft1.augmentTimesSupplier(ft0::augmentInjectTimesX, (short)3, 5)"));
    }
}
