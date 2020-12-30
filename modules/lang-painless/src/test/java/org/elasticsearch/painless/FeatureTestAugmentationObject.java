/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import java.util.function.Function;

public class FeatureTestAugmentationObject {

    public static int getTotal(FeatureTestObject ft) {
        return ft.getX() + ft.getY();
    }

    public static int addToTotal(FeatureTestObject ft, int add) {
        return getTotal(ft) + add;
    }

    public static int augmentInjectTimesX(FeatureTestObject ft, int injected, short user) {
        return ft.getX() * injected * user;
    }

    public static int augmentTimesSupplier(FeatureTestObject ft, Function<Short, Integer> fn, short fnArg, int userArg) {
        return fn.apply(fnArg) * userArg;
    }

    public static int augmentInjectWithLambda(FeatureTestObject ft, int injected, Function<Short, Integer> fn, short arg) {
        return ft.getX()*fn.apply(arg)*injected;
    }

    public static int augmentInjectMultiTimesX(FeatureTestObject ft, int inject1, int inject2, short user) {
        return ft.getX() * (inject1 + inject2) * user;
    }

    public static int augmentInjectMultiWithLambda(FeatureTestObject ft,
            int inject1, int inject2, int inject3, int inject4, Function<Short, Integer> fn, short arg) {
        return ft.getX()*fn.apply(arg)*(inject1 + inject2 + inject3 + inject4);
    }

    private FeatureTestAugmentationObject() {}
}
