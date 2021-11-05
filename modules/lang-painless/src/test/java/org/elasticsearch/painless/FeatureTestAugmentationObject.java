/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        return ft.getX() * fn.apply(arg) * injected;
    }

    public static int augmentInjectMultiTimesX(FeatureTestObject ft, int inject1, int inject2, short user) {
        return ft.getX() * (inject1 + inject2) * user;
    }

    public static int augmentInjectMultiWithLambda(
        FeatureTestObject ft,
        int inject1,
        int inject2,
        int inject3,
        int inject4,
        Function<Short, Integer> fn,
        short arg
    ) {
        return ft.getX() * fn.apply(arg) * (inject1 + inject2 + inject3 + inject4);
    }

    private FeatureTestAugmentationObject() {}
}
