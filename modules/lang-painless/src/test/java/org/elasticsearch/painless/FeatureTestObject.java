/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** Currently just a dummy class for testing a few features not yet exposed by whitelist! */
public class FeatureTestObject {
    /** static method that returns true */
    public static boolean overloadedStatic() {
        return true;
    }

    /** static method that returns what you ask it */
    public static boolean overloadedStatic(boolean whatToReturn) {
        return whatToReturn;
    }

    /** static method only whitelisted as a static */
    public static float staticAddFloatsTest(float x, float y) {
        return x + y;
    }

    /** static method with a type parameter Number */
    public static int staticNumberTest(Number number) {
        return number.intValue();
    }

    public static int staticNumberArgument(int injected, int userArgument) {
        return injected * userArgument;
    }

    public static final List<String> STRINGS = Collections.singletonList("test_string");

    private int x;
    private int y;
    public int z;

    private Integer i;

    /** empty ctor */
    public FeatureTestObject() {}

    /** ctor with params */
    public FeatureTestObject(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /** getter for x */
    public int getX() {
        return x;
    }

    /** setter for x */
    public void setX(int x) {
        this.x = x;
    }

    /** getter for y */
    public int getY() {
        return y;
    }

    /** setter for y */
    public void setY(int y) {
        this.y = y;
    }

    /** getter for i */
    public Integer getI() {
        return i;
    }

    /** setter for y */
    public void setI(Integer i) {
        this.i = i;
    }

    public int injectTimesX(int injected, short user) {
        return this.x * injected * user;
    }

    public int timesSupplier(Function<Short, Integer> fn, short fnArg, int userArg) {
        return fn.apply(fnArg) * userArg;
    }

    public int injectWithLambda(int injected, Function<Short, Integer> fn, short arg) {
        return this.x * fn.apply(arg) * injected;
    }

    public int injectMultiTimesX(int inject1, int inject2, int inject3, short user) {
        return this.x * (inject1 + inject2 + inject3) * user;
    }

    public int injectMultiWithLambda(int inject1, int inject2, int inject3, Function<Short, Integer> fn, short arg) {
        return this.x * fn.apply(arg) * (inject1 + inject2 + inject3);
    }

    public Double mixedAdd(int someInt, Byte b, char c, Float f) {
        return (double) (someInt + b + c + f);
    }

    /** method taking two functions! */
    public Object twoFunctionsOfX(Function<Object, Object> f, Function<Object, Object> g) {
        return f.apply(g.apply(x));
    }

    /** method to take in a list */
    public void listInput(List<Object> list) {

    }
}
