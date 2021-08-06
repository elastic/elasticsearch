/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

/**
 * Empty versions of all the duck-typed field values supported.
 */
public class EmptyFieldValues {
    public static final FieldValues.Doubles DOUBLE = new Doubles();
    public static final FieldValues.Longs LONG = new Longs();
    public static final FieldValues.BigIntegers BIGINTEGER = new BigIntegers();
    public static final FieldValues.Objects OBJECT = new Objects();

    protected static void badIndex(int index) {
        throw new IllegalStateException("Cannot read index [" + index + "] for empty field");
    }

    private static class Doubles implements FieldValues.Doubles {
        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public double getDouble(int index) {
            throw new IllegalStateException("Cannot read index [" + index + "] for empty field");
        }

        @Override
        public List<Double> getDoubles() {
            return Collections.emptyList();
        }
    }

    private static class Longs implements FieldValues.Longs {
        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long getLong(int index) {
            throw new IllegalStateException("Cannot read index [" + index + "] for empty field");
        }

        @Override
        public List<Long> getLongs() {
            return Collections.emptyList();
        }
    }

    private static class Objects implements FieldValues.Objects {
        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Object getObject(int index) {
            throw new IllegalStateException("Cannot read index [" + index + "] for empty field");
        }

        @Override
        public List<Object> getObjects() {
            return Collections.emptyList();
        }
    }

    private static class BigIntegers implements FieldValues.BigIntegers {
        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public BigInteger getBigInteger(int index) {
            throw new IllegalStateException("Cannot read index [" + index + "] for empty field");
        }

        @Override
        public List<BigInteger> getBigIntegers() {
            return Collections.emptyList();
        }
    }
}
