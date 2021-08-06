/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.math.BigInteger;
import java.util.List;

/**
 * Codecs for reading underlying field values.  A particular ScriptDocValues subclass may
 * implement multiple {@code FieldValues} interfaces.
 */
public interface FieldValues {
    boolean isEmpty();

    interface Doubles extends FieldValues {
        double getDouble(int index);
        List<Double> getDoubles();
    }

    interface Longs extends FieldValues {
        long getLong(int index);
        List<Long> getLongs();
    }

    interface Objects extends FieldValues {
        Object getObject(int index);
        List<Object> getObjects();
    }

    interface BigIntegers extends FieldValues {
        BigInteger getBigInteger(int index);
        List<BigInteger> getBigIntegers();
    }
}
