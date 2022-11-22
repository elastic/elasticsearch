/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import java.util.BitSet;

public abstract class NumberArrayBlock extends Block {

    Number[] internalNumberValues;

    public NumberArrayBlock(Number[] values, int positionCount) {
        super(positionCount);
        assert values.length == positionCount;
        this.internalNumberValues = new Number[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (values[i] == null) {
                nullsMask.set(i);
                internalNumberValues[i] = nullValue();
            } else {
                internalNumberValues[i] = values[i];
            }
        }
    }

    public NumberArrayBlock(int positionCount) {
        super(positionCount);
    }

    public NumberArrayBlock(int positionCount, BitSet nulls) {
        super(positionCount, nulls);
    }

    abstract Number nullValue();
}
