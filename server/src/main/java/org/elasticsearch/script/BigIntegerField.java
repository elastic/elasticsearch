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
 * Duck-typed {@code BigInteger} field.
 */
public class BigIntegerField extends AbstractField<BigInteger, FieldValues.BigIntegers> {
    public BigIntegerField(String name, FieldValues.BigIntegers values) {
        super(name, values);
    }

    public BigInteger getValue(BigInteger defaultValue) {
        if (values.isEmpty()) {
            return defaultValue;
        }
        return values.getBigInteger(0);
    }

    @Override
    protected List<BigInteger> getFieldValues() {
        return values.getBigIntegers();
    }

    @Override
    public BigIntegerField asBigIntegerField() {
        return this;
    }

    @Override
    public BigInteger asBigInteger(BigInteger defaultValue) {
        return getValue(defaultValue);
    }
}
