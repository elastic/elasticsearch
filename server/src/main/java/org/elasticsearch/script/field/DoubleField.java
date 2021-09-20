/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

public class DoubleField extends Field<Double> {

    /* ---- Conversion Helpers To Other Fields ---- */

    public static BigIntegerField toBigIntegerField(DoubleField sourceField) {
        FieldValues<Double> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<java.math.BigInteger, Double>(fv) {
            @Override
            public List<BigInteger> getValues() {
                return values.getValues().stream().map(DoubleField::toBigInteger).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return toBigInteger(values.getDoubleValue());
            }
        });
    }

    public static LongField toLongField(DoubleField sourceField) {
        FieldValues<Double> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, Double>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(Double::longValue).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return values.getLongValue();
            }
        });
    }

    /* ---- Conversion Helpers To Other Types ---- */

    public static BigInteger toBigInteger(double dbl) {
        return BigDecimal.valueOf(dbl).toBigInteger();
    }

    /* ---- Double Field Members ---- */

    public DoubleField(String name, FieldValues<Double> values) {
        super(name, values);
    }
}
