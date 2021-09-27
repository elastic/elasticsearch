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

public class StringField extends Field<String> {

    /* ---- Conversion Helpers To Other Fields ---- */

    public static BigIntegerField toBigIntegerField(StringField sourceField) {
        FieldValues<String> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<java.math.BigInteger, String>(fv) {
            @Override
            public List<BigInteger> getValues() {
                // This may throw NumberFormatException, should we catch and truncate the List? (#76951)
                return values.getValues().stream().map(StringField::toBigInteger).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return toBigInteger(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return values.getLongValue();
            }

            @Override
            public double getDoubleValue() {
                return DoubleField.toBigInteger(values.getDoubleValue()).doubleValue();
            }
        });
    }

    public static LongField toLongField(StringField sourceField) {
        FieldValues<String> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, String>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(Long::parseLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return Long.parseLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return values.getLongValue();
            }

            @Override
            public double getDoubleValue() {
                return (long)values.getDoubleValue();
            }
        });
    }

    /* ---- Conversion Helpers To Other Types ---- */

    public static BigInteger toBigInteger(String str) {
        try {
            return new BigInteger(str);
        } catch (NumberFormatException nfe) {
            return new BigDecimal(str).toBigInteger();
        }
    }

    /* ---- String Field Members ---- */

    public StringField(String name, FieldValues<String> values) {
        super(name, values);
    }
}
