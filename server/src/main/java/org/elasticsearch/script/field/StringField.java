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
                return getNonPrimitiveValue().longValue();
            }

            @Override
            public double getDoubleValue() {
                return getNonPrimitiveValue().doubleValue();
            }
        });
    }

    public static LongField toLongField(StringField sourceField) {
        FieldValues<String> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, String>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(StringField::toLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return toLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return toLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                // conversion is to LongField, doesn't make sense to parse a Double out of the String here.
                return toLong(values.getNonPrimitiveValue());
            }
        });
    }

    /* ---- Conversion Helpers To Other Types ---- */

    public static BigInteger toBigInteger(String str) {
        try {
            return new BigInteger(str);
        } catch (NumberFormatException e) {
            return new BigDecimal(str).toBigInteger();
        }
    }

    public static long toLong(String str) {
        return Long.parseLong(str);
    }

    public static double toDouble(String str) {
        return Double.parseDouble(str);
    }

    /* ---- String Field Members ---- */

    public StringField(String name, FieldValues<String> values) {
        super(name, values);
    }
}
