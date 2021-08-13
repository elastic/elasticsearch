/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.script.Converter;
import org.elasticsearch.script.DelegatingFieldValues;
import org.elasticsearch.script.Field;
import org.elasticsearch.script.FieldValues;
import org.elasticsearch.script.InvalidConversion;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;

public class UnsignedLongField extends Field.LongField {
    public UnsignedLongField(String name, FieldValues<Long> values) {
        super(name, values);
    }

    @Override
    public <CT, CF extends Field<CT>> Field<CT> as(Converter<CT, CF> converter) {
        if (converter.getFieldClass().isInstance(this)) {
            return converter.getFieldClass().cast(this);
        }

        if (converter.getTargetClass() == BigInteger.class) {
            BigIntegerField bigIntegerField = UnsignedLongToBigInteger(this);
            return converter.getFieldClass().cast(bigIntegerField);
        }

        return super.as(converter);
    }

    public static class UnsignedLongConverter implements Converter<Long, UnsignedLongField> {
        @Override
        public UnsignedLongField convert(Field<?> sourceField) {
            if (sourceField instanceof BigIntegerField) {
                return BigIntegerToUnsignedLong((BigIntegerField) sourceField);
            }

            throw new InvalidConversion(sourceField.getClass(), getFieldClass());
        }

        @Override
        public Class<UnsignedLongField> getFieldClass() {
            return UnsignedLongField.class;
        }

        @Override
        public Class<Long> getTargetClass() {
            return Long.class;
        }
    }

    public static BigIntegerField UnsignedLongToBigInteger(UnsignedLongField sourceField) {
        FieldValues<Long> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<java.math.BigInteger, Long>(fv) {
            protected BigInteger toBigInteger(long formatted) {
                return java.math.BigInteger.valueOf(formatted).and(BIGINTEGER_2_64_MINUS_ONE);
            }

            @Override
            public List<BigInteger> getValues() {
                return values.getValues().stream().map(this::toBigInteger).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return toBigInteger(values.getLongValue());
            }
        });
    }

    public static UnsignedLongField BigIntegerToUnsignedLong(BigIntegerField sourceField) {
        FieldValues<BigInteger> fv = sourceField.getFieldValues();
        return new UnsignedLongField(sourceField.getName(), new DelegatingFieldValues<>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(java.math.BigInteger::longValue).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return values.getLongValue();
            }
        });
    }
}
