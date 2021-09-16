/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.script.field.BigIntegerField;
import org.elasticsearch.script.field.Converter;
import org.elasticsearch.script.field.DelegatingFieldValues;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.script.field.FieldValues;
import org.elasticsearch.script.field.InvalidConversion;
import org.elasticsearch.script.field.LongField;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;

public class UnsignedLongField extends LongField {

    /* ---- Conversion Class From Other Fields ----*/

    public static final Converter<Long, UnsignedLongField> UnsignedLong = new Converter<Long, UnsignedLongField>() {
        @Override
        public UnsignedLongField convert(Field<?> sourceField) {
            if (sourceField instanceof BigIntegerField) {
                return fromBigIntegerField((BigIntegerField) sourceField);
            }
            if (sourceField instanceof LongField) {
                return fromLongField((LongField) sourceField);
            }

            throw new InvalidConversion(sourceField.getClass(), UnsignedLongField.class);
        }

        @Override
        public Class<UnsignedLongField> getFieldClass() {
            return UnsignedLongField.class;
        }

        @Override
        public Class<Long> getTargetClass() {
            return Long.class;
        }
    };

    /* ---- Conversion Helpers To Other Fields ---- */

    public static BigIntegerField toBigIntegerField(UnsignedLongField sourceField) {
        FieldValues<Long> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<java.math.BigInteger, Long>(fv) {
            private BigInteger toBigInteger(long formatted) {
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

    /* ---- Conversion Helpers From Other Fields ---- */

    public static UnsignedLongField fromBigIntegerField(BigIntegerField sourceField) {
        FieldValues<BigInteger> fv = sourceField.getFieldValues();
        return new UnsignedLongField(sourceField.getName(), new DelegatingFieldValues<>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(BigIntegerField::toLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return values.getNonPrimitiveValue().longValue();
            }

            @Override
            public long getLongValue() {
                return values.getNonPrimitiveValue().longValue();
            }

            @Override
            public double getDoubleValue() {
                return values.getNonPrimitiveValue().doubleValue();
            }
        });
    }

    public static UnsignedLongField fromLongField(LongField sourceField) {
        FieldValues<Long> fv = sourceField.getFieldValues();
        return new UnsignedLongField(sourceField.getName(), new DelegatingFieldValues<>(fv) {
            @Override
            public List<Long> getValues() {
                // Takes longs in raw format
                return values.getValues().stream().map(UnsignedLongScriptDocValues::shiftedLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return getLongValue();
            }

            @Override
            public long getLongValue() {
                return UnsignedLongScriptDocValues.shiftedLong(values.getLongValue());
            }

            @Override
            public double getDoubleValue() {
                return getLongValue();
            }
        });
    }

    /* ---- Unsigned Long Field Members ---- */

    public UnsignedLongField(String name, FieldValues<Long> values) {
        super(name, values);
    }

    // UnsignedLongFields must define their own conversions as they are in x-pack
    @Override
    public <CT, CF extends Field<CT>> Field<CT> convert(Converter<CT, CF> converter) {
        if (converter.getTargetClass() == BigInteger.class) {
            BigIntegerField bigIntegerField = toBigIntegerField(this);
            return converter.getFieldClass().cast(bigIntegerField);
        }

        return super.as(converter);
    }
}
