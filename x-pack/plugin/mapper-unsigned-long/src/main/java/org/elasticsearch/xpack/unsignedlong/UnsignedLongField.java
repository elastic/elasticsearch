/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.script.Converter;
import org.elasticsearch.script.Converters;
import org.elasticsearch.script.Field;
import org.elasticsearch.script.FieldValues;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;

public class UnsignedLongField extends Field.LongField {
    public UnsignedLongField(String name, FieldValues<Long> values) {
        super(name, values);
    }

    // UnsignedLongFields must define their own conversions as they are in x-pack
    @Override
    public <CT, CF extends Field<CT>> Field<CT> convert(Converter<CT, CF> converter) {
        if (converter.getTargetClass() == BigInteger.class) {
            BigIntegerField bigIntegerField = UnsignedLongToBigInteger(this);
            return converter.getFieldClass().cast(bigIntegerField);
        }

        return super.as(converter);
    }

    static BigIntegerField UnsignedLongToBigInteger(UnsignedLongField sourceField) {
        FieldValues<Long> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new Converters.DelegatingFieldValues<java.math.BigInteger, Long>(fv) {
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
}
