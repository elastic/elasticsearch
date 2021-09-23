/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

public class LongField extends Field<Long> {

    /* ---- Conversion Class From Other Fields ----*/

    /**
     * Convert to a {@link LongField} from Double, String, DateMillis, DateNanos, BigInteger or Boolean Fields.
     * Double is cast to a Long.
     * String is parsed as a Long.
     * DateMillis is milliseconds since epoch.
     * DateNanos is nanoseconds since epoch.
     * {@link java.math.BigInteger#longValue()} is used for the BigInteger conversion.
     * Boolean is {@code 1L} if {@code true}, {@code 0L} if {@code false}.
     */
    public static final Converter<Long, LongField> Long;

    static {
        Long = new Converter<Long, LongField>() {
            @Override
            public LongField convert(Field<?> sourceField) {
                if (sourceField instanceof DoubleField) {
                    return DoubleField.toLongField((DoubleField) sourceField);
                }

                if (sourceField instanceof StringField) {
                    return StringField.toLongField((StringField) sourceField);
                }

                if (sourceField instanceof DateMillisField) {
                    return DateMillisField.toLongField((DateMillisField) sourceField);
                }

                if (sourceField instanceof DateNanosField) {
                    return DateNanosField.toLongField((DateNanosField) sourceField);
                }

                if (sourceField instanceof BigIntegerField) {
                    return BigIntegerField.toLongField((BigIntegerField) sourceField);
                }

                if (sourceField instanceof BooleanField) {
                    return BooleanField.toLongField((BooleanField) sourceField);
                }

                throw new InvalidConversion(sourceField.getClass(), getFieldClass());
            }

            @Override
            public Class<LongField> getFieldClass() {
                return LongField.class;
            }

            @Override
            public Class<Long> getTargetClass() {
                return Long.class;
            }
        };
    }

    /* ---- Conversion Helpers To Other Fields ---- */

    public static BigIntegerField toBigIntegerField(LongField sourceField) {
        FieldValues<Long> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<BigInteger, Long>(fv) {
            @Override
            public List<BigInteger> getValues() {
                return values.getValues().stream().map(BigInteger::valueOf).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return BigInteger.valueOf(values.getLongValue());
            }
        });
    }

    /* ---- Long Field Members ---- */

    public LongField(String name, FieldValues<Long> values) {
        super(name, values);
    }
}
