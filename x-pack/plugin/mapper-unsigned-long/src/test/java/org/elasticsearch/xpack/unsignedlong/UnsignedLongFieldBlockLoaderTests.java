/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.NumberFieldBlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class UnsignedLongFieldBlockLoaderTests extends NumberFieldBlockLoaderTestCase<Long> {
    private static final long MASK_2_63 = 0x8000000000000000L;
    private static final BigInteger BIGINTEGER_2_64_MINUS_ONE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    public UnsignedLongFieldBlockLoaderTests(Params params) {
        super(FieldType.UNSIGNED_LONG, params);
    }

    @Override
    protected Long convert(Number value, Map<String, Object> fieldMapping) {
        // Adjust values coming from source to the way they are stored in doc_values.
        // See mapper implementation.
        var unsigned = value.longValue();
        return unsigned ^ MASK_2_63;
    }

    @Override
    protected Number tryParseString(String s) {
        try {
            return Long.parseUnsignedLong(s);
        } catch (NumberFormatException ignored) {
            try {
                var bigInteger = Numbers.newBigDecimal(s).toBigIntegerExact();
                if (bigInteger.signum() < 0 || bigInteger.compareTo(BIGINTEGER_2_64_MINUS_ONE) > 0) {
                    return null;
                }
                return bigInteger.longValue();
            } catch (ArithmeticException | NumberFormatException e) {
                return null;
            }
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }
}
