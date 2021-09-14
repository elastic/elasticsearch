/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.List;
import java.util.stream.Collectors;

public class BooleanField extends Field<Boolean> {

    /* ---- Conversion Helpers To Other Fields ---- */

    public static LongField convertBooleanToLongField(BooleanField sourceField) {
        FieldValues<Boolean> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, Boolean>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(bool -> bool ? 1L : 0L).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertBooleanToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertBooleanToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                return convertBooleanToLong(values.getNonPrimitiveValue());
            }
        });
    }

    /* ---- Conversion Helpers To Other Types ---- */

    public static long convertBooleanToLong(boolean bool) {
        return bool ? 1L : 0L;
    }

    public static double convertBooleanToDouble(boolean bool) {
        return bool ? 1.0d : 0.0d;
    }

    /* ---- Boolean Field Members ---- */

    public BooleanField(String name, FieldValues<Boolean> values) {
        super(name, values);
    }
}
