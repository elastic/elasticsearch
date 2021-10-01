/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class DateMillisField extends Field<ZonedDateTime> {

    /* ---- Conversion Helpers To Other Fields ---- */

    public static LongField toLongField(DateMillisField sourceField) {
        FieldValues<ZonedDateTime> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, ZonedDateTime>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(dt -> dt.toInstant().toEpochMilli()).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return toLong(values.getNonPrimitiveValue());
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

    public static long toLong(ZonedDateTime dt) {
        return dt.toInstant().toEpochMilli();
    }

    /* ---- DateMillis Field Members ---- */

    public DateMillisField(String name, FieldValues<ZonedDateTime> values) {
        super(name, values);
    }
}
