/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

public class DateNanosField extends Field<JodaCompatibleZonedDateTime> {

    /* ---- Conversion Helpers To Other Fields ---- */

    public static LongField toLongField(DateNanosField sourceField) {
        FieldValues<JodaCompatibleZonedDateTime> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, JodaCompatibleZonedDateTime>(fv) {
            protected long nanoLong(JodaCompatibleZonedDateTime dt) {
                return ChronoUnit.NANOS.between(Instant.EPOCH, dt.toInstant());
            }

            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(this::nanoLong).collect(Collectors.toList());
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
                return toLong(values.getNonPrimitiveValue());
            }
        });
    }

    /* ---- Conversion Helpers To Other Types ---- */

    public static long toLong(JodaCompatibleZonedDateTime dt) {
        return ChronoUnit.NANOS.between(Instant.EPOCH, dt.toInstant());
    }

    /* ---- DateNanos Field Members ---- */

    public DateNanosField(String name, FieldValues<JodaCompatibleZonedDateTime> values) {
        super(name, values);
    }
}
