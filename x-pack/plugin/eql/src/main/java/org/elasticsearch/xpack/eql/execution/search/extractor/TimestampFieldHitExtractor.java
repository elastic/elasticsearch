/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search.extractor;

import java.time.Instant;

public class TimestampFieldHitExtractor extends FieldHitExtractor {

    private static final long[] MICROS_MULTIPLIER = {0L, 100_000L, 10_000L, 1_000L, 1_00L, 10L, 1L};

    public TimestampFieldHitExtractor(FieldHitExtractor target) {
        super(target.fieldName(), target.dataType(), target.zoneId(), target.hitName(),
                target.arrayLeniency());
    }

    @Override
    protected Object parseEpochMillisAsString(String str) {
        // Only parse to Instant when needed by nanos accuracy (when getting <millis>.<micros>); in this case Long is unsuitable, since it
        // can only count nanos up to 2262-04-11T23:47:16.854775807Z.
        // NB: doubles are not accurate enough to hold six digit micros with granularity for current dates.
        int dotIndex = str.lastIndexOf('.');
        return dotIndex > 0
            ? Instant.ofEpochMilli(Long.parseLong(str.substring(0, dotIndex)))
                .plusNanos(Long.parseLong(str.substring(dotIndex + 1)) * MICROS_MULTIPLIER[str.length() - dotIndex - 1])
            : Long.parseLong(str);
    }
}
