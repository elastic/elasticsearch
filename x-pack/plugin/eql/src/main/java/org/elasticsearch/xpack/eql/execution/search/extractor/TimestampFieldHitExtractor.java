/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search.extractor;

import java.math.BigDecimal;

public class TimestampFieldHitExtractor extends FieldHitExtractor {

    public TimestampFieldHitExtractor(FieldHitExtractor target) {
        super(target.fieldName(), target.dataType(), target.zoneId(), target.hitName(),
                target.arrayLeniency());
    }

    @Override
    protected Object parseEpochMillisAsString(String str) {
        // Only parse as BigDecimal if needed by nanos accuracy (when getting millis with sub-millis). Long nanos will only count up to
        // 2262-04-11T23:47:16.854775807Z. Doubles are not accurate enough to hold six digit submillis with granularity for current dates.
        return str.lastIndexOf('.') > 0 ? new BigDecimal(str) : Long.parseLong(str);
    }
}
