/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search.extractor;

import org.elasticsearch.xpack.eql.execution.search.Timestamp;

public class TimestampFieldHitExtractor extends FieldHitExtractor {

    public TimestampFieldHitExtractor(FieldHitExtractor target) {
        super(target.fieldName(), target.dataType(), target.zoneId(), target.hitName(), target.multiValueSupport());
    }

    @Override
    protected Object parseEpochMillisAsString(String str) {
        return Timestamp.of(str);
    }
}
