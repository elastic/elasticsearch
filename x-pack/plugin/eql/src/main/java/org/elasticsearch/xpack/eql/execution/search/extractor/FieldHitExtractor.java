/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;

public class FieldHitExtractor extends AbstractFieldHitExtractor {

    static final String NAME = "f";

    public FieldHitExtractor(StreamInput in) throws IOException {
        super(in);
    }

    public FieldHitExtractor(String name, DataType dataType, ZoneId zoneId, String hitName, boolean arrayLeniency) {
        super(name, dataType, zoneId, hitName, arrayLeniency);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected ZoneId readZoneId(StreamInput in) throws IOException {
        return DateUtils.UTC;
    }

    @Override
    protected Object unwrapCustomValue(Object values) {
        DataType dataType = dataType();

        if (dataType == DATETIME) {
            if (values instanceof String) {
                // We ask @timestamp (or the defined alternative field) to be returned as `epoch_millis`
                // when matching sequence to avoid parsing into ZonedDateTime objects for performance reasons.
                return parseEpochMillisAsString(values.toString());
            }
        }

        return null;
    }

    protected Object parseEpochMillisAsString(String str) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(str)), zoneId());
    }

    @Override
    protected boolean isPrimitive(List<?> list) {
        return false;
    }
}
