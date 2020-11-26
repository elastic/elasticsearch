/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    public FieldHitExtractor(String name, String fullFieldName, DataType dataType, ZoneId zoneId, boolean useDocValue, String hitName,
                             boolean arrayLeniency) {
        super(name, fullFieldName, dataType, zoneId, useDocValue, hitName, arrayLeniency);
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
                return parseDateString(values);
            }
        }

        return null;
    }

    protected Object parseDateString(Object values) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(values.toString())), zoneId());
    }

    @Override
    protected boolean isPrimitive(List<?> list) {
        return false;
    }
}
