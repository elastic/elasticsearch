/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateDiff.Part;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class DateDiffProcessor extends ThreeArgsDateTimeProcessor {

    public static final String NAME = "dtdiff";

    public DateDiffProcessor(Processor unit, Processor startTimestamp, Processor endTimestamp, ZoneId zoneId) {
        super(unit, startTimestamp, endTimestamp, zoneId);
    }

    public DateDiffProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object doProcess(Object unit, Object startTimestamp, Object endTimestamp, ZoneId zoneId) {
        return process(unit, startTimestamp, endTimestamp, zoneId);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object unit, Object startTimestamp, Object endTimestamp, ZoneId zoneId) {
        if (unit == null || startTimestamp == null || endTimestamp == null) {
            return null;
        }
        if (unit instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", unit);
        }
        Part datePartField = Part.resolve((String) unit);
        if (datePartField == null) {
            List<String> similar = Part.findSimilar((String) unit);
            if (similar.isEmpty()) {
                throw new SqlIllegalArgumentException("A value of {} or their aliases is required; received [{}]", Part.values(), unit);
            } else {
                throw new SqlIllegalArgumentException(
                    "Received value [{}] is not valid date part to add; " + "did you mean {}?",
                    unit,
                    similar
                );
            }
        }

        if (startTimestamp instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", startTimestamp);
        }

        if (endTimestamp instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", endTimestamp);
        }

        return datePartField.diff(
            ((ZonedDateTime) startTimestamp).withZoneSameInstant(zoneId),
            ((ZonedDateTime) endTimestamp).withZoneSameInstant(zoneId)
        );
    }
}
