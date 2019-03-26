/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class TimeProcessor extends DateTimeProcessor {


    public static final String NAME = "time";

    public TimeProcessor(DateTimeExtractor extractor, ZoneId zoneId) {
        super(extractor, zoneId);
    }

    public TimeProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Object process(Object input) {
        if (input == null) {
            return null;
        }

        if (input instanceof OffsetTime) {
            OffsetTime ot = (OffsetTime) input;
            return doProcess(ot.withOffsetSameInstant(zoneId().getRules().getOffset(ot.toLocalTime().atDate(DateUtils.EPOCH))));
        }
        if (input instanceof ZonedDateTime) {
            return doProcess(((ZonedDateTime) input).withZoneSameInstant(zoneId()));
        }

        throw new SqlIllegalArgumentException("A [date], a [time] or a [datetime] is required; received {}", input);
    }

    private Object doProcess(OffsetTime time) {
        return extractor().extract(time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extractor(), zoneId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        TimeProcessor other = (TimeProcessor) obj;
        return Objects.equals(extractor(), other.extractor())
                && Objects.equals(zoneId(), other.zoneId());
    }
}
