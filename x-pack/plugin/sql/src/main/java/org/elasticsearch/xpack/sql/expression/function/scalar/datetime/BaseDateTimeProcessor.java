/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

public abstract class BaseDateTimeProcessor implements Processor {

    private final TimeZone timeZone;
    private final ZoneId zoneId;
    
    BaseDateTimeProcessor(TimeZone timeZone) {
        this.timeZone = timeZone;
        this.zoneId = timeZone.toZoneId();
    }
    
    BaseDateTimeProcessor(StreamInput in) throws IOException {
        timeZone = TimeZone.getTimeZone(in.readString());
        zoneId = timeZone.toZoneId();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(timeZone.getID());
    }
    
    TimeZone timeZone() {
        return timeZone;
    }

    @Override
    public Object process(Object input) {
        if (input == null) {
            return null;
        }

        if (!(input instanceof ZonedDateTime)) {
            throw new SqlIllegalArgumentException("A date is required; received {}", input);
        }

        return doProcess(((ZonedDateTime) input).withZoneSameInstant(zoneId));
    }

    abstract Object doProcess(ZonedDateTime dateTime);
}