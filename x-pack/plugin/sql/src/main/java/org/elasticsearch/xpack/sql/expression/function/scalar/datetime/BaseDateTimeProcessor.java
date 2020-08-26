/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public abstract class BaseDateTimeProcessor implements Processor {

    private final ZoneId zoneId;

    BaseDateTimeProcessor(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    BaseDateTimeProcessor(StreamInput in) throws IOException {
        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public Object process(Object input) {
        if (input == null) {
            return null;
        }

        if (!(input instanceof ZonedDateTime)) {
            throw new SqlIllegalArgumentException("A [date], a [time] or a [datetime] is required; received {}", input);
        }

        return doProcess(((ZonedDateTime) input).withZoneSameInstant(zoneId));
    }

    abstract Object doProcess(ZonedDateTime dateTime);
}
