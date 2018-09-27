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
import org.joda.time.ReadableInstant;

import java.io.IOException;
import java.util.TimeZone;

public abstract class BaseDateTimeProcessor implements Processor {

    private final TimeZone timeZone;
    
    BaseDateTimeProcessor(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
    
    BaseDateTimeProcessor(StreamInput in) throws IOException {
        timeZone = TimeZone.getTimeZone(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(timeZone.getID());
    }
    
    TimeZone timeZone() {
        return timeZone;
    }

    @Override
    public Object process(Object l) {
        if (l == null) {
            return null;
        }
        long millis;
        if (l instanceof String) {
            // 6.4+
            millis = Long.parseLong(l.toString());
        } else if (l instanceof ReadableInstant) {
            // 6.3-
            millis = ((ReadableInstant) l).getMillis();
        } else {
            throw new SqlIllegalArgumentException("A string or a date is required; received {}", l);
        }
        
        return doProcess(millis);
    }

    abstract Object doProcess(long millis);
}
