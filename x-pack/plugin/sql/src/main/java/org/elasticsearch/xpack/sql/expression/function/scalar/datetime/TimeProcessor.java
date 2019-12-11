/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public class TimeProcessor extends DateTimeProcessor {

    public static final String NAME = "time";

    public TimeProcessor(DateTimeExtractor extractor, ZoneId zoneId) {
        super(extractor, zoneId);
    }

    public TimeProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        if (input instanceof OffsetTime) {
            return doProcess(asTimeAtZone((OffsetTime) input, zoneId()));
        }
        return super.process(input);
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
