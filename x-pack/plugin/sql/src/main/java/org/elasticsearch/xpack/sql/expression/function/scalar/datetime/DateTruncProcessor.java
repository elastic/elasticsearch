/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.expression.gen.processor.BinaryProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTrunc.Part;

public class DateTruncProcessor extends BinaryProcessor {

    public static final String NAME = "dtrunc";

    private final ZoneId zoneId;

    public DateTruncProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2);
        this.zoneId = zoneId;
    }

    public DateTruncProcessor(StreamInput in) throws IOException {
        super(in);
        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWrite(StreamOutput out) {
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    protected Object doProcess(Object left, Object right) {
        return process(left, right, zoneId);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object source1, Object source2, String zoneId) {
        return process(source1, source2, ZoneId.of(zoneId));
    }

    static Object process(Object source1, Object source2, ZoneId zoneId) {
        if (source1 == null || source2 == null) {
            return null;
        }
        if (!(source1 instanceof String)) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", source1);
        }
        Part truncateDateField = Part.resolveTruncate((String) source1);
        if (truncateDateField == null) {
            List<String> similar = Part.findSimilar((String) source1);
            if (similar.isEmpty()) {
                throw new SqlIllegalArgumentException("A value of {} or their aliases is required; received [{}]",
                    Part.values(), source1);
            } else {
                throw new SqlIllegalArgumentException("Received value [{}] is not valid date part for truncation; " + "" +
                    "did you mean {}?", source1, similar);
            }
        }

        if (!(source2 instanceof ZonedDateTime)) {
            throw new SqlIllegalArgumentException("A datetime/date is required; received [{}]", source2);
        }

        return truncateDateField.truncate(((ZonedDateTime) source2).withZoneSameInstant(zoneId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTruncProcessor that = (DateTruncProcessor) o;
        return zoneId.equals(that.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId);
    }
}
