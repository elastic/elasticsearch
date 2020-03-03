/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTrunc.Part;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class DateTruncProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtrunc";

    public DateTruncProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2, zoneId);
    }

    public DateTruncProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object truncateTo, Object timestamp) {
        return process(truncateTo, timestamp, zoneId());
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object truncateTo, Object timestamp, ZoneId zoneId) {
        if (truncateTo == null || timestamp == null) {
            return null;
        }
        if (truncateTo instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", truncateTo);
        }
        Part truncateDateField = Part.resolve((String) truncateTo);
        if (truncateDateField == null) {
            List<String> similar = Part.findSimilar((String) truncateTo);
            if (similar.isEmpty()) {
                throw new SqlIllegalArgumentException("A value of {} or their aliases is required; received [{}]",
                    Part.values(), truncateTo);
            } else {
                throw new SqlIllegalArgumentException("Received value [{}] is not valid date part for truncation; " +
                    "did you mean {}?", truncateTo, similar);
            }
        }

        if (timestamp instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", timestamp);
        }

        return truncateDateField.truncate(((ZonedDateTime) timestamp).withZoneSameInstant(zoneId));
    }
}
