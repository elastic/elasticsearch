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
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTrunc.Part;

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
                throw new SqlIllegalArgumentException(
                    "A value of {} or their aliases is required; received [{}]",
                    Part.values(),
                    truncateTo
                );
            } else {
                throw new SqlIllegalArgumentException(
                    "Received value [{}] is not valid date part for truncation; " + "did you mean {}?",
                    truncateTo,
                    similar
                );
            }
        }

        if (timestamp instanceof ZonedDateTime == false
            && timestamp instanceof IntervalYearMonth == false
            && timestamp instanceof IntervalDayTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime/interval is required; received [{}]", timestamp);
        }
        if (truncateDateField == Part.WEEK && (timestamp instanceof IntervalDayTime || timestamp instanceof IntervalYearMonth)) {
            throw new SqlIllegalArgumentException("Truncating intervals is not supported for {} units", truncateTo);
        }

        if (timestamp instanceof ZonedDateTime) {
            return truncateDateField.truncate(((ZonedDateTime) timestamp).withZoneSameInstant(zoneId));
        } else if (timestamp instanceof IntervalYearMonth) {
            return truncateDateField.truncate((IntervalYearMonth) timestamp);
        } else {
            return truncateDateField.truncate((IntervalDayTime) timestamp);
        }

    }
}
