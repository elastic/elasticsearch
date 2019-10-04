/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePart.StartOfWeek;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePart.Part;

public class DatePartProcessor extends ThreeArgsDateTimeProcessor {

    public static final String NAME = "dtpart";

    public DatePartProcessor(Processor source1, Processor source2, Processor source3, ZoneId zoneId) {
        super(source1, source2, source3, zoneId);
    }

    public DatePartProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
            Object o1 = first().process(input);
            if (o1 == null) {
                return null;
            }
            Object o2 = second().process(input);
            if (o2 == null) {
                return null;
            }
            Object o3 = third().process(input);

        return process(o1, o2, o3, zoneId());
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object source1, Object source2, Object source3, ZoneId zoneId) {
        if (source1 == null || source2 == null) {
            return null;
        }
        if (source1 instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", source1);
        }
        Part datePartField = Part.resolve((String) source1);
        if (datePartField == null) {
            List<String> similar = Part.findSimilar((String) source1);
            if (similar.isEmpty()) {
                throw new SqlIllegalArgumentException("A value of {} or their aliases is required; received [{}]",
                    Part.values(), source1);
            } else {
                throw new SqlIllegalArgumentException("Received value [{}] is not valid date part for extraction; " +
                    "did you mean {}?", source1, similar);
            }
        }

        if (source2 instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", source2);
        }

        StartOfWeek startOfWeek;
        if (source3 == null) {
            startOfWeek = StartOfWeek.SUNDAY;
        } else {
            if (source3 instanceof String == false) {
                throw new SqlIllegalArgumentException("A string is required; received [{}]", source3);
            }
            startOfWeek = StartOfWeek.resolve((String) source3);
            if (startOfWeek == null) {
                throw new SqlIllegalArgumentException("A value of {} is required; received [{}]",
                    StartOfWeek.values(), source3);
            }
        }

        return datePartField.extract(((ZonedDateTime) source2).withZoneSameInstant(zoneId), startOfWeek);
    }
}
