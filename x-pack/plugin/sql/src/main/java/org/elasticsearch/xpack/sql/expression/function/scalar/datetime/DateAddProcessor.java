/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateAdd.Part;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class DateAddProcessor extends ThreeArgsDateTimeProcessor {

    public static final String NAME = "dtadd";

    public DateAddProcessor(Processor source1, Processor source2, Processor source3, ZoneId zoneId) {
        super(source1, source2, source3, zoneId);
    }

    public DateAddProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object doProcess(Object o1, Object o2, Object o3, ZoneId zoneId) {
        return process(o1, o2, o3, zoneId);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object source1, Object source2, Object source3, ZoneId zoneId) {
        if (source1 == null || source2 == null || source3 == null) {
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
                throw new SqlIllegalArgumentException("Received value [{}] is not valid date part to add; " +
                    "did you mean {}?", source1, similar);
            }
        }

        if (source2 instanceof Integer == false) {
            throw new SqlIllegalArgumentException("An integer is required; received [{}]", source2);
        }

        if (source3 instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", source3);
        }

        return datePartField.add(((ZonedDateTime) source3).withZoneSameInstant(zoneId), (Integer) source2);
    }
}
