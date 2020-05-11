/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateAdd.Part;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class DateAddProcessor extends ThreeArgsDateTimeProcessor {

    public static final String NAME = "dtadd";

    public DateAddProcessor(Processor unit, Processor numberOfUnits, Processor timestamp, ZoneId zoneId) {
        super(unit, numberOfUnits, timestamp, zoneId);
    }

    public DateAddProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object doProcess(Object unit, Object numberOfUnits, Object timestamp, ZoneId zoneId) {
        return process(unit, numberOfUnits, timestamp, zoneId);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object unit, Object numberOfUnits, Object timestamp, ZoneId zoneId) {
        if (unit == null || numberOfUnits == null || timestamp == null) {
            return null;
        }
        if (unit instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", unit);
        }
        Part datePartField = Part.resolve((String) unit);
        if (datePartField == null) {
            List<String> similar = Part.findSimilar((String) unit);
            if (similar.isEmpty()) {
                throw new SqlIllegalArgumentException("A value of {} or their aliases is required; received [{}]",
                    Part.values(), unit);
            } else {
                throw new SqlIllegalArgumentException("Received value [{}] is not valid date part to add; " +
                    "did you mean {}?", unit, similar);
            }
        }

        if (numberOfUnits instanceof Number == false) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", numberOfUnits);
        }

        if (timestamp instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", timestamp);
        }

        return datePartField.add(((ZonedDateTime) timestamp).withZoneSameInstant(zoneId), ((Number) numberOfUnits).intValue());
    }
}
