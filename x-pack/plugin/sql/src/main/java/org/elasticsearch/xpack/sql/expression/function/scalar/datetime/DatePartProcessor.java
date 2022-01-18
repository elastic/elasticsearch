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
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePart.Part;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class DatePartProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtpart";

    public DatePartProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2, zoneId);
    }

    public DatePartProcessor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object part, Object timestamp) {
        return process(part, timestamp, zoneId());
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object part, Object timestamp, ZoneId zoneId) {
        if (part == null || timestamp == null) {
            return null;
        }
        if (part instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", part);
        }
        Part datePartField = Part.resolve((String) part);
        if (datePartField == null) {
            List<String> similar = Part.findSimilar((String) part);
            if (similar.isEmpty()) {
                throw new SqlIllegalArgumentException("A value of {} or their aliases is required; received [{}]", Part.values(), part);
            } else {
                throw new SqlIllegalArgumentException(
                    "Received value [{}] is not valid date part for extraction; " + "did you mean {}?",
                    part,
                    similar
                );
            }
        }

        if (timestamp instanceof ZonedDateTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime is required; received [{}]", timestamp);
        }

        return datePartField.extract(((ZonedDateTime) timestamp).withZoneSameInstant(zoneId));
    }
}
