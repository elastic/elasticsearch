/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeExtractor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.io.IOException;

public class DateTimeProcessor implements ColumnProcessor {
    public static final String NAME = "d";

    private final DateTimeExtractor extractor;

    public DateTimeProcessor(DateTimeExtractor extractor) {
        this.extractor = extractor;
    }

    DateTimeProcessor(StreamInput in) throws IOException {
        extractor = in.readEnum(DateTimeExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(extractor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    DateTimeExtractor extractor() {
        return extractor;
    }

    @Override
    public Object apply(Object l) {
        ReadableDateTime dt = null;
        // most dates are returned as long
        if (l instanceof Long) {
            dt = new DateTime((Long) l, DateTimeZone.UTC);
        }
        else {
            dt = (ReadableDateTime) l;
        }
        return extractor.extract(dt);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeProcessor other = (DateTimeProcessor) obj;
        return extractor == other.extractor;
    }

    @Override
    public int hashCode() {
        return extractor.hashCode();
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
