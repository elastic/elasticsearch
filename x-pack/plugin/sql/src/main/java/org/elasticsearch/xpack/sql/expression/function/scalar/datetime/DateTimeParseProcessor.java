/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;


import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.util.DateUtils.UTC;

public class DateTimeParseProcessor extends BinaryDateTimeProcessor {

    public enum Parser {
        DATE_TIME((timestampStr, pattern) -> DateTimeFormatter.ofPattern(pattern, Locale.ROOT)
                .parseBest(timestampStr, ZonedDateTime::from, LocalDateTime::from)),
        TIME((timestampStr, pattern) -> DateTimeFormatter.ofPattern(pattern, Locale.ROOT)
                .parseBest(timestampStr, OffsetTime::from, LocalTime::from));

        private final BiFunction<String, String, TemporalAccessor> parser;

        Parser(BiFunction<String, String, TemporalAccessor> parser) {
            this.parser = parser;
        }

        public Object parse(Object timestamp, Object pattern) {
            if (timestamp == null || pattern == null) {
                return null;
            }
            if (timestamp instanceof String == false) {
                throw new SqlIllegalArgumentException("A string is required; received [{}]", timestamp);
            }
            if (pattern instanceof String == false) {
                throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
            }

            if (((String) timestamp).isEmpty() || ((String) pattern).isEmpty()) {
                return null;
            }
            try {
                TemporalAccessor ta = parser.apply((String) timestamp, (String) pattern);
                if (ta instanceof LocalDateTime) {
                    return ZonedDateTime.ofInstant((LocalDateTime) ta, ZoneOffset.UTC, UTC);
                } else if (ta instanceof LocalTime) {
                    return OffsetTime.of((LocalTime) ta, ZoneOffset.UTC);
                } else {
                    return ta;
                }
            } catch (IllegalArgumentException | DateTimeException e) {
                String msg = e.getMessage();
                if (msg.contains("Unable to convert parsed text using any of the specified queries")) {
                    msg = "Unable to convert parsed text into [datetime or time]";
                }
                throw new SqlIllegalArgumentException(
                    "Invalid time/datetime string [{}] or pattern [{}] is received; {}",
                    timestamp,
                    pattern,
                    msg
                );
            }
        }
    }
    
    private final Parser parser;

    public static final String NAME = "dtparse";

    public DateTimeParseProcessor(Processor source1, Processor source2, Parser parser) {
        super(source1, source2, null);
        this.parser = parser;
    }

    public DateTimeParseProcessor(StreamInput in) throws IOException {
        super(in);
        this.parser = in.readEnum(Parser.class);
    }
    
    @Override
    public void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(parser);
    }
    
    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object timestamp, Object pattern) {
        return this.parser.parse(timestamp, pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parser, left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DateTimeParseProcessor other = (DateTimeParseProcessor) obj;
        return Objects.equals(parser, other.parser)
            && Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

    @Override
    public String toString(){
        return parser.toString();
    }
    
    public Parser extractor() {
        return parser;
    }
}
