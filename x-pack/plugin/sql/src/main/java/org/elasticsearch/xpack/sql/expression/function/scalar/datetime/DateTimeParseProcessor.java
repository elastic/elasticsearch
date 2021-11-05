/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class DateTimeParseProcessor extends BinaryDateTimeProcessor {

    public enum Parser {
        DATE_TIME(DataTypes.DATETIME, ZonedDateTime::from, LocalDateTime::from),
        TIME(SqlDataTypes.TIME, OffsetTime::from, LocalTime::from),
        DATE(SqlDataTypes.DATE, LocalDate::from, (TemporalAccessor ta) -> { throw new DateTimeException("InvalidDate"); });

        private final BiFunction<String, String, TemporalAccessor> parser;

        private final String parseType;

        Parser(DataType parseType, TemporalQuery<?>... queries) {
            this.parseType = parseType.typeName();
            this.parser = (timestampStr, pattern) -> DateTimeFormatter.ofPattern(pattern, Locale.ROOT).parseBest(timestampStr, queries);
        }

        public Object parse(Object timestamp, Object pattern, ZoneId zoneId) {
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
                return DateUtils.atTimeZone(ta, zoneId);
            } catch (IllegalArgumentException | DateTimeException e) {
                String msg = e.getMessage();
                if (msg.contains("Unable to convert parsed text using any of the specified queries")) {
                    msg = format(null, "Unable to convert parsed text into [{}]", this.parseType);
                }
                throw new SqlIllegalArgumentException(
                    "Invalid {} string [{}] or pattern [{}] is received; {}",
                    this.parseType,
                    timestamp,
                    pattern,
                    msg
                );
            }
        }
    }

    private final Parser parser;

    public static final String NAME = "dtparse";

    public DateTimeParseProcessor(Processor source1, Processor source2, ZoneId zoneId, Parser parser) {
        super(source1, source2, zoneId);
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
        return this.parser.parse(timestamp, pattern, zoneId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parser);
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
        return super.equals(other) && Objects.equals(parser, other.parser);
    }

    public Parser parser() {
        return parser;
    }
}
