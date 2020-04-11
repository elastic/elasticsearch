/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.util.DateUtils.UTC;

public class TimeParseProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "tparse";

    public TimeParseProcessor(Processor source1, Processor source2) {
        super(source1, source2, null);
    }

    public TimeParseProcessor(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object timestampStr, Object pattern) {
        if (timestampStr == null || pattern == null) {
            return null;
        }
        if (timestampStr instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", timestampStr);
        }
        if (pattern instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
        }

        if (((String) timestampStr).isEmpty() || ((String) pattern).isEmpty()) {
            return null;
        }

        try {
            TemporalAccessor ta = DateTimeFormatter.ofPattern((String) pattern, Locale.ROOT)
                .parseBest((String) timestampStr, ZonedDateTime::from, LocalTime::from);
            return ta;
        } catch (IllegalArgumentException |
            DateTimeException e) {
            String msg = e.getMessage();
            if (msg.contains("Unable to convert parsed text using any of the specified queries")) {
                msg = "Unable to convert parsed text into [time]";
            }
            throw new SqlIllegalArgumentException(
                "Invalid time string [{}] or pattern [{}] is received; {}",
                timestampStr,
                pattern,
                msg
            );
        }

    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object timestamp, Object pattern) {
        return process(timestamp, pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimeParseProcessor other = (TimeParseProcessor) obj;
        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

}
