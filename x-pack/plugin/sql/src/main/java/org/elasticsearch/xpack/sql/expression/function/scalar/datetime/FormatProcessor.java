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
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public class FormatProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "format";

    private static final String[][] JAVA_TIME_FORMAT_REPLACEMENTS = {
        {"tt", "a"},
        {"t", "a"},
        {"dddd", "eeee"},
        {"ddd", "eee"},
        {"K", "v"},
        {"g", "G"},
        {"f", "S"},
    };

    public FormatProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2, zoneId);
    }

    public FormatProcessor(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object timestamp, Object pattern, ZoneId zoneId) {
        if (timestamp == null || pattern == null) {
            return null;
        }
        if (pattern instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
        }
        if (((String) pattern).isEmpty()) {
            return null;
        }

        if (timestamp instanceof ZonedDateTime == false && timestamp instanceof OffsetTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime/time is required; received [{}]", timestamp);
        }

        TemporalAccessor ta;
        if (timestamp instanceof ZonedDateTime) {
            ta = ((ZonedDateTime) timestamp).withZoneSameInstant(zoneId);
        } else {
            ta = asTimeAtZone((OffsetTime) timestamp, zoneId);
        }
        try {
            return DateTimeFormatter.ofPattern(getJavaPattern(pattern), Locale.ROOT).format(ta);
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new SqlIllegalArgumentException(
                "Invalid pattern [{}] is received for formatting date/time [{}]; {}",
                pattern,
                timestamp,
                e.getMessage()
            );
        }
    }

    private static String getJavaPattern(final Object pattern) {
        String javaDateFormat = (String) pattern;
        for (String[] replacement : JAVA_TIME_FORMAT_REPLACEMENTS) {
            if (javaDateFormat.contains(replacement[0])) {
                javaDateFormat = javaDateFormat.replace(replacement[0], replacement[1]);
            }
        }
        return javaDateFormat;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object timestamp, Object pattern) {
        return process(timestamp, pattern, zoneId());
    }
}
