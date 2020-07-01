
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.locationtech.jts.util.StringUtil;

import java.io.IOException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;


import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class DateFormatProcessor extends DateTimeFormatProcessor {

    private static Map<String,String> formatMap = null;

    private static Map<String, String> getPatternMap() {

        if (formatMap != null) {
            return formatMap;
        }

        formatMap = new HashMap<>();
        formatMap.put("%a", "EEE");
        formatMap.put("%b", "MMM");
        formatMap.put("%c", "MM");
        formatMap.put("%D", "dd");
        formatMap.put("%d", "dd");
        formatMap.put("%e", "dd");
        formatMap.put("%H", "HH");
        formatMap.put("%h", "hh");
        formatMap.put("%I", "hh");
        formatMap.put("%i", "mm");
        formatMap.put("%j", "DDD");
        formatMap.put("%k", "H");
        formatMap.put("%l", "hh");
        formatMap.put("%M", "MMMM");
        formatMap.put("%m", "MM");
        formatMap.put("%p", "a");
        formatMap.put("%r", "hh:mm:ss a");
        formatMap.put("%S", "ss");
        formatMap.put("%s", "ss");
        formatMap.put("%W", "EEEE");
        formatMap.put("%w", "F");
        formatMap.put("%Y", "yyyy");
        formatMap.put("%y", "yy");
        formatMap.put("%U", "w");
        formatMap.put("%u", "w");
        formatMap.put("%V", "w");
        formatMap.put("%v", "w");
        formatMap.put("%T", "HH:mm:ss a");
        formatMap.put("%X", "w");
        formatMap.put("%x", "w");
        return formatMap;
    }

    public DateFormatProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2, zoneId);
    }

    public DateFormatProcessor(StreamInput in) throws IOException {
        super(in);
    }

    public static final String NAME = "dformat";

    public static Object process(Object timestamp, String pattern) {
        formatMap = DateFormatProcessor.getPatternMap();
        ZoneId zoneId = UTC;
        if (pattern == null || pattern.isEmpty()) {
            return null;
        }

        pattern = transformSqlFormatToJavaFormat(pattern);
        return DateTimeFormatProcessor.process(timestamp, pattern, zoneId);
    }

    public static String transformSqlFormatToJavaFormat(String pattern) {
        return formatMap
            .entrySet()
            .stream()
            .reduce(pattern, (s, e) -> s.replace(e.getKey(), e.getValue()), (s, s2) -> s);
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
