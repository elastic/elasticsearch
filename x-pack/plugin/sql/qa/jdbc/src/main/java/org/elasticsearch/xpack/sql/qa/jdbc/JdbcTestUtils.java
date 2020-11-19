/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.sql.jdbc.EsType;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.Version.V_7_11_0;

final class JdbcTestUtils {

    private JdbcTestUtils() {}

    private static final Map<Class<?>, EsType> CLASS_TO_ES_TYPE;

    static final ZoneId UTC = ZoneId.of("Z");
    static final String JDBC_TIMEZONE = "timezone";
    private static final String DRIVER_VERSION_PROPERTY_NAME = "jdbc.driver.version";
    static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

    static final Version JDBC_DRIVER_VERSION;

    static {
        // master's version is x.0.0-SNAPSHOT, tho Version#fromString() won't accept that back for recent versions
        String jdbcDriverVersion = System.getProperty(DRIVER_VERSION_PROPERTY_NAME, "").replace("-SNAPSHOT", "");
        JDBC_DRIVER_VERSION = Version.fromString(jdbcDriverVersion); // takes empty and null strings, resolves them to CURRENT

        Map<Class<?>, EsType> aMap = new LinkedHashMap<>();
        aMap.put(Boolean.class, EsType.BOOLEAN);
        aMap.put(Byte.class, EsType.BYTE);
        aMap.put(Short.class, EsType.SHORT);
        aMap.put(Integer.class, EsType.INTEGER);
        aMap.put(Long.class, EsType.LONG);
        if (isUnsignedLongSupported()) {
            aMap.put(BigInteger.class, EsType.UNSIGNED_LONG);
        }
        aMap.put(Float.class, EsType.FLOAT);
        aMap.put(Double.class, EsType.DOUBLE);
        aMap.put(String.class, EsType.KEYWORD);
        aMap.put(byte[].class, EsType.BINARY);
        aMap.put(Timestamp.class, EsType.DATETIME);

        // apart from the mappings in {@code DataType} three more Java classes can be mapped to a {@code JDBCType.TIMESTAMP}
        // according to B-4 table from the jdbc4.2 spec
        aMap.put(Calendar.class, EsType.DATETIME);
        aMap.put(GregorianCalendar.class, EsType.DATETIME);
        aMap.put(java.util.Date.class, EsType.DATETIME);
        aMap.put(java.sql.Date.class, EsType.DATETIME);
        aMap.put(java.sql.Time.class, EsType.TIME);
        aMap.put(LocalDateTime.class, EsType.DATETIME);
        CLASS_TO_ES_TYPE = Collections.unmodifiableMap(aMap);
    }

    static EsType of(Class<?> clazz) {
        return CLASS_TO_ES_TYPE.get(clazz);
    }

    static String of(long millis, String zoneId) {
        return StringUtils.toString(ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(zoneId)));
    }

    static Date asDate(long millis, ZoneId zoneId) {
        return new java.sql.Date(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId).toLocalDate().atStartOfDay(zoneId).toInstant().toEpochMilli()
        );
    }

    static Time asTime(long millis, ZoneId zoneId) {
        return new Time(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId)
                .toLocalTime()
                .atDate(JdbcTestUtils.EPOCH)
                .atZone(zoneId)
                .toInstant()
                .toEpochMilli()
        );
    }

    static long convertFromCalendarToUTC(long value, Calendar cal) {
        if (cal == null) {
            return value;
        }
        Calendar c = (Calendar) cal.clone();
        c.setTimeInMillis(value);

        ZonedDateTime convertedDateTime = ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId())
            .withZoneSameLocal(ZoneOffset.UTC);

        return convertedDateTime.toInstant().toEpochMilli();
    }

    public static boolean isUnsignedLongSupported() {
        // TODO: add equality only once actually ported to 7.11
        return V_7_11_0.compareTo(JDBC_DRIVER_VERSION) < 0;
    }
}
