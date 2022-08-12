/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.ElasticsearchParseException;

import java.util.EnumSet;
import java.util.Locale;

public enum DayOfWeek {

    SUNDAY("SUN"),
    MONDAY("MON"),
    TUESDAY("TUE"),
    WEDNESDAY("WED"),
    THURSDAY("THU"),
    FRIDAY("FRI"),
    SATURDAY("SAT");

    private final String cronKey;

    DayOfWeek(String cronKey) {
        this.cronKey = cronKey;
    }

    public static String cronPart(EnumSet<DayOfWeek> days) {
        StringBuilder sb = new StringBuilder();
        for (DayOfWeek day : days) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(day.cronKey);
        }
        return sb.toString();
    }

    public static DayOfWeek resolve(int day) {
        return switch (day) {
            case 1 -> SUNDAY;
            case 2 -> MONDAY;
            case 3 -> TUESDAY;
            case 4 -> WEDNESDAY;
            case 5 -> THURSDAY;
            case 6 -> FRIDAY;
            case 7 -> SATURDAY;
            default -> throw new ElasticsearchParseException("unknown day of week number [{}]", day);
        };
    }

    public static DayOfWeek resolve(String day) {
        return switch (day.toLowerCase(Locale.ROOT)) {
            case "1", "sun", "sunday" -> SUNDAY;
            case "2", "mon", "monday" -> MONDAY;
            case "3", "tue", "tuesday" -> TUESDAY;
            case "4", "wed", "wednesday" -> WEDNESDAY;
            case "5", "thu", "thursday" -> THURSDAY;
            case "6", "fri", "friday" -> FRIDAY;
            case "7", "sat", "saturday" -> SATURDAY;
            default -> throw new ElasticsearchParseException("unknown day of week [{}]", day);
        };
    }

    @Override
    public String toString() {
        return cronKey;
    }
}
