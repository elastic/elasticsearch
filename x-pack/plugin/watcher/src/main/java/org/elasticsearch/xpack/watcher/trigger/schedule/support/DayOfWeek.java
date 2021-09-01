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
        switch (day) {
            case 1: return SUNDAY;
            case 2: return MONDAY;
            case 3: return TUESDAY;
            case 4: return WEDNESDAY;
            case 5: return THURSDAY;
            case 6: return FRIDAY;
            case 7: return SATURDAY;
            default:
                throw new ElasticsearchParseException("unknown day of week number [{}]", day);
        }
    }

    public static DayOfWeek resolve(String day) {
        switch (day.toLowerCase(Locale.ROOT)) {
            case "1":
            case "sun":
            case "sunday": return SUNDAY;
            case "2":
            case "mon":
            case "monday": return MONDAY;
            case "3":
            case "tue":
            case "tuesday": return TUESDAY;
            case "4":
            case "wed":
            case "wednesday": return WEDNESDAY;
            case "5":
            case "thu":
            case "thursday": return THURSDAY;
            case "6":
            case "fri":
            case "friday": return FRIDAY;
            case "7":
            case "sat":
            case "saturday": return SATURDAY;
            default:
                throw new ElasticsearchParseException("unknown day of week [{}]", day);
        }
    }


    @Override
    public String toString() {
        return cronKey;
    }
}
