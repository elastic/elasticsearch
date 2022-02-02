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

public enum Month {

    JANUARY("JAN"),
    FEBRUARY("FEB"),
    MARCH("MAR"),
    APRIL("APR"),
    MAY("MAY"),
    JUNE("JUN"),
    JULY("JUL"),
    AUGUST("AUG"),
    SEPTEMBER("SEP"),
    OCTOBER("OCT"),
    NOVEMBER("NOV"),
    DECEMBER("DEC");

    private final String cronKey;

    Month(String cronKey) {
        this.cronKey = cronKey;
    }

    public static String cronPart(EnumSet<Month> days) {
        StringBuilder sb = new StringBuilder();
        for (Month day : days) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(day.cronKey);
        }
        return sb.toString();
    }

    public static Month resolve(int month) {
        return switch (month) {
            case 1 -> JANUARY;
            case 2 -> FEBRUARY;
            case 3 -> MARCH;
            case 4 -> APRIL;
            case 5 -> MAY;
            case 6 -> JUNE;
            case 7 -> JULY;
            case 8 -> AUGUST;
            case 9 -> SEPTEMBER;
            case 10 -> OCTOBER;
            case 11 -> NOVEMBER;
            case 12 -> DECEMBER;
            default -> throw new ElasticsearchParseException("unknown month number [{}]", month);
        };
    }

    public static Month resolve(String day) {
        return switch (day.toLowerCase(Locale.ROOT)) {
            case "1", "jan", "first", "january" -> JANUARY;
            case "2", "feb", "february" -> FEBRUARY;
            case "3", "mar", "march" -> MARCH;
            case "4", "apr", "april" -> APRIL;
            case "5", "may" -> MAY;
            case "6", "jun", "june" -> JUNE;
            case "7", "jul", "july" -> JULY;
            case "8", "aug", "august" -> AUGUST;
            case "9", "sep", "september" -> SEPTEMBER;
            case "10", "oct", "october" -> OCTOBER;
            case "11", "nov", "november" -> NOVEMBER;
            case "12", "dec", "last", "december" -> DECEMBER;
            default -> throw new ElasticsearchParseException("unknown month [{}]", day);
        };
    }

    @Override
    public String toString() {
        return cronKey;
    }
}
