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
        switch (month) {
            case 1: return JANUARY;
            case 2: return FEBRUARY;
            case 3: return MARCH;
            case 4: return APRIL;
            case 5: return MAY;
            case 6: return JUNE;
            case 7: return JULY;
            case 8: return AUGUST;
            case 9: return SEPTEMBER;
            case 10: return OCTOBER;
            case 11: return NOVEMBER;
            case 12: return DECEMBER;
            default:
                throw new ElasticsearchParseException("unknown month number [{}]", month);
        }
    }

    public static Month resolve(String day) {
        switch (day.toLowerCase(Locale.ROOT)) {
            case "1":
            case "jan":
            case "first":
            case "january": return JANUARY;
            case "2":
            case "feb":
            case "february": return FEBRUARY;
            case "3":
            case "mar":
            case "march": return MARCH;
            case "4":
            case "apr":
            case "april": return APRIL;
            case "5":
            case "may": return MAY;
            case "6":
            case "jun":
            case "june": return JUNE;
            case "7":
            case "jul":
            case "july": return JULY;
            case "8":
            case "aug":
            case "august": return AUGUST;
            case "9":
            case "sep":
            case "september": return SEPTEMBER;
            case "10":
            case "oct":
            case "october": return OCTOBER;
            case "11":
            case "nov":
            case "november": return NOVEMBER;
            case "12":
            case "dec":
            case "last":
            case "december": return DECEMBER;
            default:
                throw new ElasticsearchParseException("unknown month [{}]", day);
        }
    }


    @Override
    public String toString() {
        return cronKey;
    }
}
