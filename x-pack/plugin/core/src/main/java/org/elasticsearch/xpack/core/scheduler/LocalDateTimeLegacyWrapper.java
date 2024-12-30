/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.scheduler;

import java.time.LocalDateTime;
import java.time.chrono.ChronoLocalDateTime;

/**
 * This class is designed to wrap the LocalDateTime class in order to make it behave, in terms of mutation, like a legacy Calendar class.
 * This is to provide compatibility with the existing Cron next runtime calculation algorithm which relies on certain quirks of the Calendar
 * such as days of the week being numbered starting on Sunday==1 and being able to set the current hour to 24 and have it roll over to
 * midnight the next day.
 */
public class LocalDateTimeLegacyWrapper {

    private LocalDateTime ldt;

    public LocalDateTimeLegacyWrapper(LocalDateTime ldt) {
        this.ldt = ldt;
    }

    public int getYear() {
        return ldt.getYear();
    }

    public int getDayOfMonth() {
        return ldt.getDayOfMonth();
    }

    public int getHour() {
        return ldt.getHour();
    }

    public int getMinute() {
        return ldt.getMinute();
    }

    public int getSecond() {
        return ldt.getSecond();
    }

    public int getDayOfWeek() {
        return (ldt.getDayOfWeek().getValue() % 7) + 1;
    }

    public int getMonth() {
        return ldt.getMonthValue() - 1;
    }

    public void setYear(int year) {
        ldt = ldt.withYear(year);
    }

    public void setDayOfMonth(int dayOfMonth) {
        var lengthOfMonth = ldt.getMonth().length(ldt.toLocalDate().isLeapYear());
        if (dayOfMonth <= lengthOfMonth) {
            ldt = ldt.withDayOfMonth(dayOfMonth);
        } else {
            var months = dayOfMonth / lengthOfMonth;
            var day = dayOfMonth % lengthOfMonth;
            ldt = ldt.plusMonths(months).withDayOfMonth(day);
        }
    }

    public void setMonth(int month) {
        month++; // Months are 0-based in Calendar
        if (month <= 12) {
            ldt = ldt.withMonth(month);
        } else {
            var years = month / 12;
            var monthOfYear = month % 12;
            ldt = ldt.plusYears(years).withMonth(monthOfYear);
        }
    }

    public void setHour(int hour) {
        if (hour < 24) {
            ldt = ldt.withHour(hour);
        } else {
            var days = hour / 24;
            var hourOfDay = hour % 24;
            ldt = ldt.plusDays(days).withHour(hourOfDay);
        }
    }

    public void setMinute(int minute) {
        if (minute < 60) {
            ldt = ldt.withMinute(minute);
        } else {
            var hours = minute / 60;
            var minuteOfHour = minute % 60;
            ldt = ldt.plusHours(hours).withMinute(minuteOfHour);
        }
    }

    public void setSecond(int second) {
        if (second < 60) {
            ldt = ldt.withSecond(second);
        } else {
            var minutes = second / 60;
            var secondOfMinute = second % 60;
            ldt = ldt.plusMinutes(minutes).withSecond(secondOfMinute);
        }
    }

    public void plusYears(long years) {
        ldt = ldt.plusYears(years);
    }

    public void plusSeconds(long seconds) {
        ldt = ldt.plusSeconds(seconds);
    }

    public boolean isAfter(ChronoLocalDateTime<?> other) {
        return ldt.isAfter(other);
    }

    public boolean isBefore(ChronoLocalDateTime<?> other) {
        return ldt.isBefore(other);
    }

    public LocalDateTime getLocalDateTime() {
        return ldt;
    }
}
