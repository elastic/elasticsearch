/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.scheduler.Cron;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.Month;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.MonthTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.WeekTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.YearTimes;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.daily;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.hourly;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.monthly;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.weekly;

public abstract class ScheduleTestCase extends ESTestCase {

    protected static String[] expressions(CronnableSchedule schedule) {
        return expressions(schedule.crons);
    }

    protected static String[] expressions(Cron[] crons) {
        String[] expressions = new String[crons.length];
        for (int i = 0; i < expressions.length; i++) {
            expressions[i] = crons[i].expression();
        }
        return expressions;
    }

    protected static MonthlySchedule randomMonthlySchedule() {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> monthly().build();
            case 2 -> monthly().time(MonthTimes.builder().atMidnight()).build();
            case 3 -> monthly().time(MonthTimes.builder().on(randomIntBetween(1, 31)).atMidnight()).build();
            default -> new MonthlySchedule(validMonthTimes());
        };
    }

    protected static WeeklySchedule randomWeeklySchedule() {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> weekly().build();
            case 2 -> weekly().time(WeekTimes.builder().atMidnight()).build();
            case 3 -> weekly().time(WeekTimes.builder().on(DayOfWeek.THURSDAY).atMidnight()).build();
            default -> new WeeklySchedule(validWeekTimes());
        };
    }

    protected static DailySchedule randomDailySchedule() {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> daily().build();
            case 2 -> daily().atMidnight().build();
            case 3 -> daily().atNoon().build();
            default -> new DailySchedule(validDayTimes());
        };
    }

    protected static HourlySchedule randomHourlySchedule() {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> hourly().build();
            case 2 -> hourly().minutes(randomIntBetween(0, 59)).build();
            case 3 -> hourly(randomIntBetween(0, 59));
            default -> hourly().minutes(validMinutes()).build();
        };
    }

    protected static IntervalSchedule randomIntervalSchedule() {
        return switch (randomIntBetween(1, 3)) {
            case 1 -> interval(randomInterval().toString());
            case 2 -> interval(randomIntBetween(1, 100), randomIntervalUnit());
            default -> new IntervalSchedule(randomInterval());
        };
    }

    protected static IntervalSchedule.Interval randomInterval() {
        return new IntervalSchedule.Interval(randomIntBetween(1, 100), randomIntervalUnit());
    }

    protected static IntervalSchedule.Interval.Unit randomIntervalUnit() {
        return IntervalSchedule.Interval.Unit.values()[randomIntBetween(0, IntervalSchedule.Interval.Unit.values().length - 1)];
    }

    protected static YearTimes validYearTime() {
        return new YearTimes(randomMonths(), randomDaysOfMonth(), validDayTimes());
    }

    protected static YearTimes[] validYearTimes() {
        int count = randomIntBetween(2, 5);
        Set<YearTimes> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(validYearTime());
        }
        return times.toArray(new YearTimes[times.size()]);
    }

    protected static MonthTimes validMonthTime() {
        return new MonthTimes(randomDaysOfMonth(), validDayTimes());
    }

    protected static MonthTimes[] validMonthTimes() {
        int count = randomIntBetween(2, 5);
        Set<MonthTimes> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            MonthTimes testMonthTimes = validMonthTime();
            boolean intersectsExistingMonthTimes = false;
            for (MonthTimes validMonthTimes : times) {
                if (validMonthTimes.intersects(testMonthTimes)) {
                    intersectsExistingMonthTimes = true;
                }
            }
            if (intersectsExistingMonthTimes == false) {
                times.add(testMonthTimes);
            }
        }
        return times.toArray(new MonthTimes[times.size()]);
    }

    protected static WeekTimes validWeekTime() {
        return new WeekTimes(randomDaysOfWeek(), validDayTimes());
    }

    protected static WeekTimes[] validWeekTimes() {
        int count = randomIntBetween(2, 5);
        Set<WeekTimes> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(validWeekTime());
        }
        return times.toArray(new WeekTimes[times.size()]);
    }

    protected static EnumSet<DayOfWeek> randomDaysOfWeek() {
        int count = randomIntBetween(1, DayOfWeek.values().length - 1);
        Set<DayOfWeek> days = new HashSet<>();
        for (int i = 0; i < count; i++) {
            days.add(DayOfWeek.values()[randomIntBetween(0, count)]);
        }
        return EnumSet.copyOf(days);
    }

    protected static EnumSet<Month> randomMonths() {
        int count = randomIntBetween(1, 11);
        Set<Month> months = new HashSet<>();
        for (int i = 0; i < count; i++) {
            months.add(Month.values()[randomIntBetween(0, 11)]);
        }
        return EnumSet.copyOf(months);
    }

    protected static Object randomMonth() {
        int m = randomIntBetween(1, 14);
        return switch (m) {
            case 13 -> "first";
            case 14 -> "last";
            default -> Month.resolve(m);
        };
    }

    protected static int[] randomDaysOfMonth() {
        if (rarely()) {
            return new int[] { 32 };
        }
        int count = randomIntBetween(1, 5);
        Set<Integer> days = new HashSet<>();
        for (int i = 0; i < count; i++) {
            days.add(randomIntBetween(1, 31));
        }
        return CollectionUtils.toArray(days);
    }

    protected static Object randomDayOfMonth() {
        int day = randomIntBetween(1, 32);
        if (day == 32) {
            return "last_day";
        }
        if (day == 1) {
            return randomBoolean() ? "first_day" : 1;
        }
        return day;
    }

    protected static int dayOfMonthToInt(Object dom) {
        if (dom instanceof Integer) {
            return (Integer) dom;
        }
        if ("last_day".equals(dom)) {
            return 32;
        }
        if ("first_day".equals(dom)) {
            return 1;
        }
        throw new IllegalStateException("cannot convert given day-of-month [" + dom + "] to int");
    }

    protected static Object invalidDayOfMonth() {
        return randomBoolean() ? randomAlphaOfLength(5) : randomBoolean() ? randomIntBetween(-30, -1) : randomIntBetween(33, 45);
    }

    protected static DayTimes validDayTime() {
        return randomBoolean() ? DayTimes.parse(validDayTimeStr()) : new DayTimes(validHours(), validMinutes());
    }

    protected static String validDayTimeStr() {
        int hour = validHour();
        int min = validMinute();
        StringBuilder sb = new StringBuilder();
        if (hour < 10 && randomBoolean()) {
            sb.append("0");
        }
        sb.append(hour).append(":");
        if (min < 10) {
            sb.append("0");
        }
        return sb.append(min).toString();
    }

    protected static HourAndMinute invalidDayTime() {
        return randomBoolean() ? new HourAndMinute(invalidHour(), invalidMinute())
            : randomBoolean() ? new HourAndMinute(validHour(), invalidMinute())
            : new HourAndMinute(invalidHour(), validMinute());
    }

    protected static String invalidDayTimeStr() {
        int hour;
        int min;
        switch (randomIntBetween(1, 3)) {
            case 1 -> {
                hour = invalidHour();
                min = validMinute();
            }
            case 2 -> {
                hour = validHour();
                min = invalidMinute();
            }
            default -> {
                hour = invalidHour();
                min = invalidMinute();
            }
        }

        StringBuilder sb = new StringBuilder();
        if (hour < 10 && randomBoolean()) {
            sb.append("0");
        }
        sb.append(hour).append(":");
        if (min < 10) {
            sb.append("0");
        }
        return sb.append(min).toString();
    }

    protected static DayTimes[] validDayTimes() {
        int count = randomIntBetween(2, 5);
        Set<DayTimes> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(validDayTime());
        }
        return times.toArray(new DayTimes[times.size()]);
    }

    protected static DayTimes[] validDayTimesFromNumbers() {
        int count = randomIntBetween(2, 5);
        Set<DayTimes> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(new DayTimes(validHours(), validMinutes()));
        }
        return times.toArray(new DayTimes[times.size()]);
    }

    protected static DayTimes[] validDayTimesFromStrings() {
        int count = randomIntBetween(2, 5);
        Set<DayTimes> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(DayTimes.parse(validDayTimeStr()));
        }
        return times.toArray(new DayTimes[times.size()]);
    }

    protected static HourAndMinute[] invalidDayTimes() {
        int count = randomIntBetween(2, 5);
        Set<HourAndMinute> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(invalidDayTime());
        }
        return times.toArray(new HourAndMinute[times.size()]);
    }

    protected static String[] invalidDayTimesAsStrings() {
        int count = randomIntBetween(2, 5);
        Set<String> times = new HashSet<>();
        for (int i = 0; i < count; i++) {
            times.add(invalidDayTimeStr());
        }
        return times.toArray(new String[times.size()]);
    }

    protected static int validMinute() {
        return randomIntBetween(0, 59);
    }

    protected static int[] validMinutes() {
        int count = randomIntBetween(2, 6);
        int inc = 59 / count;
        int[] minutes = new int[count];
        for (int i = 0; i < count; i++) {
            minutes[i] = randomIntBetween(i * inc, (i + 1) * inc);
        }
        return minutes;
    }

    protected static int invalidMinute() {
        return randomBoolean() ? randomIntBetween(60, 100) : randomIntBetween(-60, -1);
    }

    protected static int[] invalidMinutes() {
        int count = randomIntBetween(2, 6);
        int[] minutes = new int[count];
        for (int i = 0; i < count; i++) {
            minutes[i] = invalidMinute();
        }
        return minutes;
    }

    protected static int validHour() {
        return randomIntBetween(0, 23);
    }

    protected static int[] validHours() {
        int count = randomIntBetween(2, 6);
        int inc = 23 / count;
        int[] hours = new int[count];
        for (int i = 0; i < count; i++) {
            hours[i] = randomIntBetween(i * inc, (i + 1) * inc);
        }
        return hours;
    }

    protected static int invalidHour() {
        return randomBoolean() ? randomIntBetween(24, 40) : randomIntBetween(-60, -1);
    }

    static class HourAndMinute implements ToXContentObject {

        int hour;
        int minute;

        HourAndMinute(int hour, int minute) {
            this.hour = hour;
            this.minute = minute;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(DayTimes.HOUR_FIELD.getPreferredName(), hour)
                .field(DayTimes.MINUTE_FIELD.getPreferredName(), minute)
                .endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            HourAndMinute that = (HourAndMinute) o;

            if (hour != that.hour) return false;
            if (minute != that.minute) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = hour;
            result = 31 * result + minute;
            return result;
        }
    }

}
