/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

/**
 * A static factory for all available schedules.
 */
public class Schedules {

    private Schedules() {
    }

    /**
     * Creates an interval schedule. The provided string can have the following format:
     * <ul>
     *     <li>34s - a 34 seconds long interval</li>
     *     <li>23m - a 23 minutes long interval</li>
     *     <li>40h - a 40 hours long interval</li>
     *     <li>63d - a 63 days long interval</li>
     *     <li>27w - a 27 weeks long interval</li>
     * </ul>
     *
     * @param interval  The fixed interval by which the schedule will trigger.
     * @return          The newly created interval schedule
     */
    public static IntervalSchedule interval(String interval) {
        return new IntervalSchedule(IntervalSchedule.Interval.parse(interval));
    }

    /**
     * Creates an interval schedule.
     *
     * @param duration  The duration of the interval
     * @param unit      The unit of the duration (seconds, minutes, hours, days or weeks)
     * @return          The newly created interval schedule.
     */
    public static IntervalSchedule interval(long duration, IntervalSchedule.Interval.Unit unit) {
        return new IntervalSchedule(new IntervalSchedule.Interval(duration, unit));
    }

    /**
     * Creates a cron schedule.
     *
     * @param cronExpressions   one or more cron expressions
     * @return                  the newly created cron schedule.
     * @throws                  IllegalArgumentException if any of the given expression is invalid
     */
    public static CronSchedule cron(String... cronExpressions) {
        return new CronSchedule(cronExpressions);
    }

    /**
     * Creates an hourly schedule.
     *
     * @param minutes   the minutes within the hour that the schedule should trigger at. values must be
     *                  between 0 and 59 (inclusive).
     * @return          the newly created hourly schedule
     * @throws          IllegalArgumentException if any of the provided minutes are out of valid range
     */
    public static HourlySchedule hourly(int... minutes) {
        return new HourlySchedule(minutes);
    }

    /**
     * @return  A builder for an hourly schedule.
     */
    public static HourlySchedule.Builder hourly() {
        return HourlySchedule.builder();
    }

    /**
     * @return  A builder for a daily schedule.
     */
    public static DailySchedule.Builder daily() {
        return DailySchedule.builder();
    }

    /**
     * @return  A builder for a weekly schedule.
     */
    public static WeeklySchedule.Builder weekly() {
        return WeeklySchedule.builder();
    }

    /**
     * @return  A builder for a monthly schedule.
     */
    public static MonthlySchedule.Builder monthly() {
        return MonthlySchedule.builder();
    }
}
