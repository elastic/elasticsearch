/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.TimeZone;

public enum Extract {

    YEAR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new Year(source, argument, timeZone);
        }
    },
    MONTH {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new MonthOfYear(source, argument, timeZone);
        }
    },
    WEEK {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new WeekOfWeekYear(source, argument, timeZone);
        }
    },
    DAY {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new DayOfMonth(source, argument, timeZone);
        }
    },
    DAY_OF_MONTH {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return DAY.toFunction(source, argument, timeZone);
        }
    },
    DOM {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return DAY.toFunction(source, argument, timeZone);
        }
    },
    DAY_OF_WEEK {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new DayOfWeek(source, argument, timeZone);
        }
    },
    DOW {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return DAY_OF_WEEK.toFunction(source, argument, timeZone);
        }
    },
    DAY_OF_YEAR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new DayOfYear(source, argument, timeZone);
        }
    },
    DOY {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return DAY_OF_YEAR.toFunction(source, argument, timeZone);
        }
    },
    HOUR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new HourOfDay(source, argument, timeZone);
        }
    },
    MINUTE {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new MinuteOfHour(source, argument, timeZone);
        }
    },
    MINUTE_OF_HOUR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return MINUTE.toFunction(source, argument, timeZone);
        }
    },
    MINUTE_OF_DAY {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new MinuteOfDay(source, argument, timeZone);
        }
    },
    SECOND {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return new SecondOfMinute(source, argument, timeZone);
        }
    },
    SECOND_OF_MINUTE {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone) {
            return SECOND.toFunction(source, argument, timeZone);
        }
    };

    public abstract DateTimeFunction toFunction(Location source, Expression argument, TimeZone timeZone);
}