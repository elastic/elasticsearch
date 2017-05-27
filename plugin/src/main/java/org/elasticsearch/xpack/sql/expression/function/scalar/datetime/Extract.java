/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

public enum Extract {

    YEAR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new Year(source, argument);
        }
    },
    MONTH {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new MonthOfYear(source, argument);
        }
    },
    WEEK {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new WeekOfWeekYear(source, argument);
        }
    },
    DAY {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new DayOfMonth(source, argument);
        }
    },
    DAY_OF_MONTH {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return DAY.toFunction(source, argument);
        }
    },
    DOM {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return DAY.toFunction(source, argument);
        }
    },
    DAY_OF_WEEK {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new DayOfWeek(source, argument);
        }
    },
    DOW {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return DAY_OF_WEEK.toFunction(source, argument);
        }
    },
    DAY_OF_YEAR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new DayOfYear(source, argument);
        }
    },
    DOY {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return DAY_OF_YEAR.toFunction(source, argument);
        }
    },
    HOUR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new HourOfDay(source, argument);
        }
    },
    MINUTE {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new MinuteOfHour(source, argument);
        }
    },
    MINUTE_OF_HOUR {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return MINUTE.toFunction(source, argument);
        }
    },
    MINUTE_OF_DAY {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new MinuteOfDay(source, argument);
        }
    },
    SECOND {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return new SecondOfMinute(source, argument);
        }
    },
    SECOND_OF_MINUTE {
        @Override
        public DateTimeFunction toFunction(Location source, Expression argument) {
            return SECOND.toFunction(source, argument);
        }
    };

    public abstract DateTimeFunction toFunction(Location source, Expression argument);
}