/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.xpack.watcher.support.Strings.join;

public final class MonthTimes implements Times {

    public static final String LAST = "last_day";
    public static final String FIRST = "first_day";

    public static final int[] DEFAULT_DAYS = new int[] { 1 };
    public static final DayTimes[] DEFAULT_TIMES = new DayTimes[] { new DayTimes() };

    private final int[] days;
    private final DayTimes[] times;

    public MonthTimes() {
        this(DEFAULT_DAYS, DEFAULT_TIMES);
    }

    public MonthTimes(int[] days, DayTimes[] times) {
        this.days = days.length == 0 ? DEFAULT_DAYS : days;
        Arrays.sort(this.days);
        this.times = times.length == 0 ? DEFAULT_TIMES : times;
        validate();
    }

    void validate() {
        for (int day : days) {
            if (day < 1 || day > 32) { // 32 represents the last day of the month
                throw illegalArgument("invalid month day [{}]", day);
            }
        }
        for (DayTimes dayTimes : times) {
            dayTimes.validate();
        }
    }

    public int[] days() {
        return days;
    }

    public DayTimes[] times() {
        return times;
    }

    public Set<String> crons() {
        Set<String> crons = new HashSet<>();
        for (DayTimes times : this.times) {
            String hrsStr = join(",", times.hour);
            String minsStr = join(",", times.minute);
            String daysStr = join(",", this.days);
            daysStr = daysStr.replace("32", "L");
            crons.add("0 " + minsStr + " " + hrsStr + " " + daysStr + " * ?");
        }
        return crons;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MonthTimes that = (MonthTimes) o;

        return Arrays.equals(days, that.days)
            // order doesn't matter
            && newHashSet(times).equals(newHashSet(that.times));
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(days);
        result = 31 * result + Arrays.hashCode(times);
        return result;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "days [%s], times [%s]", join(",", days), Strings.arrayToCommaDelimitedString(times));
    }

    public boolean contains(int day, DayTimes dayTimes) {
        if (Arrays.binarySearch(days, day) == -1) { // days are already sorted
            return false;
        }
        for (DayTimes dayTimes1 : this.times()) {
            if (dayTimes.equals(dayTimes1)) {
                return true;
            }
        }
        return false;
    }

    public boolean intersects(MonthTimes testTimes) {
        for (int day : testTimes.days()) {
            for (DayTimes dayTimes : testTimes.times()) {
                if (contains(day, dayTimes)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(DAY_FIELD.getPreferredName(), days);
        builder.startArray(TIME_FIELD.getPreferredName());
        for (DayTimes dayTimes : times) {
            dayTimes.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static MonthTimes parse(XContentParser parser, XContentParser.Token token) throws IOException, ElasticsearchParseException {
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse month times. expected an object, but found [{}]", token);
        }
        Set<Integer> daysSet = new HashSet<>();
        Set<DayTimes> timesSet = new HashSet<>();
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (DAY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    daysSet.add(parseDayValue(parser, token));
                } else if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        daysSet.add(parseDayValue(parser, token));
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "invalid month day value for [{}] field. expected string/number value or an "
                            + "array of string/number values, but found [{}]",
                        currentFieldName,
                        token
                    );
                }
            } else if (TIME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token != XContentParser.Token.START_ARRAY) {
                    try {
                        timesSet.add(DayTimes.parse(parser, token));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("invalid time value for field [{}] - [{}]", pe, currentFieldName, token);
                    }
                } else {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        try {
                            timesSet.add(DayTimes.parse(parser, token));
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException("invalid time value for field [{}] - [{}]", pe, currentFieldName, token);
                        }
                    }
                }
            }
        }

        return new MonthTimes(CollectionUtils.toArray(daysSet), timesSet.toArray(DayTimes[]::new));
    }

    static int parseDayValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            String value = parser.text().toLowerCase(Locale.ROOT);
            if (LAST.equals(value)) {
                return 32;
            }
            if (FIRST.equals(value)) {
                return 1;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                throw new ElasticsearchParseException("invalid month day value. string value [{}] cannot be", value);
            }
        }
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return parser.intValue();
        }
        throw new ElasticsearchParseException("invalid month day value. expected a string or a number value, but found [{}]", token);
    }

    public static class Builder {

        private final Set<Integer> days = new HashSet<>();
        private final Set<DayTimes> times = new HashSet<>();

        private Builder() {}

        public Builder on(int... days) {
            Arrays.stream(days).forEach(this.days::add);
            return this;
        }

        public Builder at(int hour, int minute) {
            times.add(new DayTimes(hour, minute));
            return this;
        }

        public Builder atRoundHour(int... hours) {
            times.add(new DayTimes(hours, new int[] { 0 }));
            return this;
        }

        public Builder atNoon() {
            times.add(DayTimes.NOON);
            return this;
        }

        public Builder atMidnight() {
            times.add(DayTimes.MIDNIGHT);
            return this;
        }

        public MonthTimes build() {
            return new MonthTimes(CollectionUtils.toArray(days), times.toArray(new DayTimes[times.size()]));
        }
    }
}
