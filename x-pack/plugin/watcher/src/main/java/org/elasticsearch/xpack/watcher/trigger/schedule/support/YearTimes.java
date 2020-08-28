/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.xpack.watcher.support.Strings.join;

public class YearTimes implements Times {

    public static final EnumSet<Month> DEFAULT_MONTHS = EnumSet.of(Month.JANUARY);
    public static final int[] DEFAULT_DAYS = new int[] { 1 };
    public static final DayTimes[] DEFAULT_TIMES = new DayTimes[] { new DayTimes() };

    private final EnumSet<Month> months;
    private final int[] days;
    private final DayTimes[] times;

    public YearTimes() {
        this(DEFAULT_MONTHS, DEFAULT_DAYS, DEFAULT_TIMES);
    }

    public YearTimes(EnumSet<Month> months, int[] days, DayTimes[] times) {
        this.months = months.isEmpty() ? DEFAULT_MONTHS : months;
        this.days = days.length == 0 ? DEFAULT_DAYS : days;
        Arrays.sort(this.days);
        this.times = times.length == 0 ? DEFAULT_TIMES : times;
        validate();
    }

    void validate() {
        for (int day : days) {
            if (day < 1 || day > 32) { //32 represents the last day of the month
                throw illegalArgument("invalid month day [{}]", day);
            }
        }
        for (DayTimes dayTimes : times) {
            dayTimes.validate();
        }
    }

    public EnumSet<Month> months() {
        return months;
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
            String monthsStr = Strings.collectionToCommaDelimitedString(months);
            String expression = "0 " + minsStr + " " + hrsStr + " " + daysStr + " " + monthsStr + " ?";
            crons.add(expression);
        }
        return crons;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YearTimes that = (YearTimes) o;

        if (!Arrays.equals(days, that.days)) return false;
        if (!months.equals(that.months)) return false;
        // order doesn't matter
        if (!newHashSet(times).equals(newHashSet(that.times))) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = months.hashCode();
        result = 31 * result + Arrays.hashCode(days);
        result = 31 * result + Arrays.hashCode(times);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                Locale.ROOT,
                "months [%s], days [%s], times [%s]",
                Strings.collectionToCommaDelimitedString(months),
                join(",", days),
                Strings.arrayToCommaDelimitedString(times)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MONTH_FIELD.getPreferredName(), months);
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

    public static YearTimes parse(XContentParser parser, XContentParser.Token token) throws IOException, ElasticsearchParseException {
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse year times. expected an object, but found [{}]", token);
        }
        Set<Month> monthsSet = new HashSet<>();
        Set<Integer> daysSet = new HashSet<>();
        Set<DayTimes> timesSet = new HashSet<>();
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (MONTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    monthsSet.add(parseMonthValue(parser, token));
                } else if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        monthsSet.add(parseMonthValue(parser, token));
                    }
                } else {
                    throw new ElasticsearchParseException("invalid year month value for [{}] field. expected string/number value or an " +
                            "array of string/number values, but found [{}]", currentFieldName, token);
                }
            } else if (DAY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    daysSet.add(MonthTimes.parseDayValue(parser, token));
                } else if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        daysSet.add(MonthTimes.parseDayValue(parser, token));
                    }
                } else {
                    throw new ElasticsearchParseException("invalid year day value for [{}] field. expected string/number value or an " +
                            "array of string/number values, but found [{}]", currentFieldName, token);
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
        EnumSet<Month> months = monthsSet.isEmpty() ? DEFAULT_MONTHS : EnumSet.copyOf(monthsSet);
        int[] days = daysSet.isEmpty() ? DEFAULT_DAYS : CollectionUtils.toArray(daysSet);
        DayTimes[] times = timesSet.isEmpty() ? new DayTimes[] { new DayTimes(0, 0) } : timesSet.toArray(new DayTimes[timesSet.size()]);
        return new YearTimes(months, days, times);
    }

    static Month parseMonthValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            return Month.resolve(parser.text());
        }
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return Month.resolve(parser.intValue());
        }
        throw new ElasticsearchParseException("invalid year month value. expected a string or a number value, but found [{}]", token);
    }

    public static class Builder {

        private final Set<Month> months = new HashSet<>();
        private final Set<Integer> days = new HashSet<>();
        private final Set<DayTimes> times = new HashSet<>();

        private Builder() {
        }

        public Builder in(Month... months) {
            Collections.addAll(this.months, months);
            return this;
        }

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

        public YearTimes build() {
            return new YearTimes(EnumSet.copyOf(months), CollectionUtils.toArray(days), times.toArray(new DayTimes[times.size()]));
        }
    }
}
