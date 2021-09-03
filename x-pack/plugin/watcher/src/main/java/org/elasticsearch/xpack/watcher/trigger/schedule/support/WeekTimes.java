/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.watcher.support.Strings.join;

public class WeekTimes implements Times {

    public static final EnumSet<DayOfWeek> DEFAULT_DAYS = EnumSet.of(DayOfWeek.MONDAY);
    public static final DayTimes[] DEFAULT_TIMES = new DayTimes[] { new DayTimes() };

    private final EnumSet<DayOfWeek> days;
    private final DayTimes[] times;

    public WeekTimes() {
        this(DEFAULT_DAYS, DEFAULT_TIMES);
    }

    public WeekTimes(DayOfWeek day, DayTimes times) {
        this(day, new DayTimes[] { times });
    }

    public WeekTimes(DayOfWeek day, DayTimes[] times) {
        this(EnumSet.of(day), times);
    }

    public WeekTimes(EnumSet<DayOfWeek> days, DayTimes[] times) {
        this.days = days.isEmpty() ? DEFAULT_DAYS : days;
        this.times = times.length == 0 ? DEFAULT_TIMES : times;
    }

    public EnumSet<DayOfWeek> days() {
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
            String daysStr = DayOfWeek.cronPart(this.days);
            crons.add("0 " + minsStr + " " + hrsStr + " ? * " + daysStr);
        }
        return crons;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeekTimes that = (WeekTimes) o;

        return days.equals(that.days)
            // we don't care about order
            && newHashSet(times).equals(newHashSet(that.times));
    }

    @Override
    public int hashCode() {
        int result = days.hashCode();
        result = 31 * result + Arrays.hashCode(times);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DAY_FIELD.getPreferredName(), days);
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

    public static WeekTimes parse(XContentParser parser, XContentParser.Token token) throws IOException, ElasticsearchParseException {
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse week times. expected an object, but found [{}]", token);
        }
        Set<DayOfWeek> daysSet = new HashSet<>();
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
                    throw new ElasticsearchParseException("invalid week day value for [{}] field. expected string/number value or an " +
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
        EnumSet<DayOfWeek> days = daysSet.isEmpty() ? EnumSet.of(DayOfWeek.MONDAY) : EnumSet.copyOf(daysSet);
        DayTimes[] times = timesSet.isEmpty() ? new DayTimes[] { new DayTimes(0, 0) } : timesSet.toArray(new DayTimes[timesSet.size()]);
        return new WeekTimes(days, times);
    }

    static DayOfWeek parseDayValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            return DayOfWeek.resolve(parser.text());
        }
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return DayOfWeek.resolve(parser.intValue());
        }
        throw new ElasticsearchParseException("invalid weekly day value. expected a string or a number value, but found [" + token + "]");
    }

    public static class Builder {

        private final Set<DayOfWeek> days = new HashSet<>();
        private final Set<DayTimes> times = new HashSet<>();

        private Builder() {
        }

        public Builder on(DayOfWeek... days) {
            Collections.addAll(this.days, days);
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

        public WeekTimes build() {
            EnumSet<DayOfWeek> dow = days.isEmpty() ? WeekTimes.DEFAULT_DAYS : EnumSet.copyOf(days);
            return new WeekTimes(dow, times.toArray(new DayTimes[times.size()]));
        }

    }
}
