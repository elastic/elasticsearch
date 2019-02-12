/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DailySchedule extends CronnableSchedule {

    public static final String TYPE = "daily";

    public static final DayTimes[] DEFAULT_TIMES = new DayTimes[] { DayTimes.MIDNIGHT };

    private final DayTimes[] times;

    DailySchedule() {
        this(DEFAULT_TIMES);
    }

    DailySchedule(DayTimes... times) {
        super(crons(times));
        this.times = times;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public DayTimes[] times() {
        return times;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean("normalize", false) && times.length == 1) {
            builder.field(Parser.AT_FIELD.getPreferredName(), times[0], params);
        } else {
            builder.startArray(Parser.AT_FIELD.getPreferredName());
            for (DayTimes dayTimes : times) {
                dayTimes.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    public static Builder builder() {
        return new Builder();
    }

    static String[] crons(DayTimes[] times) {
        assert times.length > 0 : "at least one time must be defined";
        List<String> crons = new ArrayList<>(times.length);
        for (DayTimes time : times) {
            crons.add(time.cron());
        }
        return crons.toArray(new String[crons.size()]);
    }

    public static class Parser implements Schedule.Parser<DailySchedule> {

        static final ParseField AT_FIELD = new ParseField("at");

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public DailySchedule parse(XContentParser parser) throws IOException {
            List<DayTimes> times = new ArrayList<>();
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (AT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token != XContentParser.Token.START_ARRAY) {
                        try {
                            times.add(DayTimes.parse(parser, token));
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException("could not parse [{}] schedule. invalid time value for field [{}] - [{}]",
                                    pe, TYPE, currentFieldName, token);
                        }
                    } else {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                times.add(DayTimes.parse(parser, token));
                            } catch (ElasticsearchParseException pe) {
                                throw new ElasticsearchParseException("could not parse [{}] schedule. invalid time value for field [{}] -" +
                                        " [{}]", pe, TYPE, currentFieldName, token);
                            }
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] schedule. unexpected field [{}]", TYPE, currentFieldName);
                }
            }

            return times.isEmpty() ? new DailySchedule() : new DailySchedule(times.toArray(new DayTimes[times.size()]));
        }
    }

    public static class Builder {

        private Set<DayTimes> times = new HashSet<>();

        private Builder() {
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

        public DailySchedule build() {
            return times.isEmpty() ? new DailySchedule() : new DailySchedule(times.toArray(new DayTimes[times.size()]));
        }
    }

}
