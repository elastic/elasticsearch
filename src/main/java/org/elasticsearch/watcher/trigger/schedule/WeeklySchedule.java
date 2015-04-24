/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.trigger.schedule.support.WeekTimes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class WeeklySchedule extends CronnableSchedule {

    public static final String TYPE = "weekly";

    public static final WeekTimes[] DEFAULT_TIMES = new WeekTimes[] { new WeekTimes() };

    private final WeekTimes[] times;

    WeeklySchedule() {
        this(DEFAULT_TIMES);
    }

    WeeklySchedule(WeekTimes... times) {
        super(crons(times));
        this.times = times;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public WeekTimes[] times() {
        return times;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (params.paramAsBoolean("normalize", false) && times.length == 1) {
            return times[0].toXContent(builder, params);
        }
        builder.startArray();
        for (WeekTimes weekTimes : times) {
            weekTimes.toXContent(builder, params);
        }
        return builder.endArray();
    }

    public static Builder builder() {
        return new Builder();
    }

    static String[] crons(WeekTimes[] times) {
        assert times.length > 0 : "at least one time must be defined";
        List<String> crons = new ArrayList<>(times.length);
        for (WeekTimes time : times) {
            crons.addAll(time.crons());
        }
        return crons.toArray(new String[crons.size()]);
    }

    public static class Parser implements Schedule.Parser<WeeklySchedule> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public WeeklySchedule parse(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                try {
                    return new WeeklySchedule(WeekTimes.parse(parser, parser.currentToken()));
                } catch (WeekTimes.ParseException pe) {
                    throw new WatcherSettingsException("could not parse [weekly] schedule. invalid weekly times", pe);
                }
            }
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                List<WeekTimes> times = new ArrayList<>();
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    try {
                        times.add(WeekTimes.parse(parser, token));
                    } catch (WeekTimes.ParseException pe) {
                        throw new WatcherSettingsException("could not parse [weekly] schedule. invalid weekly times", pe);
                    }
                }
                return times.isEmpty() ? new WeeklySchedule() : new WeeklySchedule(times.toArray(new WeekTimes[times.size()]));
            }
            throw new WatcherSettingsException("could not parse [weekly] schedule. expected either an object or an array " +
                    "of objects representing weekly times, but found [" + parser.currentToken() + "] instead");
        }
    }

    public static class Builder {

        private final Set<WeekTimes> times = new HashSet<>();

        public Builder time(WeekTimes time) {
            times.add(time);
            return this;
        }

        public Builder time(WeekTimes.Builder time) {
            return time(time.build());
        }

        public WeeklySchedule build() {
            return times.isEmpty() ? new WeeklySchedule() : new WeeklySchedule(times.toArray(new WeekTimes[times.size()]));
        }

    }


}
