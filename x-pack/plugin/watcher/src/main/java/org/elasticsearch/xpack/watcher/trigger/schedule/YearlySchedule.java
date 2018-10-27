/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.YearTimes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YearlySchedule extends CronnableSchedule {

    public static final String TYPE = "yearly";

    public static final YearTimes[] DEFAULT_TIMES = new YearTimes[] { new YearTimes() };

    private final YearTimes[] times;

    YearlySchedule() {
        this(DEFAULT_TIMES);
    }

    YearlySchedule(YearTimes... times) {
        super(crons(times));
        this.times = times;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public YearTimes[] times() {
        return times;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (params.paramAsBoolean("normalize", false) && times.length == 1) {
            return times[0].toXContent(builder, params);
        }
        builder.startArray();
        for (YearTimes yearTimes : times) {
            yearTimes.toXContent(builder, params);
        }
        return builder.endArray();
    }

    public static Builder builder() {
        return new Builder();
    }

    static String[] crons(YearTimes[] times) {
        assert times.length > 0 : "at least one time must be defined";
        Set<String> crons = new HashSet<>(times.length);
        for (YearTimes time : times) {
            crons.addAll(time.crons());
        }
        return crons.toArray(new String[crons.size()]);
    }

    public static class Parser implements Schedule.Parser<YearlySchedule> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public YearlySchedule parse(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                try {
                    return new YearlySchedule(YearTimes.parse(parser, parser.currentToken()));
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException("could not parse [{}] schedule. invalid year times", pe, TYPE);
                }
            }
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                List<YearTimes> times = new ArrayList<>();
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    try {
                        times.add(YearTimes.parse(parser, token));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse [{}] schedule. invalid year times", pe, TYPE);
                    }
                }
                return times.isEmpty() ? new YearlySchedule() : new YearlySchedule(times.toArray(new YearTimes[times.size()]));
            }
            throw new ElasticsearchParseException("could not parse [{}] schedule. expected either an object or an array " +
                    "of objects representing year times, but found [{}] instead", TYPE, parser.currentToken());
        }
    }

    public static class Builder {

        private final Set<YearTimes> times = new HashSet<>();

        private Builder() {
        }

        public Builder time(YearTimes time) {
            times.add(time);
            return this;
        }

        public Builder time(YearTimes.Builder builder) {
            return time(builder.build());
        }

        public YearlySchedule build() {
            return times.isEmpty() ? new YearlySchedule() : new YearlySchedule(times.toArray(new YearTimes[times.size()]));
        }
    }

}
