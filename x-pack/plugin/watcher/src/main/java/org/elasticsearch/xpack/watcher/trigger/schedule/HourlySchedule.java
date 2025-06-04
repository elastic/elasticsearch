/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayTimes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;

public class HourlySchedule extends CronnableSchedule {

    public static final String TYPE = "hourly";

    public static final int[] DEFAULT_MINUTES = new int[] { 0 };

    private final int[] minutes;

    HourlySchedule() {
        this(DEFAULT_MINUTES);
    }

    HourlySchedule(int... minutes) {
        super(cron(minutes));
        this.minutes = minutes;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public int[] minutes() {
        return minutes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean("normalize", false) && minutes.length == 1) {
            builder.field(Parser.MINUTE_FIELD.getPreferredName(), minutes[0]);
        } else {
            builder.array(Parser.MINUTE_FIELD.getPreferredName(), minutes);
        }
        return builder.endObject();
    }

    public static Builder builder() {
        return new Builder();
    }

    static String cron(int[] minutes) {
        assert minutes.length > 0 : "at least one minute must be defined";
        StringBuilder sb = new StringBuilder("0 ");
        for (int i = 0; i < minutes.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            if (validMinute(minutes[i]) == false) {
                throw illegalArgument("invalid hourly minute [{}]. minute must be between 0 and 59 incl.", minutes[i]);
            }
            sb.append(minutes[i]);
        }
        return sb.append(" * * * ?").toString();
    }

    static boolean validMinute(int minute) {
        return minute >= 0 && minute < 60;
    }

    public static class Parser implements Schedule.Parser<HourlySchedule> {

        static final ParseField MINUTE_FIELD = new ParseField("minute");

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public HourlySchedule parse(XContentParser parser) throws IOException {
            List<Integer> minutes = new ArrayList<>();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName == null) {
                    throw new ElasticsearchParseException("could not parse [{}] schedule. unexpected token [{}]", TYPE, token);
                } else if (MINUTE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token.isValue()) {
                        try {
                            minutes.add(DayTimes.parseMinuteValue(parser, token));
                        } catch (ElasticsearchParseException pe) {
                            throw new ElasticsearchParseException(
                                "could not parse [{}] schedule. invalid value for [{}]",
                                pe,
                                TYPE,
                                currentFieldName
                            );
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                minutes.add(DayTimes.parseMinuteValue(parser, token));
                            } catch (ElasticsearchParseException pe) {
                                throw new ElasticsearchParseException(
                                    "could not parse [{}] schedule. invalid value for [{}]",
                                    pe,
                                    TYPE,
                                    currentFieldName
                                );
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException(
                            "could not parse [{}] schedule. invalid value for [{}]. "
                                + "expected either string/value or an array of string/number values, but found [{}]",
                            TYPE,
                            currentFieldName,
                            token
                        );
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse [{}] schedule. unexpected field [{}]", TYPE, currentFieldName);
                }
            }

            return minutes.isEmpty() ? new HourlySchedule() : new HourlySchedule(CollectionUtils.toArray(minutes));
        }

    }

    public static class Builder {

        private Set<Integer> minutes = new HashSet<>();

        private Builder() {}

        public Builder minutes(int... minutes) {
            for (int minute : minutes) {
                this.minutes.add(minute);
            }
            return this;
        }

        public HourlySchedule build() {
            return minutes.isEmpty() ? new HourlySchedule() : new HourlySchedule(CollectionUtils.toArray(minutes));
        }
    }
}
