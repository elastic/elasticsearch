/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.processor.date;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class DateProcessor implements Processor {

    public static final String TYPE = "date";
    public static final String DEFAULT_TARGET_FIELD = "@timestamp";

    private final DateTimeZone timezone;
    private final Locale locale;
    private final String matchField;
    private final String targetField;
    private final List<String> matchFormats;
    private final List<DateParser> parserList;

    public DateProcessor(String timezone, String locale, String matchField, List<String> matchFormats, String targetField) {
        this.timezone = (timezone == null) ? DateTimeZone.UTC : DateTimeZone.forID(timezone);
        this.locale = Locale.forLanguageTag(locale);
        this.matchField = matchField;
        this.matchFormats = matchFormats;
        this.parserList = matchFormats.stream().map(elt -> getParser(elt)).collect(Collectors.toList());
        this.targetField = (targetField == null) ? DEFAULT_TARGET_FIELD : targetField;
    }

    @Override
    public void execute(Data data) {
        String value = (String) data.getProperty(matchField);
        // TODO(talevy): handle multiple patterns
        // TODO(talevy): handle custom timestamp fields
        String dateAsISO8601 = parserList.get(0).parseDateTime(value).toString();
        data.addField(targetField, dateAsISO8601);
    }

    private DateParser getParser(String format) {
        if ("ISO8601".equals(format)) {
            // TODO(talevy): fallback solution for almost ISO8601
            if (timezone == null) {
                return new ISO8601DateParser();
            } else {
                return new ISO8601DateParser(timezone);
            }
        } else if ("UNIX".equals(format)) {
            return new UnixDateParser(timezone);
        } else if ("UNIX_MS".equals(format)) {
            return new UnixMsDateParser(timezone);
        } else if ("TAI64N".equals(format)) {
            return new TAI64NDateParser(timezone);
        } else {
            if (timezone != null && locale != null) {
                return new JodaPatternDateParser(format, timezone, locale);
            } else if (timezone != null) {
                return new JodaPatternDateParser(format, timezone);
            } else if (locale != null) {
                return new JodaPatternDateParser(format, locale);
            } else {
                return new JodaPatternDateParser(format);
            }
        }
    }

    public static class Builder implements Processor.Builder {

        private String timezone;
        private String locale;
        private String matchField;
        private List<String> matchFormats;
        private String targetField;

        public Builder() {
            matchFormats = new ArrayList<String>();
        }

        public void setTimezone(String timezone) {
            this.timezone = timezone;
        }

        public void setLocale(String locale) {
            this.locale = locale;
        }

        public void setMatchField(String matchField) {
            this.matchField = matchField;
        }

        public void addMatchFormat(String matchFormat) {
            matchFormats.add(matchFormat);
        }

        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }

        @SuppressWarnings("unchecked")
        public void fromMap(Map<String, Object> config) {
            this.timezone = (String) config.get("timezone");
            this.locale = (String) config.get("locale");
            this.matchField = (String) config.get("match_field");
            this.matchFormats = (List<String>) config.get("match_formats");
            this.targetField = (String) config.get("target_field");
        }

        @Override
        public Processor build() {
            return new DateProcessor(timezone, locale, matchField, matchFormats, targetField);
        }

        public static class Factory implements Processor.Builder.Factory {

            @Override
            public Processor.Builder create() {
                return new Builder();
            }
        }

    }

}
