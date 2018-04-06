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

package org.elasticsearch.ingest.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class DateIndexNameProcessor extends AbstractProcessor {

    public static final String TYPE = "date_index_name";

    private final String field;
    private final String indexNamePrefix;
    private final String dateRounding;
    private final String indexNameFormat;
    private final DateTimeZone timezone;
    private final List<Function<String, DateTime>> dateFormats;

    DateIndexNameProcessor(String tag, String field, List<Function<String, DateTime>> dateFormats, DateTimeZone timezone,
                           String indexNamePrefix, String dateRounding, String indexNameFormat) {
        super(tag);
        this.field = field;
        this.timezone = timezone;
        this.dateFormats = dateFormats;
        this.indexNamePrefix = indexNamePrefix;
        this.dateRounding = dateRounding;
        this.indexNameFormat = indexNameFormat;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        // Date can be specified as a string or long:
        Object obj = ingestDocument.getFieldValue(field, Object.class);
        String date = null;
        if (obj != null) {
            // Not use Objects.toString(...) here, because null gets changed to "null" which may confuse some date parsers
            date = obj.toString();
        }

        DateTime dateTime = null;
        Exception lastException = null;
        for (Function<String, DateTime> dateParser : dateFormats) {
            try {
                dateTime = dateParser.apply(date);
            } catch (Exception e) {
                //try the next parser and keep track of the exceptions
                lastException = ExceptionsHelper.useOrSuppress(lastException, e);
            }
        }

        if (dateTime == null) {
            throw new IllegalArgumentException("unable to parse date [" + date + "]", lastException);
        }

        DateTimeFormatter formatter = DateTimeFormat.forPattern(indexNameFormat);
        StringBuilder builder = new StringBuilder()
                .append('<')
                .append(indexNamePrefix)
                    .append('{')
                        .append(formatter.print(dateTime)).append("||/").append(dateRounding)
                            .append('{').append(indexNameFormat).append('|').append(timezone).append('}')
                    .append('}')
                .append('>');
        String dynamicIndexName  = builder.toString();
        ingestDocument.setFieldValue(IngestDocument.MetaData.INDEX.getFieldName(), dynamicIndexName);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    String getIndexNamePrefix() {
        return indexNamePrefix;
    }

    String getDateRounding() {
        return dateRounding;
    }

    String getIndexNameFormat() {
        return indexNameFormat;
    }

    DateTimeZone getTimezone() {
        return timezone;
    }

    List<Function<String, DateTime>> getDateFormats() {
        return dateFormats;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public DateIndexNameProcessor create(Map<String, Processor.Factory> registry, String tag,
                                             Map<String, Object> config) throws Exception {
            String localeString = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "locale");
            String timezoneString = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "timezone");
            DateTimeZone timezone = timezoneString == null ? DateTimeZone.UTC : DateTimeZone.forID(timezoneString);
            Locale locale = Locale.ENGLISH;
            if (localeString != null) {
                try {
                    locale = (new Locale.Builder()).setLanguageTag(localeString).build();
                } catch (IllformedLocaleException e) {
                    throw new IllegalArgumentException("Invalid language tag specified: " + localeString);
                }
            }
            List<String> dateFormatStrings = ConfigurationUtils.readOptionalList(TYPE, tag, config, "date_formats");
            if (dateFormatStrings == null) {
                dateFormatStrings = Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            }
            List<Function<String, DateTime>> dateFormats = new ArrayList<>(dateFormatStrings.size());
            for (String format : dateFormatStrings) {
                DateFormat dateFormat = DateFormat.fromString(format);
                dateFormats.add(dateFormat.getFunction(format, timezone, locale));
            }

            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String indexNamePrefix = ConfigurationUtils.readStringProperty(TYPE, tag, config, "index_name_prefix", "");
            String dateRounding = ConfigurationUtils.readStringProperty(TYPE, tag, config, "date_rounding");
            String indexNameFormat = ConfigurationUtils.readStringProperty(TYPE, tag, config, "index_name_format", "yyyy-MM-dd");
            return new DateIndexNameProcessor(tag, field, dateFormats, timezone, indexNamePrefix, dateRounding, indexNameFormat);
        }
    }

}
