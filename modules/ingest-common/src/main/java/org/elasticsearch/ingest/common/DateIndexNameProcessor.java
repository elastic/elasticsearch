/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public final class DateIndexNameProcessor extends AbstractProcessor {

    public static final String TYPE = "date_index_name";

    private final String field;
    private final TemplateScript.Factory indexNamePrefixTemplate;
    private final TemplateScript.Factory dateRoundingTemplate;
    private final TemplateScript.Factory indexNameFormatTemplate;
    private final ZoneId timezone;
    private final List<Function<String, ZonedDateTime>> dateFormats;

    DateIndexNameProcessor(String tag, String description, String field, List<Function<String, ZonedDateTime>> dateFormats,
                           ZoneId timezone, TemplateScript.Factory indexNamePrefixTemplate, TemplateScript.Factory dateRoundingTemplate,
                           TemplateScript.Factory indexNameFormatTemplate) {
        super(tag, description);
        this.field = field;
        this.timezone = timezone;
        this.dateFormats = dateFormats;
        this.indexNamePrefixTemplate = indexNamePrefixTemplate;
        this.dateRoundingTemplate = dateRoundingTemplate;
        this.indexNameFormatTemplate = indexNameFormatTemplate;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        // Date can be specified as a string or long:
        Object obj = ingestDocument.getFieldValue(field, Object.class);
        String date = null;
        if (obj != null) {
            // Not use Objects.toString(...) here, because null gets changed to "null" which may confuse some date parsers
            date = obj.toString();
        }

        ZonedDateTime dateTime = null;
        Exception lastException = null;
        for (Function<String, ZonedDateTime> dateParser : dateFormats) {
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
        String indexNamePrefix = ingestDocument.renderTemplate(indexNamePrefixTemplate);
        String indexNameFormat = ingestDocument.renderTemplate(indexNameFormatTemplate);
        String dateRounding = ingestDocument.renderTemplate(dateRoundingTemplate);

        DateFormatter formatter = DateFormatter.forPattern(indexNameFormat);
        // use UTC instead of Z is string representation of UTC, so behaviour is the same between 6.x and 7
        String zone = timezone.equals(ZoneOffset.UTC) ? "UTC" : timezone.getId();
        StringBuilder builder = new StringBuilder()
                .append('<')
                .append(indexNamePrefix)
                    .append('{')
                        .append(formatter.format(dateTime)).append("||/").append(dateRounding)
                            .append('{').append(indexNameFormat).append('|').append(zone).append('}')
                    .append('}')
                .append('>');
        String dynamicIndexName  = builder.toString();
        ingestDocument.setFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), dynamicIndexName);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    TemplateScript.Factory getIndexNamePrefixTemplate() {
        return indexNamePrefixTemplate;
    }

    TemplateScript.Factory getDateRoundingTemplate() {
        return dateRoundingTemplate;
    }

    TemplateScript.Factory getIndexNameFormatTemplate() {
        return indexNameFormatTemplate;
    }

    ZoneId getTimezone() {
        return timezone;
    }

    List<Function<String, ZonedDateTime>> getDateFormats() {
        return dateFormats;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public DateIndexNameProcessor create(Map<String, Processor.Factory> registry, String tag,
                                             String description, Map<String, Object> config) throws Exception {
            String localeString = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "locale");
            String timezoneString = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "timezone");
            ZoneId timezone = timezoneString == null ? ZoneOffset.UTC : ZoneId.of(timezoneString);
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
                dateFormatStrings = Collections.singletonList("yyyy-MM-dd'T'HH:mm:ss.SSSXX");
            }
            List<Function<String, ZonedDateTime>> dateFormats = new ArrayList<>(dateFormatStrings.size());
            for (String format : dateFormatStrings) {
                DateFormat dateFormat = DateFormat.fromString(format);
                dateFormats.add(dateFormat.getFunction(format, timezone, locale));
            }

            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String indexNamePrefix = ConfigurationUtils.readStringProperty(TYPE, tag, config, "index_name_prefix", "");
            TemplateScript.Factory indexNamePrefixTemplate =
                ConfigurationUtils.compileTemplate(TYPE, tag, "index_name_prefix", indexNamePrefix, scriptService);
            String dateRounding = ConfigurationUtils.readStringProperty(TYPE, tag, config, "date_rounding");
            TemplateScript.Factory dateRoundingTemplate =
                ConfigurationUtils.compileTemplate(TYPE, tag, "date_rounding", dateRounding, scriptService);
            String indexNameFormat = ConfigurationUtils.readStringProperty(TYPE, tag, config, "index_name_format", "yyyy-MM-dd");
            TemplateScript.Factory indexNameFormatTemplate =
                ConfigurationUtils.compileTemplate(TYPE, tag, "index_name_format", indexNameFormat, scriptService);
            return new DateIndexNameProcessor(tag, description, field, dateFormats, timezone, indexNamePrefixTemplate,
                dateRoundingTemplate, indexNameFormatTemplate);
        }
    }

}
