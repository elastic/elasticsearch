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

package org.elasticsearch.search.facet.datehistogram;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DateHistogramFacetParser extends AbstractComponent implements FacetParser {

    private final ImmutableMap<String, DateTimeUnit> dateTimeUnits;

    @Inject
    public DateHistogramFacetParser(Settings settings) {
        super(settings);
        InternalDateHistogramFacet.registerStreams();

        dateTimeUnits = MapBuilder.<String, DateTimeUnit>newMapBuilder()
                .put("year", DateTimeUnit.YEAR_OF_CENTURY)
                .put("1y", DateTimeUnit.YEAR_OF_CENTURY)
                .put("quarter", DateTimeUnit.QUARTER)
                .put("1q", DateTimeUnit.QUARTER)
                .put("month", DateTimeUnit.MONTH_OF_YEAR)
                .put("1M", DateTimeUnit.MONTH_OF_YEAR)
                .put("week", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("day", DateTimeUnit.DAY_OF_MONTH)
                .put("1d", DateTimeUnit.DAY_OF_MONTH)
                .put("hour", DateTimeUnit.HOUR_OF_DAY)
                .put("1h", DateTimeUnit.HOUR_OF_DAY)
                .put("minute", DateTimeUnit.MINUTES_OF_HOUR)
                .put("1m", DateTimeUnit.MINUTES_OF_HOUR)
                .put("second", DateTimeUnit.SECOND_OF_MINUTE)
                .put("1s", DateTimeUnit.SECOND_OF_MINUTE)
                .immutableMap();
    }

    @Override
    public String[] types() {
        return new String[]{DateHistogramFacet.TYPE, "dateHistogram"};
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String valueScript = null;
        ScriptService.ScriptType valueScriptType = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        String interval = null;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        boolean preZoneAdjustLargeInterval = false;
        long preOffset = 0;
        long postOffset = 0;
        float factor = 1.0f;
        Chronology chronology = ISOChronology.getInstanceUTC();
        DateHistogramFacet.ComparatorType comparatorType = DateHistogramFacet.ComparatorType.TIME;
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(fieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("field".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("key_field".equals(fieldName) || "keyField".equals(fieldName)) {
                    keyField = parser.text();
                } else if ("value_field".equals(fieldName) || "valueField".equals(fieldName)) {
                    valueField = parser.text();
                } else if ("interval".equals(fieldName)) {
                    interval = parser.text();
                } else if ("time_zone".equals(fieldName) || "timeZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone".equals(fieldName) || "preZone".equals(fieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone_adjust_large_interval".equals(fieldName) || "preZoneAdjustLargeInterval".equals(fieldName)) {
                    preZoneAdjustLargeInterval = parser.booleanValue();
                } else if ("post_zone".equals(fieldName) || "postZone".equals(fieldName)) {
                    postZone = parseZone(parser, token);
                } else if ("pre_offset".equals(fieldName) || "preOffset".equals(fieldName)) {
                    preOffset = parseOffset(parser.text());
                } else if ("post_offset".equals(fieldName) || "postOffset".equals(fieldName)) {
                    postOffset = parseOffset(parser.text());
                } else if ("factor".equals(fieldName)) {
                    factor = parser.floatValue();
                } else if (ScriptService.VALUE_SCRIPT_INLINE.match(fieldName)) {
                    valueScript = parser.text();
                    valueScriptType = ScriptService.ScriptType.INLINE;
                } else if (ScriptService.VALUE_SCRIPT_ID.match(fieldName)) {
                    valueScript = parser.text();
                    valueScriptType = ScriptService.ScriptType.INDEXED;
                } else if (ScriptService.VALUE_SCRIPT_FILE.match(fieldName)) {
                    valueScript = parser.text();
                    valueScriptType = ScriptService.ScriptType.FILE;
                } else if ("order".equals(fieldName) || "comparator".equals(fieldName)) {
                    comparatorType = DateHistogramFacet.ComparatorType.fromString(parser.text());
                } else if (ScriptService.SCRIPT_LANG.match(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (interval == null) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for histogram facet, either using [field] or using [key_field]");
        }

        FieldMapper keyMapper = context.smartNameFieldMapper(keyField);
        if (keyMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "(key) field [" + keyField + "] not found");
        }
        IndexNumericFieldData keyIndexFieldData = context.fieldData().getForField(keyMapper);

        TimeZoneRounding.Builder tzRoundingBuilder;
        DateTimeUnit dateTimeUnit = dateTimeUnits.get(interval);
        if (dateTimeUnit != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(dateTimeUnit);
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
        }

        TimeZoneRounding tzRounding = tzRoundingBuilder
                .preZone(preZone).postZone(postZone)
                .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                .preOffset(preOffset).postOffset(postOffset)
                .factor(factor)
                .build();

        if (valueScript != null) {
            SearchScript script = context.scriptService().search(context.lookup(), scriptLang, valueScript, valueScriptType, params);
            return new ValueScriptDateHistogramFacetExecutor(keyIndexFieldData, script, tzRounding, comparatorType, context.cacheRecycler());
        } else if (valueField != null) {
            FieldMapper valueMapper = context.smartNameFieldMapper(valueField);
            if (valueMapper == null) {
                throw new FacetPhaseExecutionException(facetName, "(value) field [" + valueField + "] not found");
            }
            IndexNumericFieldData valueIndexFieldData = context.fieldData().getForField(valueMapper);
            return new ValueDateHistogramFacetExecutor(keyIndexFieldData, valueIndexFieldData, tzRounding, comparatorType, context.cacheRecycler());
        } else {
            return new CountDateHistogramFacetExecutor(keyIndexFieldData, tzRounding, comparatorType, context.cacheRecycler());
        }
    }

    private long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(offset.substring(beginIndex), null).millis();
    }

    private DateTimeZone parseZone(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return DateTimeZone.forOffsetHours(parser.intValue());
        } else {
            String text = parser.text();
            int index = text.indexOf(':');
            if (index != -1) {
                int beginIndex = text.charAt(0) == '+' ? 1 : 0;
                // format like -02:30
                return DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(beginIndex, index)),
                        Integer.parseInt(text.substring(index + 1))
                );
            } else {
                // id, listed here: http://joda-time.sourceforge.net/timezones.html
                return DateTimeZone.forID(text);
            }
        }
    }

}
