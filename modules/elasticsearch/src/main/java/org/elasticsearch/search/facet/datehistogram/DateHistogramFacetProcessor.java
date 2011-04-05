/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.Chronology;
import org.elasticsearch.common.joda.time.DateTimeField;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.impl.Constants;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class DateHistogramFacetProcessor extends AbstractComponent implements FacetProcessor {

    private final ImmutableMap<String, DateFieldParser> dateFieldParsers;
    private final TObjectIntHashMap<String> rounding = new TObjectIntHashMap<String>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1);

    @Inject public DateHistogramFacetProcessor(Settings settings) {
        super(settings);
        InternalDateHistogramFacet.registerStreams();

        dateFieldParsers = MapBuilder.<String, DateFieldParser>newMapBuilder()
                .put("year", new DateFieldParser.YearOfCentury())
                .put("1y", new DateFieldParser.YearOfCentury())
                .put("month", new DateFieldParser.MonthOfYear())
                .put("1m", new DateFieldParser.MonthOfYear())
                .put("day", new DateFieldParser.DayOfMonth())
                .put("1d", new DateFieldParser.DayOfMonth())
                .put("hour", new DateFieldParser.HourOfDay())
                .put("1h", new DateFieldParser.HourOfDay())
                .put("minute", new DateFieldParser.MinuteOfHour())
                .put("1m", new DateFieldParser.MinuteOfHour())
                .put("second", new DateFieldParser.SecondOfMinute())
                .put("1s", new DateFieldParser.SecondOfMinute())
                .immutableMap();

        rounding.put("floor", MutableDateTime.ROUND_FLOOR);
        rounding.put("ceiling", MutableDateTime.ROUND_CEILING);
        rounding.put("half_even", MutableDateTime.ROUND_HALF_EVEN);
        rounding.put("halfEven", MutableDateTime.ROUND_HALF_EVEN);
        rounding.put("half_floor", MutableDateTime.ROUND_HALF_FLOOR);
        rounding.put("halfFloor", MutableDateTime.ROUND_HALF_FLOOR);
        rounding.put("half_ceiling", MutableDateTime.ROUND_HALF_CEILING);
        rounding.put("halfCeiling", MutableDateTime.ROUND_HALF_CEILING);
    }

    @Override public String[] types() {
        return new String[]{DateHistogramFacet.TYPE, "dateHistogram"};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String keyField = null;
        String valueField = null;
        String valueScript = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        boolean intervalSet = false;
        long interval = 1;
        String sInterval = null;
        MutableDateTime dateTime = new MutableDateTime(DateTimeZone.UTC);
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
                    intervalSet = true;
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        interval = parser.longValue();
                    } else {
                        sInterval = parser.text();
                    }
                } else if ("time_zone".equals(fieldName) || "timeZone".equals(fieldName)) {
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        dateTime.setZone(DateTimeZone.forOffsetHours(parser.intValue()));
                    } else {
                        String text = parser.text();
                        int index = text.indexOf(':');
                        if (index != -1) {
                            // format like -02:30
                            dateTime.setZone(DateTimeZone.forOffsetHoursMinutes(
                                    Integer.parseInt(text.substring(0, index)),
                                    Integer.parseInt(text.substring(index + 1))
                            ));
                        } else {
                            // id, listed here: http://joda-time.sourceforge.net/timezones.html
                            dateTime.setZone(DateTimeZone.forID(text));
                        }
                    }
                } else if ("value_script".equals(fieldName) || "valueScript".equals(fieldName)) {
                    valueScript = parser.text();
                } else if ("order".equals(fieldName) || "comparator".equals(fieldName)) {
                    comparatorType = DateHistogramFacet.ComparatorType.fromString(parser.text());
                } else if ("lang".equals(fieldName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for histogram facet, either using [field] or using [key_field]");
        }

        FieldMapper mapper = context.mapperService().smartNameFieldMapper(keyField);
        if (mapper.fieldDataType() != FieldDataType.DefaultTypes.LONG) {
            throw new FacetPhaseExecutionException(facetName, "(key) field [" + keyField + "] is not of type date");
        }

        if (!intervalSet) {
            throw new FacetPhaseExecutionException(facetName, "[interval] is required to be set for histogram facet");
        }

        // we set the rounding after we set the zone, for it to take affect
        if (sInterval != null) {
            int index = sInterval.indexOf(':');
            if (index != -1) {
                // set with rounding
                DateFieldParser fieldParser = dateFieldParsers.get(sInterval.substring(0, index));
                if (fieldParser == null) {
                    throw new FacetPhaseExecutionException(facetName, "failed to parse interval [" + sInterval + "] with custom rounding using built in intervals (year/month/...)");
                }
                DateTimeField field = fieldParser.parse(dateTime.getChronology());
                int rounding = this.rounding.get(sInterval.substring(index + 1));
                if (rounding == -1) {
                    throw new FacetPhaseExecutionException(facetName, "failed to parse interval [" + sInterval + "], rounding type [" + (sInterval.substring(index + 1)) + "] not found");
                }
                dateTime.setRounding(field, rounding);
            } else {
                DateFieldParser fieldParser = dateFieldParsers.get(sInterval);
                if (fieldParser != null) {
                    DateTimeField field = fieldParser.parse(dateTime.getChronology());
                    dateTime.setRounding(field, MutableDateTime.ROUND_FLOOR);
                } else {
                    // time interval
                    try {
                        interval = TimeValue.parseTimeValue(sInterval, null).millis();
                    } catch (Exception e) {
                        throw new FacetPhaseExecutionException(facetName, "failed to parse interval [" + sInterval + "], tried both as built in intervals (year/month/...) and as a time format");
                    }
                }
            }
        }


        if (valueScript != null) {
            return new ValueScriptDateHistogramFacetCollector(facetName, keyField, scriptLang, valueScript, params, dateTime, interval, comparatorType, context);
        } else if (valueField == null) {
            return new CountDateHistogramFacetCollector(facetName, keyField, dateTime, interval, comparatorType, context);
        } else {
            return new ValueDateHistogramFacetCollector(facetName, keyField, valueField, dateTime, interval, comparatorType, context);
        }
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        InternalDateHistogramFacet first = (InternalDateHistogramFacet) facets.get(0);
        return first.reduce(name, facets);
    }

    static interface DateFieldParser {

        DateTimeField parse(Chronology chronology);

        static class YearOfCentury implements DateFieldParser {
            @Override public DateTimeField parse(Chronology chronology) {
                return chronology.yearOfCentury();
            }
        }

        static class MonthOfYear implements DateFieldParser {
            @Override public DateTimeField parse(Chronology chronology) {
                return chronology.monthOfYear();
            }
        }

        static class DayOfMonth implements DateFieldParser {
            @Override public DateTimeField parse(Chronology chronology) {
                return chronology.dayOfMonth();
            }
        }

        static class HourOfDay implements DateFieldParser {
            @Override public DateTimeField parse(Chronology chronology) {
                return chronology.hourOfDay();
            }
        }

        static class MinuteOfHour implements DateFieldParser {
            @Override public DateTimeField parse(Chronology chronology) {
                return chronology.minuteOfHour();
            }
        }

        static class SecondOfMinute implements DateFieldParser {
            @Override public DateTimeField parse(Chronology chronology) {
                return chronology.secondOfMinute();
            }
        }
    }
}
