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
package org.elasticsearch.common.time;

import java.util.Calendar;
import java.util.Locale;
import java.util.spi.CalendarDataProvider;

/**
 * This class is loaded by JVM SPI mechanism in order to provide ISO compatible behaviour for week calculations using java.time.
 * It defines start of the week as Monday and requires 4 days in the first week of the year.
 * This class overrides default behaviour for Locale.ROOT (start of the week Sunday, minimum 1 day in the first week of the year).
 * Java SPI mechanism requires java.locale.providers to contain SPI value (i.e. java.locale.providers=SPI,COMPAT)
 * @see <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO week date</a>
 */
public class IsoCalendarDataProvider extends CalendarDataProvider {

    @Override
    public int getFirstDayOfWeek(Locale locale) {
        return Calendar.MONDAY;
    }

    @Override
    public int getMinimalDaysInFirstWeek(Locale locale) {
        return 4;
    }

    @Override
    public Locale[] getAvailableLocales() {
        return new Locale[]{Locale.ROOT};
    }
}
