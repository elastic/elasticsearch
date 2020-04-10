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
