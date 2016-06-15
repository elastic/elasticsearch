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
package org.elasticsearch.benchmark;

import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

@SuppressWarnings("unused") //invoked by benchmarking framework
@State(Scope.Benchmark)
public class DateBenchmark {
    private long instant;

    private MutableDateTime jodaDate = new MutableDateTime(0, DateTimeZone.UTC);

    private Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

    private ZonedDateTime javaDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0L), ZoneOffset.UTC);

    private TemporalAmount diff = Duration.ofMillis(1L);

    @Benchmark
    public int mutableDateTimeSetMillisGetDayOfMonth() {
        jodaDate.setMillis(instant++);
        return jodaDate.getDayOfMonth();
    }

    @Benchmark
    public int calendarSetMillisGetDayOfMonth() {
        calendar.setTimeInMillis(instant++);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    @Benchmark
    public int javaDateTimeSetMillisGetDayOfMonth() {
        // all classes in java.time are immutable, we have to use a new instance
        javaDateTime = javaDateTime.plus(diff);
        return javaDateTime.getDayOfMonth();
    }
}
