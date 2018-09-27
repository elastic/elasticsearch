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

import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DateFormattersTests extends ESTestCase {

    public void testEpochMilliParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");

        DateTimeParseException e = expectThrows(DateTimeParseException.class, () -> formatter.parse("invalid"));
        assertThat(e.getMessage(), containsString("invalid number"));

        // different zone, should still yield the same output, as epoch is time zone independent
        ZoneId zoneId = randomZone();
        DateFormatter zonedFormatter = formatter.withZone(zoneId);

        // test with negative and non negative values
        assertThatSameDateTime(formatter, zonedFormatter, randomNonNegativeLong() * -1);
        assertThatSameDateTime(formatter, zonedFormatter, randomNonNegativeLong());
        assertThatSameDateTime(formatter, zonedFormatter, 0);
        assertThatSameDateTime(formatter, zonedFormatter, -1);
        assertThatSameDateTime(formatter, zonedFormatter, 1);

        // format() output should be equal as well
        assertSameFormat(formatter, randomNonNegativeLong() * -1);
        assertSameFormat(formatter, randomNonNegativeLong());
        assertSameFormat(formatter, 0);
        assertSameFormat(formatter, -1);
        assertSameFormat(formatter, 1);
    }

    public void testEpochMilliParsersWithDifferentFormatters() {
        DateFormatter formatter = DateFormatters.forPattern("strict_date_optional_time||epoch_millis");
        TemporalAccessor accessor = formatter.parse("123");
        assertThat(DateFormatters.toZonedDateTime(accessor).toInstant().toEpochMilli(), is(123L));
        assertThat(formatter.pattern(), is("strict_date_optional_time||epoch_millis"));
    }

    public void testLocales() {
        assertThat(DateFormatters.forPattern("strict_date_optional_time").getLocale(), is(Locale.ROOT));
        Locale locale = randomLocale(random());
        assertThat(DateFormatters.forPattern("strict_date_optional_time").withLocale(locale).getLocale(), is(locale));
        assertThat(DateFormatters.forPattern("epoch_millis").withLocale(locale).getLocale(), is(locale));
        assertThat(DateFormatters.forPattern("epoch_second").withLocale(locale).getLocale(), is(locale));
    }

    public void testTimeZones() {
        // zone is null by default due to different behaviours between java8 and above
        assertThat(DateFormatters.forPattern("strict_date_optional_time").getZone(), is(nullValue()));
        ZoneId zoneId = randomZone();
        assertThat(DateFormatters.forPattern("strict_date_optional_time").withZone(zoneId).getZone(), is(zoneId));
        assertThat(DateFormatters.forPattern("epoch_millis").withZone(zoneId).getZone(), is(zoneId));
        assertThat(DateFormatters.forPattern("epoch_second").withZone(zoneId).getZone(), is(zoneId));
    }

    public void testEqualsAndHashcode() {
        assertThat(DateFormatters.forPattern("strict_date_optional_time"),
            sameInstance(DateFormatters.forPattern("strict_date_optional_time")));
        assertThat(DateFormatters.forPattern("YYYY"), equalTo(DateFormatters.forPattern("YYYY")));
        assertThat(DateFormatters.forPattern("YYYY").hashCode(),
            is(DateFormatters.forPattern("YYYY").hashCode()));

        // different timezone, thus not equals
        assertThat(DateFormatters.forPattern("YYYY").withZone(ZoneId.of("CET")), not(equalTo(DateFormatters.forPattern("YYYY"))));

        // different locale, thus not equals
        assertThat(DateFormatters.forPattern("YYYY").withLocale(randomLocale(random())),
            not(equalTo(DateFormatters.forPattern("YYYY"))));

        // different pattern, thus not equals
        assertThat(DateFormatters.forPattern("YYYY"), not(equalTo(DateFormatters.forPattern("YY"))));

        DateFormatter epochSecondFormatter = DateFormatters.forPattern("epoch_second");
        assertThat(epochSecondFormatter, sameInstance(DateFormatters.forPattern("epoch_second")));
        assertThat(epochSecondFormatter.hashCode(), is(DateFormatters.forPattern("epoch_second").hashCode()));
        assertThat(epochSecondFormatter, not(equalTo(DateFormatters.forPattern("epoch_second").withLocale(randomLocale(random())))));
        assertThat(epochSecondFormatter, not(equalTo(DateFormatters.forPattern("epoch_second").withZone(ZoneId.of("CET")))));

        DateFormatter epochMillisFormatter = DateFormatters.forPattern("epoch_millis");
        assertThat(epochMillisFormatter.hashCode(), is(DateFormatters.forPattern("epoch_millis").hashCode()));
        assertThat(epochMillisFormatter, sameInstance(DateFormatters.forPattern("epoch_millis")));
        assertThat(epochMillisFormatter, not(equalTo(DateFormatters.forPattern("epoch_millis").withLocale(randomLocale(random())))));
        assertThat(epochMillisFormatter, not(equalTo(DateFormatters.forPattern("epoch_millis").withZone(ZoneId.of("CET")))));
    }

    private void assertThatSameDateTime(DateFormatter formatter, DateFormatter zonedFormatter, long millis) {
        String millisAsString = String.valueOf(millis);
        ZonedDateTime formatterZonedDateTime = DateFormatters.toZonedDateTime(formatter.parse(millisAsString));
        ZonedDateTime zonedFormatterZonedDateTime = DateFormatters.toZonedDateTime(zonedFormatter.parse(millisAsString));
        assertThat(formatterZonedDateTime.toInstant().toEpochMilli(), is(zonedFormatterZonedDateTime.toInstant().toEpochMilli()));
    }

    private void assertSameFormat(DateFormatter formatter, long millis) {
        String millisAsString = String.valueOf(millis);
        TemporalAccessor accessor = formatter.parse(millisAsString);
        assertThat(millisAsString, is(formatter.format(accessor)));
    }
}
