/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class TimezoneUtilsTests extends ESTestCase {

    public void testExpectedFormatParsing() {
        assertThat(TimezoneUtils.parse("Europe/London").getId(), equalTo("Europe/London"));
        assertThat(TimezoneUtils.parse("+1").getId(), equalTo("+01:00"));
        assertThat(TimezoneUtils.parse("GMT+01:00").getId(), equalTo("GMT+01:00"));
    }

    public void testParsingIsCaseInsensitive() {
        ZoneId timeZone = randomTimeZone().toZoneId();
        assertThat(TimezoneUtils.parse(timeZone.getId()), equalTo(timeZone));
        assertThat(TimezoneUtils.parse(timeZone.getId().toLowerCase(Locale.ROOT)), equalTo(timeZone));
        assertThat(TimezoneUtils.parse(timeZone.getId().toUpperCase(Locale.ROOT)), equalTo(timeZone));
    }

    public void testParsingOffsets() {
        ZoneId timeZone = ZoneId.of("GMT+01:00");
        assertThat(TimezoneUtils.parse("GMT+01:00"), equalTo(timeZone));
        assertThat(TimezoneUtils.parse("gmt+01:00"), equalTo(timeZone));
        assertThat(TimezoneUtils.parse("GMT+1"), equalTo(timeZone));

        assertThat(TimezoneUtils.parse("+1"), equalTo(ZoneId.of("+01:00")));
    }
}
