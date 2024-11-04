/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.TimeZone;

import static org.hamcrest.Matchers.equalTo;

public class TimezoneUtilsTests extends ESTestCase {

    public void testParsingIsCaseInsensitive() {
        TimeZone timeZone = randomTimeZone();
        assertThat(TimezoneUtils.parse(timeZone.getID()), equalTo(timeZone));
        assertThat(TimezoneUtils.parse(timeZone.getID().toLowerCase(Locale.ROOT)), equalTo(timeZone));
        assertThat(TimezoneUtils.parse(timeZone.getID().toUpperCase(Locale.ROOT)), equalTo(timeZone));
    }

    public void testParsingOffsets() {
        TimeZone timeZone = TimeZone.getTimeZone("GMT+01:00");
        assertThat(TimezoneUtils.parse("GMT+01:00"), equalTo(timeZone));
        assertThat(TimezoneUtils.parse("gmt+01:00"), equalTo(timeZone));
        assertThat(TimezoneUtils.parse("GMT+1"), equalTo(timeZone));
        assertThat(TimezoneUtils.parse("+1"), equalTo(timeZone));
    }
}
