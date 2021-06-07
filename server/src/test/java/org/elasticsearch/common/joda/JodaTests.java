/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.ZoneOffset;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;


public class JodaTests extends ESTestCase {

    public void testBasicTTimePattern() {
        DateFormatter formatter1 = Joda.forPattern("basic_t_time");
        assertEquals(formatter1.pattern(), "basic_t_time");
        assertEquals(formatter1.zone(), ZoneOffset.UTC);

        DateFormatter formatter2 = Joda.forPattern("basic_t_time");
        assertEquals(formatter2.pattern(), "basic_t_time");
        assertEquals(formatter2.zone(), ZoneOffset.UTC);

        DateTime dt = new DateTime(2004, 6, 9, 10, 20, 30, 40, DateTimeZone.UTC);
        assertEquals("T102030.040Z", formatter1.formatJoda(dt));
        assertEquals("T102030.040Z", formatter1.formatJoda(dt));

        expectThrows(IllegalArgumentException.class, () -> Joda.forPattern("basic_t_Time"));
        expectThrows(IllegalArgumentException.class, () -> Joda.forPattern("basic_T_Time"));
        expectThrows(IllegalArgumentException.class, () -> Joda.forPattern("basic_T_time"));
    }

    public void testEqualsAndHashcode() {
        String format = randomFrom("yyyy/MM/dd HH:mm:ss", "basic_t_time");
        JodaDateFormatter first = Joda.forPattern(format);
        JodaDateFormatter second = Joda.forPattern(format);
        JodaDateFormatter third = Joda.forPattern(" HH:mm:ss, yyyy/MM/dd");

        assertThat(first, is(second));
        assertThat(second, is(first));
        assertThat(first, is(not(third)));
        assertThat(second, is(not(third)));

        assertThat(first.hashCode(), is(second.hashCode()));
        assertThat(second.hashCode(), is(first.hashCode()));
        assertThat(first.hashCode(), is(not(third.hashCode())));
        assertThat(second.hashCode(), is(not(third.hashCode())));
    }
}
