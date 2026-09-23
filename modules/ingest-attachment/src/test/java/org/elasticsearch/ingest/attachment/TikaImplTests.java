/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.attachment;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.test.ESTestCase;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TikaImplTests extends ESTestCase {

    public void testTikaLoads() throws Exception {
        Class.forName("org.elasticsearch.ingest.attachment.TikaImpl");
    }

    public void testNormalizeDates() throws Exception {
        Metadata metadata = new Metadata();
        Calendar yearZero = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        yearZero.clear();
        yearZero.set(0, Calendar.JANUARY, 1);
        metadata.set(TikaCoreProperties.CREATED, yearZero);
        metadata.set(TikaCoreProperties.MODIFIED, "2009-04-16T11:32:02");
        metadata.set(TikaCoreProperties.PRINT_DATE, "2010-05-09T12:34:38Z");
        metadata.set(TikaCoreProperties.METADATA_DATE, "2010-05-09T21:34:38+0200");
        metadata.set(TikaCoreProperties.CREATOR_TOOL, "-not a date");

        TikaImpl.normalizeDates(metadata);

        assertThat(metadata.get(TikaCoreProperties.CREATED), nullValue());
        assertThat(metadata.get(TikaCoreProperties.MODIFIED), equalTo("2009-04-16T11:32:02Z"));
        assertThat(metadata.get(TikaCoreProperties.PRINT_DATE), equalTo("2010-05-09T12:34:38Z"));
        assertThat(metadata.get(TikaCoreProperties.METADATA_DATE), equalTo("2010-05-09T19:34:38Z"));
        assertThat(metadata.get(TikaCoreProperties.CREATOR_TOOL), equalTo("-not a date"));
    }

    public void testNormalizeDatesWithColonOffsetAndFraction() throws Exception {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CREATED, "2012-04-27T10:22:43.123-07:00");
        metadata.set(TikaCoreProperties.MODIFIED, "2017-06-18T17:58:20.225347530");
        metadata.set(TikaCoreProperties.PRINT_DATE, "2012-11-30");

        TikaImpl.normalizeDates(metadata);

        assertThat(metadata.get(TikaCoreProperties.CREATED), equalTo("2012-04-27T17:22:43Z"));
        assertThat(metadata.get(TikaCoreProperties.MODIFIED), equalTo("2017-06-18T17:58:20Z"));
        assertThat(metadata.get(TikaCoreProperties.PRINT_DATE), equalTo("2012-11-30"));
    }

}
