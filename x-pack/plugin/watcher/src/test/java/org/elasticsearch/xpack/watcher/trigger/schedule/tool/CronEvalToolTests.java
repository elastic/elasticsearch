/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.tool;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CronEvalToolTests extends CommandTestCase {

    @Override
    protected Command newCommand() {
        return new CronEvalTool();
    }

    public void testParse() throws Exception {
        String countOption = randomBoolean() ? "-c" : "--count";
        int count = randomIntBetween(1, 100);
        String output = execute(countOption, Integer.toString(count), "0 0 0 1-6 * ?");
        assertThat(output, containsString("Here are the next " + count + " times this cron expression will trigger"));
    }

    public void testGetNextValidTimes() throws Exception {
        final int year = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT).get(Calendar.YEAR) + 1;
        {
            String output = execute("0 3 23 8 9 ? " + year);
            assertThat(output, containsString("Here are the next 10 times this cron expression will trigger:"));
            assertThat(output, not(containsString("ERROR")));
            assertThat(output, not(containsString("2.\t")));
        }
        {
            String output = execute("0 3 23 */4 9 ? " + year);
            assertThat(output, containsString("Here are the next 10 times this cron expression will trigger:"));
            assertThat(output, not(containsString("ERROR")));
        }
        {
            Exception expectThrows = expectThrows(Exception.class, () -> execute("0 3 23 */4 9 ? 2017"));
            String message = expectThrows.getMessage();
            assertThat(message, containsString("Could not compute future times since"));
            assertThat(message, containsString("(perhaps the cron expression only points to times in the past?)"));
        }
    }

    // randomized testing sets arbitrary locales and timezones, and we do not care
    // we always have to output in standard locale and independent from timezone
    public void testEnsureDateIsShownInRootLocale() throws Exception {
        String output = execute("-c", "1", "0 0 11 ? * MON-SAT 2040");
        if (ZoneId.systemDefault().equals(ZoneOffset.UTC)) {
            assertThat(output, not(containsString("local time is")));
            long linesStartingWithOne = Arrays.stream(output.split("\n")).filter(s -> s.startsWith("\t")).count();
            assertThat(linesStartingWithOne, is(0L));
        } else {
            // check for header line
            assertThat(output, containsString("] in UTC, local time is"));
            assertThat(output, containsString("Mon, 2 Jan 2040 11:00:00"));
            logger.info(output);
            long linesStartingWithOne = Arrays.stream(output.split("\n")).filter(s -> s.startsWith("\t")).count();
            assertThat(linesStartingWithOne, is(1L));
        }
    }
}
