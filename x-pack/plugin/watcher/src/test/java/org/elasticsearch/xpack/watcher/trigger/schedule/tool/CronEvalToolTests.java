/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.tool;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

public class CronEvalToolTests extends CommandTestCase {
    @Override
    protected Command newCommand() {
        return new CronEvalTool();
    }

    public void testParse() throws Exception {
        String countOption = randomBoolean() ? "-c" : "--count";
        int count = randomIntBetween(1, 100);
        String output = execute(countOption, Integer.toString(count), "0 0 0 1-6 * ?");
        assertTrue(output, output.contains("Here are the next " + count + " times this cron expression will trigger"));
    }

    public void testGetNextValidTimes() throws Exception {
        {
            String output = execute(
                    "0 3 23 8 9 ? " + (Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT).get(Calendar.YEAR) + 1));
            assertTrue(output, output.contains("There are 1 times this cron expression will trigger:"));
            assertFalse(output.contains("ERROR"));
            assertFalse(output.contains("2.\t"));
        }
        {
            String output = execute(
                    "0 3 23 */4 9 ? " + (Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT).get(Calendar.YEAR) + 1));
            assertTrue(output, output.contains("There are 8 times this cron expression will trigger:"));
            assertFalse(output, output.contains("Here are the next 10 times this cron expression will trigger:"));
            assertFalse(output, output.contains("ERROR"));
        }
        {
            Exception expectThrows = expectThrows(Exception.class, () -> execute(
                    "0 3 23 */4 9 ? " + (Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT).get(Calendar.YEAR) - 1)));
            String message = expectThrows.getMessage();
            assertTrue(message, message.contains("Could not compute future times since"));
            assertTrue(message, message.contains("(perhaps the cron expression only points to times in the past?)"));
        }
    }
}
