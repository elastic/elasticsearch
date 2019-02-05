/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.tool;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.LoggingAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.xpack.core.scheduler.Cron;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class CronEvalTool extends LoggingAwareCommand {

    public static void main(String[] args) throws Exception {
        exit(new CronEvalTool().main(args, Terminal.DEFAULT));
    }

    private static final DateTimeFormatter UTC_FORMATTER = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss")
        .withZone(DateTimeZone.UTC).withLocale(Locale.ROOT);
    private static final DateTimeFormatter LOCAL_FORMATTER = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss Z")
        .withZone(DateTimeZone.forTimeZone(null));

    private final OptionSpec<Integer> countOption;
    private final OptionSpec<String> arguments;

    CronEvalTool() {
        super("Validates and evaluates a cron expression");
        this.countOption = parser.acceptsAll(Arrays.asList("c", "count"),
            "The number of future times this expression will be triggered")
            .withRequiredArg().ofType(Integer.class).defaultsTo(10);
        this.arguments = parser.nonOptions("expression");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        int count = countOption.value(options);
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserException(ExitCodes.USAGE, "expecting a single argument that is the cron expression to evaluate");
        }
        execute(terminal, args.get(0), count);
    }

    void execute(Terminal terminal, String expression, int count) throws Exception {
        Cron.validate(expression);
        terminal.println("Valid!");

        final DateTime date = DateTime.now(DateTimeZone.UTC);
        final boolean isLocalTimeUTC = UTC_FORMATTER.getZone().equals(LOCAL_FORMATTER.getZone());
        if (isLocalTimeUTC) {
            terminal.println("Now is [" + UTC_FORMATTER.print(date) + "] in UTC");
        } else {
            terminal.println("Now is [" + UTC_FORMATTER.print(date) + "] in UTC, local time is [" + LOCAL_FORMATTER.print(date) + "]");

        }
        terminal.println("Here are the next " + count + " times this cron expression will trigger:");

        Cron cron = new Cron(expression);
        long time = date.getMillis();

        for (int i = 0; i < count; i++) {
            long prevTime = time;
            time = cron.getNextValidTimeAfter(time);
            if (time < 0) {
                if (i == 0) {
                    throw new UserException(ExitCodes.OK, "Could not compute future times since ["
                            + UTC_FORMATTER.print(prevTime) + "] " + "(perhaps the cron expression only points to times in the past?)");
                }
                break;
            }

            if (isLocalTimeUTC) {
                terminal.println((i + 1) + ".\t" + UTC_FORMATTER.print(time));
            } else {
                terminal.println((i + 1) + ".\t" + UTC_FORMATTER.print(time));
                terminal.println("\t" + LOCAL_FORMATTER.print(time));
            }
        }
    }
}
