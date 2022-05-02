/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.tool;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

class CronEvalTool extends Command {

    private static final DateFormatter UTC_FORMATTER = DateFormatter.forPattern("EEE, d MMM yyyy HH:mm:ss")
        .withZone(ZoneOffset.UTC)
        .withLocale(Locale.ROOT);

    private static final DateFormatter LOCAL_FORMATTER = DateFormatter.forPattern("EEE, d MMM yyyy HH:mm:ss Z")
        .withZone(ZoneId.systemDefault());

    private final OptionSpec<Integer> countOption;
    private final OptionSpec<String> arguments;
    private final OptionSpec<Void> detailOption;

    CronEvalTool() {
        super("Validates and evaluates a cron expression");
        this.countOption = parser.acceptsAll(Arrays.asList("c", "count"), "The number of future times this expression will be triggered")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(10);
        this.arguments = parser.nonOptions("expression");
        this.detailOption = parser.acceptsAll(Arrays.asList("d", "detail"), "Show detail for invalid cron expression");

        parser.accepts("E", "Unused. Only for compatibility with other CLI tools.").withRequiredArg();
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
        int count = countOption.value(options);
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserException(ExitCodes.USAGE, "expecting a single argument that is the cron expression to evaluate, got " + args);
        }
        boolean printDetail = options.has(detailOption);
        execute(terminal, args.get(0), count, printDetail);
    }

    private void execute(Terminal terminal, String expression, int count, boolean printDetail) throws Exception {
        try {
            Cron.validate(expression);
            terminal.println("Valid!");

            final ZonedDateTime date = ZonedDateTime.now(ZoneOffset.UTC);
            final boolean isLocalTimeUTC = UTC_FORMATTER.zone().equals(LOCAL_FORMATTER.zone());
            if (isLocalTimeUTC) {
                terminal.println("Now is [" + UTC_FORMATTER.format(date) + "] in UTC");
            } else {
                terminal.println(
                    "Now is [" + UTC_FORMATTER.format(date) + "] in UTC, local time is [" + LOCAL_FORMATTER.format(date) + "]"
                );

            }
            terminal.println("Here are the next " + count + " times this cron expression will trigger:");

            Cron cron = new Cron(expression);
            long time = date.toInstant().toEpochMilli();

            for (int i = 0; i < count; i++) {
                long prevTime = time;
                time = cron.getNextValidTimeAfter(time);
                if (time < 0) {
                    if (i == 0) {
                        throw new UserException(
                            ExitCodes.OK,
                            "Could not compute future times since ["
                                + UTC_FORMATTER.format(Instant.ofEpochMilli(prevTime))
                                + "] "
                                + "(perhaps the cron expression only points to "
                                + "times in the"
                                + " "
                                + "past?)"
                        );
                    }
                    break;
                }

                if (isLocalTimeUTC) {
                    terminal.println((i + 1) + ".\t" + UTC_FORMATTER.format(Instant.ofEpochMilli(time)));
                } else {
                    terminal.println((i + 1) + ".\t" + UTC_FORMATTER.format(Instant.ofEpochMilli(time)));
                    terminal.println("\t" + LOCAL_FORMATTER.format(Instant.ofEpochMilli(time)));
                }
            }
        } catch (Exception e) {
            if (printDetail) {
                throw e;
            } else {
                throw new UserException(ExitCodes.OK, e.getMessage() + (e.getCause() == null ? "" : ": " + e.getCause().getMessage()));
            }

        }
    }
}
