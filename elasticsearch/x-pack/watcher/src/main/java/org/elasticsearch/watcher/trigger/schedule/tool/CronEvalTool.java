/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.tool;

import java.util.Arrays;
import java.util.List;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.cli.CommandLine;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.trigger.schedule.Cron;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;

public class CronEvalTool extends Command {

    public static void main(String[] args) throws Exception {
        exit(new CronEvalTool().main(args, Terminal.DEFAULT));
    }

    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss");

    private final OptionSpec<String> countOption;
    private final OptionSpec<String> arguments;

    CronEvalTool() {
        super("Validates and evaluates a cron expression");
        this.countOption = parser.acceptsAll(Arrays.asList("c", "count"),
            "The number of future times this expression will be triggered")
            // TODO: change this to ofType(Integer.class) with jopt-simple 5.0
            // before then it will cause a security exception in tests
            .withRequiredArg().defaultsTo("10");
        this.arguments = parser.nonOptions("expression");
    }

    @Override
    protected int execute(Terminal terminal, OptionSet options) throws Exception {
        int count = Integer.parseInt(countOption.value(options));
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserError(ExitCodes.USAGE, "expecting a single argument that is the cron expression to evaluate");
        }
        execute(terminal, args.get(0), count);
        return ExitCodes.OK;
    }

    void execute(Terminal terminal, String expression, int count) throws Exception {
        Cron.validate(expression);
        terminal.println("Valid!");

        DateTime date = DateTime.now(DateTimeZone.UTC);
        terminal.println("Now is [" + formatter.print(date) + "]");
        terminal.println("Here are the next " + count + " times this cron expression will trigger:");

        Cron cron = new Cron(expression);
        long time = date.getMillis();
        for (int i = 0; i < count; i++) {
            long prevTime = time;
            time = cron.getNextValidTimeAfter(time);
            if (time < 0) {
                throw new UserError(ExitCodes.OK, (i + 1) + ".\t Could not compute future times since ["
                    + formatter.print(prevTime) + "] " + "(perhaps the cron expression only points to times in the past?)");
            }
            terminal.println((i+1) + ".\t" + formatter.print(time));
        }
    }
}
