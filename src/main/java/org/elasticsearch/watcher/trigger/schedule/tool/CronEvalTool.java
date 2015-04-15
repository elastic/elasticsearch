/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.tool;

import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.cli.commons.CommandLine;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.trigger.schedule.Cron;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;

/**
 *
 */
public class CronEvalTool extends CliTool {

    private static final CliToolConfig CONFIG = config("croneval", CronEvalTool.class)
            .cmds(Eval.CMD)
            .build();

    public static void main(String[] args) throws Exception {
        int status = new CronEvalTool().execute(args);
        System.exit(status);
    }

    public CronEvalTool() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String s, CommandLine cli) throws Exception {
        return Eval.parse(terminal, cli);
    }

    static class Eval extends Command {

        private static final CliToolConfig.Cmd CMD = cmd("eval", Eval.class)
                .options(option("c", "count").hasArg(false).required(false))
                .build();

        private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss");

        final String expression;
        final int count;

        Eval(Terminal terminal, String expression, int count) {
            super(terminal);
            this.expression = expression;
            this.count = count;
        }

        public static Command parse(Terminal terminal, CommandLine cli) {
            String[] args = cli.getArgs();
            if (args.length != 1) {
                return exitCmd(ExitStatus.USAGE, terminal, "expecting a single argument that is the cron expression to evaluate");
            }
            String count = cli.getOptionValue("count", "10");
            try {
                return new Eval(terminal, args[0], Integer.parseInt(count));
            } catch (NumberFormatException nfe) {
                return exitCmd(ExitStatus.USAGE, terminal, "passed in count [%s] cannot be converted to a number", count);
            }
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {

            // when invalid, a parse expression will be thrown with a descriptive error message
            // the cli infra handles such exceptions and hows the exceptions' message
            Cron.validate(expression);

            terminal.println("Valid!");

            DateTime date = DateTime.now(DateTimeZone.getDefault());

            terminal.println("Now is [" + formatter.print(date) + "]");
            terminal.println("Here are the next " + count + " times this cron expression will trigger:");

            Cron cron = new Cron(expression);

            long time = date.getMillis();
            for (int i = 0; i < count; i++) {
                long prevTime = time;
                time = cron.getNextValidTimeAfter(time);
                if (time < 0) {
                    terminal.printError((i + 1) + ".\t Could not compute future times since [" + formatter.print(prevTime) + "] " +
                            "(perhaps the cron expression only points to times in the past?)");
                    return ExitStatus.OK;
                }
                terminal.println((i+1) + ".\t" + formatter.print(time));
            }
            return ExitStatus.OK;
        }
    }
}
