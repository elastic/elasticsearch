/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.signature.tool;

import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.cli.commons.CommandLine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.signature.InternalSignatureService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.config;

/**
 *
 */
public class SystemKeyTool extends CliTool {

    public static void main(String[] args) throws Exception {
        int status = new SystemKeyTool().execute(args);
        System.exit(status);
    }

    private static final CliToolConfig CONFIG = config("syskey", SystemKeyTool.class)
            .cmds(Generate.CMD)
            .build();

    public SystemKeyTool() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String cmd, CommandLine commandLine) throws Exception {
        return Generate.parse(terminal, commandLine);
    }

    static class Generate extends Command {

        private static final CliToolConfig.Cmd CMD = cmd("generate", Generate.class).build();

        final Path path;

        Generate(Terminal terminal, Path path) {
            super(terminal);
            this.path = path;
        }

        public static Command parse(Terminal terminal, CommandLine cl) {
            String[] args = cl.getArgs();
            if (args.length > 1) {
                return exitCmd(ExitStatus.USAGE, terminal, "Too many arguments");
            }
            Path path = args.length != 0 ? Paths.get(args[0]) : null;
            return new Generate(terminal, path);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Path path = this.path;
            if (path == null) {
                path = InternalSignatureService.resolveFile(settings, env);
            }
            terminal.println(Terminal.Verbosity.VERBOSE, "generating...");
            byte[] key = InternalSignatureService.generateKey();
            terminal.println("Storing generated key in [%s]", path.toAbsolutePath());
            Files.write(path, key, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            return ExitStatus.OK;
        }
    }

}