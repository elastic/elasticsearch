/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto.tool;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CheckFileCommand;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.crypto.InternalCryptoService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.config;

/**
 *
 */
public class SystemKeyTool extends CliTool {

    public static final Set<PosixFilePermission> PERMISSION_OWNER_READ_WRITE = Sets.newHashSet(PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE);


    public static void main(String[] args) throws Exception {
        ExitStatus exitStatus = new SystemKeyTool().execute(args);
        exit(exitStatus.status());
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    private static void exit(int status) {
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
        return Generate.parse(terminal, commandLine, env);
    }

    static class Generate extends CheckFileCommand {

        private static final CliToolConfig.Cmd CMD = cmd("generate", Generate.class).build();

        final Path path;

        Generate(Terminal terminal, Path path) {
            super(terminal);
            this.path = path;
        }

        public static Command parse(Terminal terminal, CommandLine cl, Environment env) {
            String[] args = cl.getArgs();
            if (args.length > 1) {
                return exitCmd(ExitStatus.USAGE, terminal, "Too many arguments");
            }
            Path path = args.length != 0 ? env.binFile().getParent().resolve(args[0]) : null;
            return new Generate(terminal, path);
        }

        @Override
        protected Path[] pathsForPermissionsCheck(Settings settings, Environment env) {
            Path path = this.path;
            if (path == null) {
                path = InternalCryptoService.resolveSystemKey(settings, env);
            }
            return new Path[] { path };
        }

        @Override
        public ExitStatus doExecute(Settings settings, Environment env) throws Exception {
            Path path = this.path;
            try {
                if (path == null) {
                    path = InternalCryptoService.resolveSystemKey(settings, env);
                }
                terminal.println(Terminal.Verbosity.VERBOSE, "generating...");
                byte[] key = InternalCryptoService.generateKey();
                terminal.println(String.format(Locale.ROOT, "Storing generated key in [%s]...", path.toAbsolutePath()));
                Files.write(path, key, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException ioe) {
                terminal.printError(String.format(Locale.ROOT, "Cannot generate and save system key file [%s]", path.toAbsolutePath()));
                return ExitStatus.IO_ERROR;
            }

            boolean supportsPosixPermissions = Environment.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
            if (supportsPosixPermissions) {
                try {
                    Files.setPosixFilePermissions(path, PERMISSION_OWNER_READ_WRITE);
                } catch (IOException ioe) {
                    terminal.printError(String.format(Locale.ROOT, "Cannot set owner read/write permissions to generated system key file [%s]", path.toAbsolutePath()));
                    return ExitStatus.IO_ERROR;
                }
                terminal.println("Ensure the generated key can be read by the user that Elasticsearch runs as, permissions are set to owner read/write only");
            }

            return ExitStatus.OK;
        }
    }

}
