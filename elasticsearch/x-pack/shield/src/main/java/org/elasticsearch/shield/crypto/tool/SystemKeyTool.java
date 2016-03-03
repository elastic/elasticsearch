/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto.tool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.support.FileAttributesChecker;

public class SystemKeyTool extends Command {

    public static final Set<PosixFilePermission> PERMISSION_OWNER_READ_WRITE = Sets.newHashSet(PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE);

    public static void main(String[] args) throws Exception {
        Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, Terminal.DEFAULT);
        exit(new SystemKeyTool(env).main(args, Terminal.DEFAULT));
    }

    private final Environment env;
    private final OptionSpec<String> arguments;

    public SystemKeyTool(Environment env) {
        super("Generates the system key");
        this.env = env;
        this.arguments = parser.nonOptions("key path");
    }

    @Override
    protected int execute(Terminal terminal, OptionSet options) throws Exception {
        Path keyPath = null;
        List<String> args = arguments.values(options);
        if (args.size() > 1) {
            throw new UserError(ExitCodes.USAGE, "No more than one key path can be supplied");
        } else if (args.size() == 1) {
            keyPath = PathUtils.get(args.get(0));
        }
        execute(terminal, keyPath);
        return ExitCodes.OK;
    }

    // pkg private for tests
    void execute(Terminal terminal, Path keyPath) throws Exception {
        if (keyPath == null) {
            keyPath = InternalCryptoService.resolveSystemKey(env.settings(), env);
        }
        FileAttributesChecker attributesChecker = new FileAttributesChecker(keyPath);

        // write the key
        terminal.println(Terminal.Verbosity.VERBOSE, "generating...");
        byte[] key = InternalCryptoService.generateKey();
        terminal.println(String.format(Locale.ROOT, "Storing generated key in [%s]...", keyPath.toAbsolutePath()));
        Files.write(keyPath, key, StandardOpenOption.CREATE_NEW);

        // set permissions to 600
        PosixFileAttributeView view = Files.getFileAttributeView(keyPath, PosixFileAttributeView.class);
        if (view != null) {
            view.setPermissions(PERMISSION_OWNER_READ_WRITE);
            terminal.println("Ensure the generated key can be read by the user that Elasticsearch runs as, "
                + "permissions are set to owner read/write only");
        }

        // check if attributes changed
        attributesChecker.check(terminal);
    }

}
