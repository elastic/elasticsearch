/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto.tool;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

class SystemKeyTool extends EnvironmentAwareCommand {

    static final String KEY_ALGO = "HmacSHA512";
    static final int KEY_SIZE = 1024;

    private final OptionSpec<String> arguments;

    SystemKeyTool() {
        super("system key tool");
        arguments = parser.nonOptions("key path");
    }

    public static final Set<PosixFilePermission> PERMISSION_OWNER_READ_WRITE = Set.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE
    );

    @Override
    public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        final Path keyPath;

        if (options.hasArgument(arguments)) {
            List<String> args = arguments.values(options);
            if (args.size() > 1) {
                throw new UserException(ExitCodes.USAGE, "No more than one key path can be supplied");
            }
            keyPath = parsePath(args.get(0));
        } else {
            keyPath = XPackPlugin.resolveConfigFile(env, "system_key");
        }

        // write the key
        terminal.println(Terminal.Verbosity.VERBOSE, "generating...");
        byte[] key = generateKey();
        terminal.println(String.format(Locale.ROOT, "Storing generated key in [%s]...", keyPath.toAbsolutePath()));
        Files.write(keyPath, key, StandardOpenOption.CREATE_NEW);

        // set permissions to 600
        PosixFileAttributeView view = Files.getFileAttributeView(keyPath, PosixFileAttributeView.class);
        if (view != null) {
            view.setPermissions(PERMISSION_OWNER_READ_WRITE);
            terminal.println(
                "Ensure the generated key can be read by the user that Elasticsearch runs as, "
                    + "permissions are set to owner read/write only"
            );
        }
    }

    static byte[] generateKey() {
        return generateSecretKey(KEY_SIZE).getEncoded();
    }

    static SecretKey generateSecretKey(int keyLength) {
        try {
            KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGO);
            generator.init(keyLength);
            return generator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new ElasticsearchException("failed to generate key", e);
        }
    }

    @SuppressForbidden(reason = "Parsing command line path")
    private static Path parsePath(String path) {
        return PathUtils.get(path);
    }

}
